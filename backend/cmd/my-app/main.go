package main

import (
	"context"
	"errors"
	"keyflicks_app/internals/cache"
	"keyflicks_app/internals/celery"
	"keyflicks_app/internals/handlers"
	"keyflicks_app/internals/routes"
	"keyflicks_app/internals/s3_store"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/gin-gonic/gin"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

func createRedisPool(redisURL string) *redigo.Pool {
	return &redigo.Pool{
		MaxIdle:   80,
		MaxActive: 12000,
		Dial: func() (redigo.Conn, error) {
			c, err := redigo.DialURL(redisURL)
			if err != nil {
				log.Fatalf("Failed to dial redis: %v", err)
			}
			return c, err
		},
	}
}

func ensureBuckets(ctx context.Context, client *s3.Client, region string, buckets ...string) error {
	for _, b := range buckets {
		// 1) Check if bucket exists
		headCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := client.HeadBucket(headCtx, &s3.HeadBucketInput{
			Bucket: aws.String(b),
		})
		cancel()

		if err != nil {
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) {
				switch apiErr.(type) {
				case *types.NotFound:
					// 2) Create when not found (omit LocationConstraint for us-east-1)
					in := &s3.CreateBucketInput{
						Bucket: aws.String(b),
					}
					if region != "us-east-1" {
						in.CreateBucketConfiguration = &types.CreateBucketConfiguration{
							LocationConstraint: types.BucketLocationConstraint(region),
						}
					}

					createCtx, cancelCreate := context.WithTimeout(ctx, 10*time.Second)
					_, cerr := client.CreateBucket(createCtx, in)
					cancelCreate()
					if cerr != nil {
						return cerr
					}
					log.Printf("Bucket %q created.\n", b)
				default:
					// Any other error means you might not own it or another issue occurred
					return err
				}
			} else {
				return err
			}
		} else {
			log.Printf("Bucket %q already exists.\n", b)
		}
	}
	return nil
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found (continuing)")
	}

	minio_endpoint := os.Getenv("MINIO_ENDPOINT")
	minio_root_user := os.Getenv("MINIO_ROOT_USER")
	minio_root_pass := os.Getenv("MINIO_ROOT_PASSWORD")
	log.Printf("DEBUG: MINIO_ENDPOINT value is: '%s'\n", minio_endpoint) // <-- Add this line

	redis_url := os.Getenv("REDIS_URL")

	s3_streaming_bucket := os.Getenv("STREAMING_BUCKET")
	s3_pending_bucket := os.Getenv("PENDING_BUCKET")

	uri_secret_token := os.Getenv("URI_SIGNATURE_SECRET")

	// celery configuration
	redis_pool := createRedisPool("redis://localhost:6379")

	celery_ins, err := celery.NewCelery(redis_pool)
	if err != nil {
		log.Printf("error occured while configuring celery : %v", err)
	}

	// redis configuration
	redis_client := redis.NewClient(&redis.Options{
		Addr: redis_url,
	})

	redis_ins := cache.NewRdisDB(redis_client)

	// for s3 configuration

	// 2. Load the base configuration (credentials, region, etc.).
	//    We NO LONGER pass the resolver here.
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(minio_root_user, minio_root_pass, "")),
		config.WithRegion("us-east-1"), // A dummy region is still needed
	)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// 3. Create the S3 client, injecting the new resolver here.
	//    The custom resolver is passed as an option directly to s3.NewFromConfig.
	s3_client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(minio_endpoint)
		o.UsePathStyle = true
	})

	s3_ins := s3_store.NewS3Store(s3_client)

	if err := ensureBuckets(context.Background(), s3_client, cfg.Region, s3_streaming_bucket, s3_pending_bucket); err != nil {
		log.Fatalf("ensureBuckets error: %v", err)
	}

	//now configuring handler
	handler_ins := handlers.NewStreamHandler(s3_ins, redis_ins, celery_ins, uri_secret_token, s3_pending_bucket, s3_streaming_bucket, 1800)

	router := gin.Default()

	routes.SetupStreamingRoutes(router, handler_ins)

	log.Println("Starting server on :8000")
	if err := router.Run(":8000"); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}

}
