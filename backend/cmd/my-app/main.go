package main

import (
	"context"
	"errors"
	"keyflicks_app/cmd/services"
	"keyflicks_app/internals/auth"
	"keyflicks_app/internals/cache"
	"keyflicks_app/internals/celery"
	database "keyflicks_app/internals/db"
	"keyflicks_app/internals/handlers"
	"keyflicks_app/internals/middlewares"
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
	"github.com/jackc/pgx/v5/pgxpool"
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

	// loading all the .env variables
	minio_endpoint := os.Getenv("MINIO_ENDPOINT")
	minio_root_user := os.Getenv("MINIO_ROOT_USER")
	minio_root_pass := os.Getenv("MINIO_ROOT_PASSWORD")
	redis_url := os.Getenv("REDIS_URL")
	jwt_secret := os.Getenv("JWT_SECRET")
	// dbUser := os.Getenv("POSTGRES_USER")
	// dbPass := os.Getenv("POSTGRES_PASSWORD")
	// dbHost := os.Getenv("POSTGRES_HOST")
	// dbName := os.Getenv("POSTGRES_DB")
	db_url := os.Getenv("REMOTE_DB")
	s3_streaming_bucket := os.Getenv("STREAMING_BUCKET")
	s3_pending_bucket := os.Getenv("PENDING_BUCKET")
	s3_profile_bucket := os.Getenv("PROFILE_BUCKET")
	uri_secret_token := os.Getenv("URI_SIGNATURE_SECRET")

	log.Printf("DEBUG: MINIO_ENDPOINT value is: '%s'\n", minio_endpoint) // just for basic debugging

	// postgres database configuration
	dbPool, err := pgxpool.New(context.Background(), db_url)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer dbPool.Close()

	// verify database connection
	if err := dbPool.Ping(context.Background()); err != nil {
		log.Fatalf("Database ping failed: %v\n", err)
	}
	log.Println("Connected to PostgreSQL successfully")

	db_store := database.NewDbStore(dbPool)

	// celery configuration
	redis_pool := createRedisPool(redis_url)
	celery_ins, err := celery.NewCelery(redis_pool)
	if err != nil {
		log.Printf("error occured while configuring celery! : %v", err)
	}

	// redis configuration
	opt, err := redis.ParseURL(redis_url)
	if err != nil {
		log.Printf("error occured while parsing redis url! : %v", err)
	}
	redis_client := redis.NewClient(opt)

	defer redis_client.Close()

	// verify redis connection
	if err := redis_client.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("Redis ping failed: %v\n", err)
	}
	log.Println("Connected to Redis successfully")

	redis_ins := cache.NewRdisDB(redis_client)

	// S3 Storage configuration (minio here)

	// loading the base configuration (credentials, region, etc.).
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(minio_root_user, minio_root_pass, "")),
		config.WithRegion("us-east-1"), // A dummy region is still needed
	)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Create the S3 client, injecting the new resolver here.
	s3_client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(minio_endpoint)
		o.UsePathStyle = true
	})

	s3_ins := s3_store.NewS3Store(s3_client)

	if err := ensureBuckets(context.Background(), s3_client, cfg.Region, s3_streaming_bucket, s3_pending_bucket, s3_profile_bucket); err != nil {
		log.Fatalf("ensureBuckets error: %v", err)
	}

	// initializing jwt_auth
	jwt_auth := auth.NewJwt(jwt_secret)

	// now initializing different handlers handler
	stream_handler := handlers.NewStreamHandler(s3_ins, db_store, redis_ins, celery_ins, uri_secret_token, s3_pending_bucket, s3_streaming_bucket, 1800)
	auth_handler := handlers.NewAuthHandler(db_store, s3_ins, jwt_auth, redis_ins, s3_profile_bucket)
	event_handler := handlers.NewEventHandler(db_store, redis_ins)

	// now inititalize the moddleware
	auth_middleware := middlewares.AuthMiddleware(db_store, redis_ins, jwt_auth)

	// now defining and starting the background services

	bgCtx := context.Background()

	dbWriterService := services.NewDBWriter(dbPool, redis_client, 3)
	commentsWriterService := services.NewCommentsWriter(dbPool, redis_ins, 3)
	commentsDeleterService := services.NewCommentsDeleter(dbPool, redis_ins, 3)
	likeStreamUpdaterService := services.NewStreamLikesWorker(dbPool, redis_ins)

	// starting the background services
	go dbWriterService.Start(bgCtx)
	go commentsWriterService.Start(bgCtx)
	go likeStreamUpdaterService.Start(bgCtx, 3, 2)
	go commentsDeleterService.Start(bgCtx)

	log.Printf("All the Services are running")

	// settings up the routes
	router := gin.Default()

	routes.SetupStreamingRoutes(router, stream_handler, auth_handler, event_handler, auth_middleware)

	log.Println("Starting server on :8000")
	if err := router.Run(":8000"); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}

}
