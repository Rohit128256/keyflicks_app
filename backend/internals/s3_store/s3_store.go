package s3_store

import (
	"context"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3Store struct {
	client          *s3.Client
	presignedClient *s3.PresignClient
}

// acts like constructor for S3Store
func NewS3Store(client *s3.Client) *S3Store {
	return &S3Store{
		client:          client,
		presignedClient: s3.NewPresignClient(client),
	}
}

// funtion to create a presigned url
func (s *S3Store) GeneratePresignedUploadUrl(ctx context.Context, bucket string, key string, contentType string) (string, error) {
	input := &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
	}

	presigned_url, err := s.presignedClient.PresignPutObject(ctx, input, func(po *s3.PresignOptions) {
		po.Expires = 60 * time.Minute
	})
	if err != nil {
		return "", err
	}

	return presigned_url.URL, nil

}

// function to get any object from s3 store

func (s S3Store) GetObject(ctx context.Context, bucket string, key string) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	output, err := s.client.GetObject(ctx, input)
	if err != nil {
		return nil, err
	}

	return output.Body, nil
}

// ListObjects is the Go equivalent of list_objects_v2
func (s *S3Store) ListObjects(ctx context.Context, bucket string, prefix string) ([]types.Object, error) {
	// 1. Create the input struct, which is the Go equivalent of the Python parameters.
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	// 2. Call the ListObjectsV2 method on the S3 client.
	output, err := s.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, err // Return an empty slice and the error if the call fails.
	}

	// 3. The 'output.Contents' field is a slice of objects,
	// exactly like Python's resp.get("Contents", []).
	return output.Contents, nil
}
