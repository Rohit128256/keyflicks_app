package s3_store

import (
	"context"
	"fmt"
	"io"
	"log"
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

// function to put an object to the s3Store
func (s *S3Store) PutObject(ctx context.Context, bucket, key string, body io.Reader, contentType string) (*s3.PutObjectOutput, error) {
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   body,
	}

	if contentType != "" {
		input.ContentType = aws.String(contentType)
	}

	// perform the upload
	out, err := s.client.PutObject(ctx, input)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// DeleteObjectsByPrefix finds all objects with a given prefix and deletes them.
func (s *S3Store) DeleteObjectsByPrefix(ctx context.Context, bucket, prefix string) error {
	// 1. List all objects with the prefix
	listOut, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return fmt.Errorf("failed to list objects: %w", err)
	}

	// If there are no objects, we're done
	if len(listOut.Contents) == 0 {
		log.Printf("No objects found in bucket %s with prefix %s", bucket, prefix)
		return nil
	}

	// 2. Prepare the list of objects to delete
	var objectsToDelete []types.ObjectIdentifier
	for _, obj := range listOut.Contents {
		objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
			Key: obj.Key,
		})
	}

	// 3. Delete the objects in a batch
	// Note: This simple version assumes < 1000 objects.
	// For HLS/DASH, this is almost always true.
	_, err = s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{Objects: objectsToDelete},
	})
	if err != nil {
		return fmt.Errorf("failed to delete objects: %w", err)
	}

	log.Printf("Successfully deleted %d objects from bucket %s with prefix %s", len(objectsToDelete), bucket, prefix)
	return nil
}
