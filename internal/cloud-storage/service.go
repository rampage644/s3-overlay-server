package cloud_storage

import (
	"context"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-kit/kit/log"

	"github.com/rampage644/s3-overlay-proxy/internal/repository"
)

// CloudStorage represents an interface for interacting with a cloud-based storage service.
// Implementations of this interface can provide functionality to manage buckets and objects
// within the cloud storage system.
type CloudStorage interface {
	// ListBuckets lists all buckets associated with the account.
	// It takes a context.Context as an argument for cancellation and timeout purposes.
	// It returns a slice of Bucket objects and an error if the operation fails.
	ListBuckets(ctx context.Context) ([]Bucket, error)

	// CreateBucket creates a new bucket with the specified name.
	// It takes a context.Context for cancellation and timeout, and the desired bucket name.
	// It returns an error if the bucket creation fails.
	CreateBucket(ctx context.Context, bucketName string) error

	// DeleteBucket deletes the bucket with the provided name.
	// It requires a context.Context for timeout and cancellation.
	// It returns an error if the bucket deletion operation fails.
	DeleteBucket(ctx context.Context, bucketName string) error

	// ListObjects lists the objects within the specified bucket.
	// It takes a context.Context for cancellation and timeout, and the target bucket name.
	// It returns a slice of Object objects and an error if the listing operation fails.
	ListObjects(ctx context.Context, bucketName string, prefix string) ([]Object, error)

	// PutObject uploads an object to the specified bucket and object key.
	// It requires a context.Context, the bucket name, and a reader for the object's content.
	// It returns an error if the object upload operation fails.
	PutObject(ctx context.Context, bucketName, objectKey string, content io.Reader, length int64, md5 string, sha256 string) error

	HeadObject(ctx context.Context, bucketName, objectKey string) (ObjectMetadata, error)
	// GetObject downloads the object with the given bucket and object key.
	// It takes a context.Context, the bucket name, and object key.
	// It returns an io.ReadCloser for reading the object content and an error if the operation fails.
	GetObject(ctx context.Context, bucketName, objectKey, contentRange string) (io.ReadCloser, error)

	// DeleteObject deletes the object with the specified bucket and object key.
	// It requires a context.Context, the bucket name, and the object key.
	// It returns an error if the object deletion operation fails.
	DeleteObject(ctx context.Context, bucketName, objectKey string) error
}

type cloudStorageService struct {
	os     repository.ObjectStorage
	logger log.Logger
}

type ObjectMetadata = *s3.HeadObjectOutput

func (s *cloudStorageService) ListBuckets(ctx context.Context) ([]Bucket, error) {
	bckts, err := s.os.ListBuckets(ctx, &repository.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	buckets := make([]Bucket, len(bckts.Buckets))
	for i, b := range bckts.Buckets {
		buckets[i] = Bucket{
			Name:         *b.Name,
			CreationDate: b.CreationDate.Format(time.RFC3339),
		}
	}
	return buckets, nil
}

func (s *cloudStorageService) CreateBucket(ctx context.Context, bucketName string) error {
	return nil
}

func (s *cloudStorageService) DeleteBucket(ctx context.Context, bucketName string) error {
	return nil
}

func (s *cloudStorageService) ListObjects(ctx context.Context, bucketName string, prefix string) ([]Object, error) {
	objs, err := s.os.ListObjects(ctx, &repository.ListObjectsInput{
		Bucket: &bucketName,
		Prefix: &prefix,
	})
	if err != nil {
		return nil, err
	}

	objects := make([]Object, len(objs.Contents))
	for i, obj := range objs.Contents {
		objects[i] = Object{
			Key:          *obj.Key,
			LastModified: obj.LastModified.Format(time.RFC3339),
			Size:         obj.Size,
		}
	}
	return objects, nil
}

func (s *cloudStorageService) PutObject(ctx context.Context, bucketName, objectKey string, content io.Reader, length int64, md5 string, sha256 string) error {
	req := &repository.PutObjectInput{
		Bucket:        &bucketName,
		Key:           &objectKey,
		Body:          content,
		ContentLength: length,
		ContentMD5:    &md5,
	}

	_, err := s.os.PutObject(ctx, req)
	s.logger.Log("method", "PutObject", "err", err)

	if err != nil {
		return err
	}

	return nil
}

func (s *cloudStorageService) HeadObject(ctx context.Context, bucketName, objectKey string) (*s3.HeadObjectOutput, error) {
	metadata, err := s.os.HeadObject(ctx, &repository.HeadObjectInput{
		Bucket: &bucketName,
		Key:    &objectKey,
	})

	if err != nil {
		return nil, err
	}

	return metadata, nil
}

func (s *cloudStorageService) GetObject(ctx context.Context, bucketName, objectKey, contentRange string) (io.ReadCloser, error) {
	output, err := s.os.GetObject(ctx, &repository.GetObjectInput{
		Bucket: &bucketName,
		Key:    &objectKey,
		Range:  &contentRange,
	})

	if err != nil {
		return nil, err
	}

	return output.Body, nil
}

func (s *cloudStorageService) DeleteObject(ctx context.Context, bucketName, objectKey string) error {
	return nil
}

func NewCloudStorage(os repository.ObjectStorage, logger log.Logger) *cloudStorageService {
	return &cloudStorageService{
		os:     os,
		logger: logger,
	}
}
