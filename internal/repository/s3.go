package repository

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TODO: too coupled with underlying aws-sdk-go-v2!
type ListBucketsInput = s3.ListBucketsInput
type ListBucketsOutput = s3.ListBucketsOutput
type ListObjectsInput = s3.ListObjectsV2Input
type ListObjectsOutput = s3.ListObjectsV2Output
type HeadObjectInput = s3.HeadObjectInput
type HeadObjectOutput = s3.HeadObjectOutput
type GetObjectInput = s3.GetObjectInput
type GetObjectOutput = s3.GetObjectOutput
type PutObjectInput = s3.PutObjectInput
type PutObjectOutput = s3.PutObjectOutput
type DeleteObjectInput = s3.DeleteObjectInput
type DeleteObjectOutput = s3.DeleteObjectOutput

type ObjectStorage interface {
	ListBuckets(ctx context.Context, params *ListBucketsInput) (*ListBucketsOutput, error)
	ListObjects(ctx context.Context, params *ListObjectsInput) (*ListObjectsOutput, error)
	HeadObject(ctx context.Context, params *HeadObjectInput) (*HeadObjectOutput, error)
	GetObject(ctx context.Context, params *GetObjectInput) (*GetObjectOutput, error)
	PutObject(ctx context.Context, params *PutObjectInput) (*PutObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error)
}
