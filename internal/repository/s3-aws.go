package repository

import (
	"context"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type AWSS3 struct {
	client *s3.Client
}

func MakeAWSS3(client *s3.Client) *AWSS3 {
	return &AWSS3{
		client: client,
	}
}

func (s *AWSS3) ListBuckets(ctx context.Context, params *s3.ListBucketsInput) (*s3.ListBucketsOutput, error) {
	return s.client.ListBuckets(ctx, params)
}

func (s *AWSS3) ListObjects(ctx context.Context, params *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	return s.client.ListObjectsV2(ctx, params)
}

func (s *AWSS3) HeadObject(ctx context.Context, params *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	return s.client.HeadObject(ctx, params)
}
func (s *AWSS3) GetObject(ctx context.Context, params *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return s.client.GetObject(ctx, params)
}
func (s *AWSS3) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	return s.client.DeleteObject(ctx, params)
}

func (s *AWSS3) PutObject(ctx context.Context, params *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	return s.client.PutObject(ctx, params, s3.WithAPIOptions(
		v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware,
	))
}
