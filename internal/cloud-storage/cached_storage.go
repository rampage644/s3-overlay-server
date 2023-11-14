package cloud_storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dgraph-io/ristretto"
	"github.com/go-kit/kit/log"
)

type cachedCloudStorage struct {
	baseStorage CloudStorage
	logger      log.Logger
	cache       *ristretto.Cache
}

func (s *cachedCloudStorage) ListBuckets(ctx context.Context) ([]Bucket, error) {
	return s.baseStorage.ListBuckets(ctx)
}

func (s *cachedCloudStorage) CreateBucket(ctx context.Context, bucketName string) error {
	return s.baseStorage.CreateBucket(ctx, bucketName)
}

func (s *cachedCloudStorage) DeleteBucket(ctx context.Context, bucketName string) error {
	return s.baseStorage.DeleteBucket(ctx, bucketName)
}

func (s *cachedCloudStorage) ListObjects(ctx context.Context, bucketName string, prefix string) ([]Object, error) {
	return s.baseStorage.ListObjects(ctx, bucketName, prefix)
}

func (s *cachedCloudStorage) PutObject(ctx context.Context, bucketName, objectKey string, content io.Reader, length int64, md5 string, sha256 string) error {
	cacheKey := fmt.Sprintf("%s/%s", bucketName, objectKey)
	value, err := io.ReadAll(content)
	if err != nil {
		return err
	}
	reader := io.NopCloser(bytes.NewReader(value))

	_ = s.cache.Set(cacheKey, value, 1)

	go func() {
		start := time.Now()
		err = s.baseStorage.PutObject(context.Background(), bucketName, objectKey, reader, length, md5, sha256)
		s.logger.Log("method", "PutObject", "bucket", bucketName, "object", objectKey, "took", time.Since(start), "err", err)
	}()
	return nil
}

func (s *cachedCloudStorage) HeadObject(ctx context.Context, bucketName, objectKey string) (*s3.HeadObjectOutput, error) {
	cacheKey := fmt.Sprintf("head/%s/%s", bucketName, objectKey)
	if value, found := s.cache.Get(cacheKey); found {
		if ret, ok := value.(*s3.HeadObjectOutput); ok {
			return ret, nil
		}
	}

	headObjectOutput, err := s.baseStorage.HeadObject(ctx, bucketName, objectKey)
	if err != nil {
		return nil, err
	}

	_ = s.cache.Set(cacheKey, headObjectOutput, 1)

	return headObjectOutput, nil
}
func parseContentRange(contentRange string) (int, int, error) {
	var start, end int
	_, err := fmt.Sscanf(contentRange, "bytes=%d-%d", &start, &end)
	if err != nil {
		return 0, 0, err
	}
	return start, end, nil
}

func parceContentRangeOpen(contentRange string) (int, error) {
	var start int
	_, err := fmt.Sscanf(contentRange, "bytes=%d-", &start)
	if err != nil {
		return 0, err
	}
	return start, nil
}

func (s *cachedCloudStorage) GetObject(ctx context.Context, bucketName, objectKey, contentRange string) (io.ReadCloser, error) {
	cacheKey := fmt.Sprintf("%s/%s", bucketName, objectKey)
	if value, found := s.cache.Get(cacheKey); found {
		if ret, ok := value.([]byte); ok {
			// Handle Range Request explicitly here as base S3 handles this automatically
			if contentRange != "" {
				start, end, err := parseContentRange(contentRange)
				if err != nil {
					start, err = parceContentRangeOpen(contentRange)
				}
				if err != nil {
					return nil, err
				}
				s.logger.Log("method", "GetObject", "bucket", bucketName, "object", objectKey, "objectSize", len(ret), "contentRange", contentRange, "start", start, "end", end, "err", err)
				if end == 0 {
					ret = ret[start:]
				} else {
					ret = ret[start:end]
				}
			}

			return io.NopCloser(bytes.NewReader(ret)), nil
		}
	}

	object, err := s.baseStorage.GetObject(ctx, bucketName, objectKey, contentRange)
	if err != nil {
		return nil, err
	}

	value, err := io.ReadAll(object)
	if err != nil {
		return nil, err
	}

	// Avoid caching imcomplete objects
	if contentRange == "" {
		_ = s.cache.Set(cacheKey, value, 1)
	} else {
		// Instead, schedule getting full one
		go func() {
			start := time.Now()
			_, err = s.GetObject(context.Background(), bucketName, objectKey, "")
			s.logger.Log("method", "GetObject", "bucket", bucketName, "object", objectKey, "took", time.Since(start), "err", err)
		}()
	}

	return io.NopCloser(bytes.NewReader(value)), nil
}

func (s *cachedCloudStorage) DeleteObject(ctx context.Context, bucketName, objectKey string) error {
	err := s.baseStorage.DeleteObject(ctx, bucketName, objectKey)
	if err == nil {
		cacheKey := fmt.Sprintf("%s/%s", bucketName, objectKey)
		s.cache.Del(cacheKey)
	}
	return err
}

func NewCachedCloudStorage(baseStorage CloudStorage, logger log.Logger, cache *ristretto.Cache) *cachedCloudStorage {
	return &cachedCloudStorage{
		baseStorage: baseStorage,
		logger:      logger,
		cache:       cache,
	}
}
