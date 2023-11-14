package cloud_storage

import (
	"context"
	"time"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
)

type LoggingValuer interface {
	KeyVals() []interface{}
}

func (r GetObjectRequest) KeyVals() []interface{} {
	return []interface{}{
		"bucket", r.Bucket,
		"object", r.Key,
		"range", r.Range,
	}
}

func (r HeadObjectRequest) KeyVals() []interface{} {
	return []interface{}{
		"bucket", r.Bucket,
		"object", r.Key,
	}
}

func (r PutObjectRequest) KeyVals() []interface{} {
	return []interface{}{
		"bucket", r.BucketName,
		"object", r.ObjectKey,
		"contentLength", r.ContentLength,
	}
}

func (r APIErrorResponse) KeyVals() []interface{} {
	return []interface{}{
		"code", r.Code,
		"message", r.Message,
	}
}

// LoggingMiddleware returns an endpoint middleware that logs the
// duration of each invocation, and the resulting error, if any.
func LoggingMiddleware(logger log.Logger) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request interface{}) (response interface{}, err error) {

			defer func(begin time.Time) {
				keyvals := []interface{}{
					"took", time.Since(begin),
					"err", err,
				}
				requestLogger, ok := request.(LoggingValuer)
				if ok {
					keyvals = append(keyvals, requestLogger.KeyVals()...)
				}

				if responseLogger, ok := response.(LoggingValuer); ok {
					keyvals = append(keyvals, responseLogger.KeyVals()...)
				}
				logger.Log(keyvals...)

			}(time.Now())
			return next(ctx, request)

		}
	}
}
