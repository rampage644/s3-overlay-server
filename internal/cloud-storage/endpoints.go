package cloud_storage

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"strconv"

	"github.com/aws/smithy-go"
	"github.com/go-kit/kit/endpoint"
)

// GetObject request
type GetObjectRequest struct {
	Bucket string
	Key    string
	Range  string
}

// GetObject response
type GetObjectResponse struct {
	Body io.ReadCloser
}

type PutObjectRequest struct {
	BucketName     string
	ObjectKey      string
	ObjectBody     io.ReadCloser
	ContentLength  int64
	ContentMD5     string
	ChecksumSHA256 string
}

type PutObjectResponse struct {
}
type HeadObjectRequest struct {
	Bucket string
	Key    string
}

type HeadObjectResponse struct {
	Metadata map[string]string `json:"metadata"`
}

// ListObjects request
type ListObjectsRequest struct {
	Bucket       string
	Prefix       string
	Delimiter    string
	EncodingType string
}

type ListBucketsRequest struct {
}
type ListBucketsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult" json:"-"`

	// Container for one or more buckets.
	Buckets struct {
		Buckets []Bucket `xml:"Bucket"`
	} // Buckets are nested

	// Error to indicate business logic error
	Err string `json:"err,omitempty"`
}
type ListObjectsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult" json:"-"`

	Name       string
	Prefix     string
	StartAfter string `xml:"StartAfter,omitempty"`

	ContinuationToken     string `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string `xml:"NextContinuationToken,omitempty"`

	KeyCount  int
	MaxKeys   int
	Delimiter string `xml:"Delimiter,omitempty"`

	IsTruncated bool

	Contents       []Object
	CommonPrefixes []CommonPrefix

	// Encoding type used to encode object keys in the response.
	EncodingType string `xml:"EncodingType,omitempty"`
}

type DeleteObjectRequest struct {
	BucketName string
	ObjectKey  string
}

type DeleteObjectResponse struct {
}

type APIErrorResponse struct {
	XMLName    xml.Name `xml:"Error" json:"-"`
	Code       string
	Message    string
	Key        string `xml:"Key,omitempty" json:"Key,omitempty"`
	BucketName string `xml:"BucketName,omitempty" json:"BucketName,omitempty"`
	Resource   string
	Region     string `xml:"Region,omitempty" json:"Region,omitempty"`
	RequestID  string `xml:"RequestId" json:"RequestId"`
	HostID     string `xml:"HostId" json:"HostId"`
}

type Bucket struct {
	Name         string
	CreationDate string // time string of format "2006-01-02T15:04:05.000Z"
}

type Object struct {
	Key          string
	LastModified string // time string of format "2006-01-02T15:04:05.000Z"
	ETag         string
	Size         int64
}
type CommonPrefix struct {
	Prefix string
}

func MakeHeadObjectEndpoint(svc CloudStorage) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(HeadObjectRequest)
		metadata, err := svc.HeadObject(ctx, req.Bucket, req.Key)
		if err != nil {
			code, message := "InternalError", err.Error()
			var ae smithy.APIError
			if errors.As(err, &ae) {
				code, message = ae.ErrorCode(), ae.ErrorMessage()
			}

			return APIErrorResponse{
				Code:    code,
				Message: message,
			}, nil
		}
		return HeadObjectResponse{map[string]string{
			"Content-Length": strconv.Itoa(int(metadata.ContentLength)),
			"Content-Type":   *metadata.ContentType,
			"ETag":           *metadata.ETag,
			"Last-Modified":  metadata.LastModified.Format("Mon, 02 Jan 2006 15:04:05 GMT"),
		}}, nil
	}
}

// GetObject endpoint
func MakeGetObjectEndpoint(svc CloudStorage) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(GetObjectRequest)
		body, err := svc.GetObject(ctx, req.Bucket, req.Key, req.Range)
		if err != nil {
			code, message := "InternalError", err.Error()
			var ae smithy.APIError
			if errors.As(err, &ae) {
				code, message = ae.ErrorCode(), ae.ErrorMessage()
			}
			return APIErrorResponse{
				Code:    code,
				Message: message,
			}, nil
		}
		return GetObjectResponse{body}, nil
	}
}

// ListObjects endpoint
func MakeListObjectsEndpoint(svc CloudStorage) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(ListObjectsRequest)
		objects, err := svc.ListObjects(ctx, req.Bucket, req.Prefix)
		if err != nil {
			code, message := "InternalError", err.Error()
			var ae smithy.APIError
			if errors.As(err, &ae) {
				code, message = ae.ErrorCode(), ae.ErrorMessage()
			}
			return APIErrorResponse{
				Code:    code,
				Message: message,
			}, nil
		}

		response := ListObjectsResponse{
			Name:      req.Bucket,
			Prefix:    req.Prefix,
			Delimiter: req.Delimiter,
			Contents:  objects,
		}

		return response, nil
	}
}

func MakeListBucketsEndpoint(svc CloudStorage) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		objects, err := svc.ListBuckets(ctx)
		if err != nil {
			code, message := "InternalError", err.Error()
			var ae smithy.APIError
			if errors.As(err, &ae) {
				code, message = ae.ErrorCode(), ae.ErrorMessage()
			}
			return APIErrorResponse{
				Code:    code,
				Message: message,
			}, nil
		}

		buckets := make([]Bucket, len(objects))
		for i, obj := range objects {
			buckets[i] = Bucket{
				Name:         obj.Name,
				CreationDate: obj.CreationDate,
			}
		}

		response := ListBucketsResponse{}
		response.Buckets.Buckets = buckets
		return response, nil
	}
}

func MakePutObjectEndpoint(svc CloudStorage) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(PutObjectRequest)
		err := svc.PutObject(ctx, req.BucketName, req.ObjectKey, req.ObjectBody, req.ContentLength, req.ContentMD5, req.ChecksumSHA256)
		defer req.ObjectBody.Close()
		if err != nil {
			code, message := "InternalError", err.Error()
			var ae smithy.APIError
			if errors.As(err, &ae) {
				code, message = ae.ErrorCode(), ae.ErrorMessage()
			}
			return APIErrorResponse{
				Code:    code,
				Message: message,
			}, nil
		}
		return PutObjectResponse{}, nil
	}
}

func MakeDeleteObjectEndpoint(svc CloudStorage) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(DeleteObjectRequest)
		err := svc.DeleteObject(ctx, req.BucketName, req.ObjectKey)
		if err != nil {
			code, message := "InternalError", err.Error()
			var ae smithy.APIError
			if errors.As(err, &ae) {
				code, message = ae.ErrorCode(), ae.ErrorMessage()
			}
			return APIErrorResponse{
				Code:    code,
				Message: message,
			}, nil
		}
		return DeleteObjectResponse{}, nil
	}
}
