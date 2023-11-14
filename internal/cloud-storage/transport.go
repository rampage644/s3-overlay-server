package cloud_storage

// The profilesvc is just over HTTP, so we just have a single transport.go.

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/aws/smithy-go"
	"github.com/gorilla/mux"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/transport"
	httptransport "github.com/go-kit/kit/transport/http"
)

var (
	// ErrBadRouting is returned when an expected path variable is missing.
	// It always indicates programmer error.
	ErrBadRouting = errors.New("inconsistent mapping between route and handler (programmer error)")
)

// MakeHTTPHandler mounts all of the service endpoints into an http.Handler.
// Useful in a profilesvc server.
func MakeHTTPHandler(s CloudStorage, logger log.Logger) http.Handler {
	r := mux.NewRouter()
	options := []httptransport.ServerOption{
		httptransport.ServerErrorHandler(transport.NewLogErrorHandler(logger)),
		httptransport.ServerErrorEncoder(encodeError),
	}

	var (
		getObjectEndpoint    endpoint.Endpoint
		headObjectEndpoint   endpoint.Endpoint
		putObjectEndpoint    endpoint.Endpoint
		listObjectsEndpoint  endpoint.Endpoint
		listBucketsEndpoint  endpoint.Endpoint
		deleteObjectEndpoint endpoint.Endpoint
	)
	{
		getObjectEndpoint = MakeGetObjectEndpoint(s)
		getObjectEndpoint = LoggingMiddleware(log.With(logger, "method", "GetObject"))(getObjectEndpoint)

		headObjectEndpoint = MakeHeadObjectEndpoint(s)
		headObjectEndpoint = LoggingMiddleware(log.With(logger, "method", "HeadObject"))(headObjectEndpoint)

		putObjectEndpoint = MakePutObjectEndpoint(s)
		putObjectEndpoint = LoggingMiddleware(log.With(logger, "method", "PutObject"))(putObjectEndpoint)

		listObjectsEndpoint = MakeListObjectsEndpoint(s)
		listObjectsEndpoint = LoggingMiddleware(log.With(logger, "method", "ListObjects"))(listObjectsEndpoint)

		listBucketsEndpoint = MakeListBucketsEndpoint(s)
		listBucketsEndpoint = LoggingMiddleware(log.With(logger, "method", "ListBuckets"))(listBucketsEndpoint)

		deleteObjectEndpoint = MakeDeleteObjectEndpoint(s)
		deleteObjectEndpoint = LoggingMiddleware(log.With(logger, "method", "DeleteObject"))(deleteObjectEndpoint)
	}

	r.Methods("GET").Path("/{bucket}/{object:.+}").Handler(httptransport.NewServer(
		getObjectEndpoint,
		decodeGetObjectRequest,
		encodeGetObjectResponse,
		options...,
	))
	r.Methods("DELETE").Path("/{bucket}/{object:.+}").Handler(httptransport.NewServer(
		deleteObjectEndpoint,
		decodeDeleteObjectRequest,
		encodeResponse,
		options...,
	))
	r.Methods("HEAD").Path("/{bucket}/{object:.+}").Handler(httptransport.NewServer(
		headObjectEndpoint,
		decodeHeadObjectRequest,
		encodeHeadResponse,
		options...,
	))
	r.Methods("PUT").Path("/{bucket}/{object:.+}").Handler(httptransport.NewServer(
		putObjectEndpoint,
		decodePutObjectRequest,
		encodeResponse,
		options...,
	))
	r.Methods("GET").Path("/{bucket}/").Queries("list-type", "2", "prefix", "{prefix:.*}").Handler(httptransport.NewServer(
		listObjectsEndpoint,
		decodeListObjectsRequest,
		encodeResponse,
		options...,
	))
	r.Methods("GET").Path("/").Handler(httptransport.NewServer(
		listBucketsEndpoint,
		decodeListBucketRequest,
		encodeResponse,
		options...,
	))

	return r
}

func isRequestSignStreamingV4(r *http.Request) bool {
	const streamingContentSHA256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	return r.Header.Get("x-amz-content-sha256") == streamingContentSHA256 &&
		r.Method == http.MethodPut
}

func decodePutObjectRequest(_ context.Context, r *http.Request) (request interface{}, err error) {
	vars := mux.Vars(r)

	var body io.ReadCloser = r.Body
	var contentLength int64 = r.ContentLength
	if isRequestSignStreamingV4(r) {
		reader, err := newSignV4ChunkedReader(r, false)
		if err != nil {
			return nil, err
		}
		body = reader

		contentLengthStr := r.Header.Get("x-amz-decoded-content-length")
		contentLength, _ = strconv.ParseInt(contentLengthStr, 10, 64)
	}

	return PutObjectRequest{
		ObjectKey:     vars["object"],
		BucketName:    vars["bucket"],
		ObjectBody:    body,
		ContentLength: contentLength,
	}, nil
}

func decodeDeleteObjectRequest(_ context.Context, r *http.Request) (request interface{}, err error) {
	vars := mux.Vars(r)
	return DeleteObjectRequest{
		ObjectKey:  vars["object"],
		BucketName: vars["bucket"],
	}, nil
}

func decodeHeadObjectRequest(_ context.Context, r *http.Request) (request interface{}, err error) {
	vars := mux.Vars(r)
	return HeadObjectRequest{
		Key:    vars["object"],
		Bucket: vars["bucket"],
	}, nil
}

func decodeGetObjectRequest(_ context.Context, r *http.Request) (request interface{}, err error) {
	vars := mux.Vars(r)
	return GetObjectRequest{
		Key:    vars["object"],
		Bucket: vars["bucket"],
		Range:  r.Header.Get("Range"),
	}, nil
}

type StatusCoder interface {
	StatusCode() int
}

func (r HeadObjectResponse) Headers() http.Header {
	ret := http.Header{}
	for k, v := range r.Metadata {
		ret.Add(k, v)
	}
	return ret
}

func (r APIErrorResponse) StatusCode() int {
	switch r.Code {
	case "NotFound":
		return http.StatusNotFound
	case "NoSuchKey":
		return http.StatusNotFound
	case "NoSuchBucket":
		return http.StatusNotFound
	case "InternalError":
		return http.StatusInternalServerError
	default:
		return http.StatusInternalServerError
	}
}

func encodeGetObjectResponse(ctx context.Context, w http.ResponseWriter, response interface{}) error {
	if _, ok := response.(GetObjectResponse); !ok {
		return encodeResponse(ctx, w, response)
	}

	resp := response.(GetObjectResponse)
	defer resp.Body.Close()

	_, err := io.Copy(w, resp.Body)
	return err
}

func decodeListBucketRequest(_ context.Context, r *http.Request) (request interface{}, err error) {
	return ListBucketsRequest{}, nil
}

func decodeListObjectsRequest(_ context.Context, r *http.Request) (request interface{}, err error) {
	return ListObjectsRequest{
		Bucket:       mux.Vars(r)["bucket"],
		Prefix:       mux.Vars(r)["prefix"],
		Delimiter:    mux.Vars(r)["delimiter"],
		EncodingType: mux.Vars(r)["encoding-type"],
	}, nil
}

// encodeResponse is the common method to encode all response types to the
// client. I chose to do it this way because, since we're using JSON, there's no
// reason to provide anything more specific. It's certainly possible to
// specialize on a per-response (per-method) basis.
func encodeResponse(ctx context.Context, w http.ResponseWriter, response interface{}) error {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	if sc, ok := response.(StatusCoder); ok {
		w.WriteHeader(sc.StatusCode())
	}
	if headerer, ok := response.(httptransport.Headerer); ok {
		for k, values := range headerer.Headers() {
			for _, v := range values {
				w.Header().Add(k, v)
			}
		}
	}

	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	return enc.Encode(response)
}

func encodeHeadResponse(ctx context.Context, w http.ResponseWriter, response interface{}) error {
	if sc, ok := response.(StatusCoder); ok {
		w.WriteHeader(sc.StatusCode())
	}
	if headerer, ok := response.(httptransport.Headerer); ok {
		for k, values := range headerer.Headers() {
			for _, v := range values {
				w.Header().Add(k, v)
			}
		}
	}
	return nil
}

func encodeError(_ context.Context, err error, w http.ResponseWriter) {
	if err == nil {
		panic("encodeError with nil error")
	}

	response := APIErrorResponse{
		Code:    "UnknownError",
		Message: err.Error(),
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		w.WriteHeader(http.StatusNotFound)
		response = APIErrorResponse{
			Code:    ae.ErrorCode(),
			Message: ae.ErrorMessage(),
		}
	}
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	enc.Encode(response)
}
