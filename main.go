package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dgraph-io/ristretto"

	"github.com/go-kit/kit/log"
	cloud_storage "github.com/rampage644/s3-overlay-proxy/internal/cloud-storage"
	"github.com/rampage644/s3-overlay-proxy/internal/repository"
)

func main() {
	var (
		httpAddr         = flag.String("http.addr", ":8080", "HTTP listen address")
		objectStorageUrl = flag.String("object-storage.url", "", "object storage url")
	)
	flag.Parse()

	var logger log.Logger
	{
		logger = log.NewLogfmtLogger(os.Stderr)
		logger = log.With(logger, "ts", log.DefaultTimestampUTC)
		logger = log.With(logger, "caller", log.DefaultCaller)
	}

	var aws_s3_storage repository.ObjectStorage
	{
		cfg, err := config.LoadDefaultConfig(context.TODO())
		if err != nil {
			logger.Log("err", err)
			os.Exit(1)
		}

		optFns := []func(*s3.Options){func(o *s3.Options) {
			o.Retryer = aws.NopRetryer{}
		}}

		if *objectStorageUrl != "" {
			optFns = append(optFns, func(o *s3.Options) {
				o.BaseEndpoint = aws.String(*objectStorageUrl)
			})
		}

		client := s3.NewFromConfig(cfg, optFns...)
		aws_s3_storage = repository.MakeAWSS3(client)
	}

	var s cloud_storage.CloudStorage
	{
		cache, err := ristretto.NewCache(&ristretto.Config{
			NumCounters: 1e5,     // number of keys to track frequency of (10M).
			MaxCost:     1 << 35, // maximum cost of cache (1GB).
			BufferItems: 64,      // number of keys per Get buffer.
		})
		if err != nil {
			panic(err)
		}
		s = cloud_storage.NewCloudStorage(aws_s3_storage, log.With(logger, "component", "service"))
		s = cloud_storage.NewCachedCloudStorage(s, log.With(logger, "component", "cache"), cache)
	}

	var h http.Handler
	{
		h = cloud_storage.MakeHTTPHandler(s, log.With(logger, "component", "HTTP"))
	}

	errs := make(chan error)
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errs <- fmt.Errorf("%s", <-c)
	}()

	go func() {
		logger.Log("transport", "HTTP", "addr", *httpAddr)
		errs <- http.ListenAndServe(*httpAddr, h)
	}()

	logger.Log("exit", <-errs)
}
