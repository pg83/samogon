package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// Storage is the S3 client. Uses aws-sdk-go-v2 directly — one
// long-lived HTTP client with connection pooling. Earlier revisions
// shelled out to `minio-client` per call; forking a process for every
// 256 KiB piece upload caps throughput at a few hundred pieces/sec
// regardless of parallelism. Using the SDK drops fork+exec overhead
// and lets retries happen inside one TCP connection.
type Storage struct {
	cfg *Config
	cli *s3.Client

	// Puts counts how many PutObject calls we've considered
	// successful. Compared against the number of objects actually
	// in the bucket, this tells you whether the SDK is reporting
	// success without uploading anything.
	Puts atomic.Int64
}

func newStorage(cfg *Config) *Storage {
	awsCfg := aws.Config{
		Region:      cfg.Region,
		Credentials: credentials.NewStaticCredentialsProvider(cfg.AWSKey, cfg.AWSSecret, ""),
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        256,
				MaxIdleConnsPerHost: 256,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}

	cli := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.S3Endpt)
		o.UsePathStyle = true
		o.RetryMaxAttempts = 6
	})

	return &Storage{cfg: cfg, cli: cli}
}

// Close is a no-op; kept so callers can `defer store.Close()` the
// same way they would for a resource that needs cleanup.
func (s *Storage) Close() {}

// isNotFound matches the S3 "no such bucket / no such key / 404"
// family. Used by List callers to turn an empty/fresh prefix into an
// empty listing rather than an error.
func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	var nsb *types.NoSuchBucket
	var notFound *types.NotFound
	var apiErr smithy.APIError

	if errors.As(err, &nsk) || errors.As(err, &nsb) || errors.As(err, &notFound) {
		return true
	}

	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchBucket", "NoSuchKey", "NotFound", "404":
			return true
		}
	}

	return false
}

// Stat returns true iff key exists. Any error (network, auth, missing)
// becomes false — callers treat "not there" as "not uploaded yet" and
// the real error surfaces on the next Put.
func (s *Storage) Stat(key string) bool {
	_, err := s.cli.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(s.cfg.S3Bucket),
		Key:    aws.String(key),
	})

	return err == nil
}

func (s *Storage) PutBytes(key string, data []byte) {
	_, err := s.cli.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:        aws.String(s.cfg.S3Bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})

	if err != nil {
		ThrowFmt("s3 PutObject %s: %v", key, err)
	}

	s.Puts.Add(1)
}

func (s *Storage) PutFile(key, p string) {
	f := Throw2(os.Open(p))
	defer f.Close()

	info := Throw2(f.Stat())

	_, err := s.cli.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:        aws.String(s.cfg.S3Bucket),
		Key:           aws.String(key),
		Body:          f,
		ContentLength: aws.Int64(info.Size()),
	})

	if err != nil {
		ThrowFmt("s3 PutObject %s (from %s): %v", key, p, err)
	}

	s.Puts.Add(1)
}

func (s *Storage) Cat(key string) []byte {
	out, err := s.cli.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.cfg.S3Bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		ThrowFmt("s3 GetObject %s: %v", key, err)
	}

	defer out.Body.Close()

	return Throw2(io.ReadAll(out.Body))
}

// List returns the full keys under prefix. A "no such bucket/key"
// error is treated as an empty listing (same observable state as an
// empty prefix) — auth and network failures still throw.
func (s *Storage) List(prefix string) []string {
	pager := s3.NewListObjectsV2Paginator(s.cli, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.cfg.S3Bucket),
		Prefix: aws.String(prefix),
	})

	var keys []string

	for pager.HasMorePages() {
		page, err := pager.NextPage(context.Background())

		if err != nil {
			if isNotFound(err) {
				return nil
			}

			ThrowFmt("s3 ListObjectsV2 %s: %v", prefix, err)
		}

		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}

	return keys
}

// ListPieces returns a set of piece hashes (the leaf of each CAS key
// under PrefixPieces). Called once at fetch start so anacrolix's
// per-piece Completion() can check an in-memory map instead of
// hitting S3 for every piece.
func (s *Storage) ListPieces(cfg *Config) map[string]bool {
	keys := s.List(cfg.PrefixPieces())
	out := make(map[string]bool, len(keys))

	for _, k := range keys {
		out[path.Base(k)] = true
	}

	return out
}

