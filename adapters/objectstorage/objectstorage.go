// Package objectstorage provides JSON Lines streaming reading and writing against cloud object stores.
package objectstorage

import (
	"context"
	"io"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/adapters/jsonl"
)

// ObjectStorageClient represents an abstract cloud storage provider (e.g. S3, GCS, Azure Blob).
type ObjectStorageClient interface {
	// OpenReader opens a readable stream for the given bucket and object key.
	OpenReader(ctx context.Context, bucket, key string) (io.ReadCloser, error)
	// OpenWriter opens a writable stream for the given bucket and object key.
	OpenWriter(ctx context.Context, bucket, key string) (io.WriteCloser, error)
}

// JsonlObjectStorageStream provides JSON Lines streaming reading and writing backed by cloud object storage.
type JsonlObjectStorageStream[T any] struct {
	client ObjectStorageClient
	bucket string
	key    string
}

// NewJsonlObjectStorageStream constructs an Object Storage-backed JSONL stream.
func NewJsonlObjectStorageStream[T any](client ObjectStorageClient, bucket, key string) *JsonlObjectStorageStream[T] {
	return &JsonlObjectStorageStream[T]{
		client: client,
		bucket: bucket,
		key:    key,
	}
}

// Receive reads the object stream from storage as JSONL and dispatches items to handler.
func (s *JsonlObjectStorageStream[T]) Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error {
	rc, err := s.client.OpenReader(ctx, s.bucket, s.key)
	if err != nil {
		return err
	}
	defer rc.Close()

	stream := jsonl.NewJsonlStream[T](rc, nil)
	return stream.Receive(ctx, handler)
}

// WriteAll streams all items from receiver and writes them to a storage object as JSONL.
func (s *JsonlObjectStorageStream[T]) WriteAll(ctx context.Context, source streaming.Receiver[T]) error {
	wc, err := s.client.OpenWriter(ctx, s.bucket, s.key)
	if err != nil {
		return err
	}
	defer wc.Close()

	stream := jsonl.NewJsonlStream[T](nil, wc)
	return source.Receive(ctx, func(ctx context.Context, item T) error {
		return stream.Send(ctx, item)
	})
}
