package objectstorage_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/adapters/objectstorage"
)

type memoryObjectStorage struct {
	objects map[string][]byte
}

type nopCloserWriter struct {
	*bytes.Buffer
}

func (n *nopCloserWriter) Close() error { return nil }

func newMemoryStorage() *memoryObjectStorage {
	return &memoryObjectStorage{
		objects: make(map[string][]byte),
	}
}

func (m *memoryObjectStorage) OpenReader(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	fullKey := bucket + "/" + key
	data, ok := m.objects[fullKey]
	if !ok {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *memoryObjectStorage) OpenWriter(ctx context.Context, bucket, key string) (io.WriteCloser, error) {
	fullKey := bucket + "/" + key
	buf := new(bytes.Buffer)
	return &storageWriter{
		Buffer:  buf,
		storage: m,
		key:     fullKey,
	}, nil
}

type storageWriter struct {
	*bytes.Buffer
	storage *memoryObjectStorage
	key     string
}

func (w *storageWriter) Close() error {
	w.storage.objects[w.key] = w.Buffer.Bytes()
	return nil
}

type event struct {
	Type string `json:"type"`
	Val  int    `json:"val"`
}

func TestJsonlObjectStorageStream(t *testing.T) {
	ctx := context.Background()
	storage := newMemoryStorage()

	stream := objectstorage.NewJsonlObjectStorageStream[event](storage, "my-bucket", "events.jsonl")

	// 1. Write to object storage
	inCh := make(chan event, 2)
	inCh <- event{Type: "login", Val: 1}
	inCh <- event{Type: "logout", Val: 2}
	close(inCh)

	sourceStream := streaming.NewChanStream(inCh, nil)
	err := stream.WriteAll(ctx, sourceStream)
	require.NoError(t, err)

	// 2. Read back from object storage
	var readBack []event
	err = stream.Receive(ctx, func(ctx context.Context, item event) error {
		readBack = append(readBack, item)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, readBack, 2)
	require.Equal(t, "login", readBack[0].Type)
	require.Equal(t, "logout", readBack[1].Type)
}
