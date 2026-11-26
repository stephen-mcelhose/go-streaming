package messaging_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming/adapters/messaging"
)

type mockConsumer[T any] struct {
	items []T
}

func (m *mockConsumer[T]) Consume(ctx context.Context, handler func(ctx context.Context, item T) error) error {
	for _, it := range m.items {
		if err := handler(ctx, it); err != nil {
			return err
		}
	}
	return nil
}

type mockProducer[T any] struct {
	produced []T
}

func (m *mockProducer[T]) Produce(ctx context.Context, item T) error {
	m.produced = append(m.produced, item)
	return nil
}

type mockCloser struct {
	closed bool
}

func (m *mockCloser) Close() error {
	m.closed = true
	return nil
}

func TestBrokerStream(t *testing.T) {
	ctx := context.Background()

	consumer := &mockConsumer[string]{items: []string{"msg1", "msg2"}}
	producer := &mockProducer[string]{}
	closer := &mockCloser{}

	stream := messaging.NewBrokerStream[string](consumer, producer, closer)

	var received []string
	err := stream.Receive(ctx, func(ctx context.Context, item string) error {
		received = append(received, item)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"msg1", "msg2"}, received)

	err = stream.Send(ctx, "out1")
	require.NoError(t, err)
	require.Equal(t, []string{"out1"}, producer.produced)

	err = stream.Close()
	require.NoError(t, err)
	require.True(t, closer.closed)
}
