// Package messaging provides adapters bridging message brokers (e.g. Kafka, Google Cloud Pub/Sub) to Stream contracts.
package messaging

import (
	"context"
	"io"
)

// Consumer defines a message broker subscriber/consumer interface.
type Consumer[T any] interface {
	Consume(ctx context.Context, handler func(ctx context.Context, item T) error) error
}

// Producer defines a message broker publisher/producer interface.
type Producer[T any] interface {
	Produce(ctx context.Context, item T) error
}

// BrokerStream binds broker consumer and producer into a unified Stream[T] and io.Closer.
type BrokerStream[T any] struct {
	consumer Consumer[T]
	producer Producer[T]
	closer   io.Closer
}

// NewBrokerStream creates a Stream backed by messaging broker primitives.
func NewBrokerStream[T any](consumer Consumer[T], producer Producer[T], closer io.Closer) *BrokerStream[T] {
	return &BrokerStream[T]{
		consumer: consumer,
		producer: producer,
		closer:   closer,
	}
}

// Receive consumes messages from the broker consumer and delegates to handler.
func (b *BrokerStream[T]) Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error {
	if b.consumer == nil {
		return nil
	}
	return b.consumer.Consume(ctx, handler)
}

// Send produces an item to the message broker.
func (b *BrokerStream[T]) Send(ctx context.Context, item T) error {
	if b.producer == nil {
		return nil
	}
	return b.producer.Produce(ctx, item)
}

// Close releases any broker consumer or producer resources if closer was provided.
func (b *BrokerStream[T]) Close() error {
	if b.closer != nil {
		return b.closer.Close()
	}
	return nil
}
