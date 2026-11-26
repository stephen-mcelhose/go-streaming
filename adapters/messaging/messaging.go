package messaging

import (
	"context"
	"io"
)

// Consumer is a message broker consumer interface (Kafka / PubSub).
type Consumer[T any] interface {
	Consume(ctx context.Context, handler func(ctx context.Context, item T) error) error
}

// Producer is a message broker producer interface (Kafka / PubSub).
type Producer[T any] interface {
	Produce(ctx context.Context, item T) error
}

// BrokerStream binds broker consumer and producer into a Stream[T].
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

func (b *BrokerStream[T]) Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error {
	if b.consumer == nil {
		return nil
	}
	return b.consumer.Consume(ctx, handler)
}

func (b *BrokerStream[T]) Send(ctx context.Context, item T) error {
	if b.producer == nil {
		return nil
	}
	return b.producer.Produce(ctx, item)
}

func (b *BrokerStream[T]) Close() error {
	if b.closer != nil {
		return b.closer.Close()
	}
	return nil
}
