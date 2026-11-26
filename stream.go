package streaming

import (
	"context"
	"io"
)

// Receiver consumes items from a stream and passes them to the handler callback.
type Receiver[T any] interface {
	Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error
}

// Sender publishes items into a stream.
type Sender[T any] interface {
	Send(ctx context.Context, item T) error
}

// Stream combines both Receiver and Sender contracts for bidirectional/pass-through streams.
type Stream[T any] interface {
	Receiver[T]
	Sender[T]
}

// ReceiverCloser defines a Receiver that can be closed.
type ReceiverCloser[T any] interface {
	Receiver[T]
	io.Closer
}

// SenderCloser defines a Sender that can be closed.
type SenderCloser[T any] interface {
	Sender[T]
	io.Closer
}

// StreamCloser defines a Stream that can be closed.
type StreamCloser[T any] interface {
	Stream[T]
	io.Closer
}
