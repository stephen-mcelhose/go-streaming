// Package streaming provides composable, typed primitives and interfaces for stream processing in Go.
package streaming

import (
	"context"
	"io"
)

// Receiver consumes items from a stream and passes them to the handler callback.
//
// Implementations should invoke handler sequentially or concurrently according to
// their concurrency guarantees and return any terminal error encountered during consumption.
type Receiver[T any] interface {
	// Receive processes items from the stream until the stream is exhausted,
	// the context is canceled, or the handler returns an error.
	Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error
}

// Sender publishes items into a stream or downstream sink.
type Sender[T any] interface {
	// Send publishes a single item into the stream, blocking if necessary until
	// the item is accepted, the context is canceled, or an error occurs.
	Send(ctx context.Context, item T) error
}

// Stream combines both Receiver and Sender contracts for bidirectional or pass-through streams.
type Stream[T any] interface {
	Receiver[T]
	Sender[T]
}

// ReceiverCloser defines a Receiver that must release underlying resources upon completion.
type ReceiverCloser[T any] interface {
	Receiver[T]
	io.Closer
}

// SenderCloser defines a Sender that must be closed to signal completion or flush buffered state.
type SenderCloser[T any] interface {
	Sender[T]
	io.Closer
}

// StreamCloser defines a Stream that requires explicit resource cleanup via io.Closer.
type StreamCloser[T any] interface {
	Stream[T]
	io.Closer
}
