// Package grpcstream provides adapters bridging gRPC streaming RPC interfaces to Receiver and Sender contracts.
package grpcstream

import (
	"context"
	"io"
)

// ClientStream represents an abstract gRPC client-streaming RPC interface.
type ClientStream[T any] interface {
	Send(*T) error
	CloseAndRecv() error
}

// ServerStream represents an abstract gRPC server-streaming RPC interface.
type ServerStream[T any] interface {
	Recv() (*T, error)
}

// BidiStream represents an abstract gRPC bidirectional streaming RPC interface.
type BidiStream[Req any, Resp any] interface {
	Send(*Req) error
	Recv() (*Resp, error)
}

// StreamReceiver wraps a gRPC ServerStream as a streaming.Receiver[T].
type StreamReceiver[T any] struct {
	stream ServerStream[T]
}

// NewStreamReceiver constructs a StreamReceiver wrapping a gRPC server stream.
func NewStreamReceiver[T any](stream ServerStream[T]) *StreamReceiver[T] {
	return &StreamReceiver[T]{stream: stream}
}

// Receive processes incoming gRPC messages until io.EOF is encountered or ctx is canceled.
func (r *StreamReceiver[T]) Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		item, err := r.stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if err := handler(ctx, *item); err != nil {
			return err
		}
	}
}

// StreamSender wraps a gRPC ClientStream as a streaming.Sender[T].
type StreamSender[T any] struct {
	stream ClientStream[T]
}

// NewStreamSender constructs a StreamSender wrapping a gRPC client stream.
func NewStreamSender[T any](stream ClientStream[T]) *StreamSender[T] {
	return &StreamSender[T]{stream: stream}
}

// Send delivers item to the underlying gRPC client stream.
func (s *StreamSender[T]) Send(ctx context.Context, item T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return s.stream.Send(&item)
}
