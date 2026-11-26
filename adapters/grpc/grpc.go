package grpcstream

import (
	"context"
	"io"
)

// ClientStream represents a gRPC client-streaming RPC.
type ClientStream[T any] interface {
	Send(*T) error
	CloseAndRecv() error
}

// ServerStream represents a gRPC server-streaming RPC.
type ServerStream[T any] interface {
	Recv() (*T, error)
}

// BidiStream represents a gRPC bidirectional stream.
type BidiStream[Req any, Resp any] interface {
	Send(*Req) error
	Recv() (*Resp, error)
}

// StreamReceiver wraps a gRPC ServerStream as a streaming.Receiver[T].
type StreamReceiver[T any] struct {
	stream ServerStream[T]
}

func NewStreamReceiver[T any](stream ServerStream[T]) *StreamReceiver[T] {
	return &StreamReceiver[T]{stream: stream}
}

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

func NewStreamSender[T any](stream ClientStream[T]) *StreamSender[T] {
	return &StreamSender[T]{stream: stream}
}

func (s *StreamSender[T]) Send(ctx context.Context, item T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return s.stream.Send(&item)
}
