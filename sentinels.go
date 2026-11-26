package streaming

import (
	"context"
	"io"
)

// NoopSender discards all sent items.
type NoopSender[T any] struct{}

func NewNoopSender[T any]() *NoopSender[T] {
	return &NoopSender[T]{}
}

func (s *NoopSender[T]) Send(ctx context.Context, item T) error {
	return nil
}

// NoopReceiver emits nothing and terminates cleanly.
type NoopReceiver[T any] struct{}

func NewNoopReceiver[T any]() *NoopReceiver[T] {
	return &NoopReceiver[T]{}
}

func (s *NoopReceiver[T]) Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error {
	return nil
}

// EOFSender returns io.EOF on send.
type EOFSender[T any] struct{}

func NewEOFSender[T any]() *EOFSender[T] {
	return &EOFSender[T]{}
}

func (s *EOFSender[T]) Send(ctx context.Context, item T) error {
	return io.EOF
}

// EOFReceiver returns io.EOF immediately on receive.
type EOFReceiver[T any] struct{}

func NewEOFReceiver[T any]() *EOFReceiver[T] {
	return &EOFReceiver[T]{}
}

func (s *EOFReceiver[T]) Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error {
	return io.EOF
}

// ChanStream adapts Go channels into Stream[T].
type ChanStream[T any] struct {
	receiver <-chan T
	sender   chan<- T
}

func NewChanStream[T any](receiver <-chan T, sender chan<- T) *ChanStream[T] {
	return &ChanStream[T]{
		receiver: receiver,
		sender:   sender,
	}
}

func (s *ChanStream[T]) Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error {
	if s.receiver == nil {
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case item, ok := <-s.receiver:
			if !ok {
				return nil
			}
			if err := handler(ctx, item); err != nil {
				return err
			}
		}
	}
}

func (s *ChanStream[T]) Send(ctx context.Context, item T) error {
	if s.sender == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.sender <- item:
		return nil
	}
}
