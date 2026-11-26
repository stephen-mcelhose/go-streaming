package streaming

import (
	"context"
	"io"
)

// NoopSender discards all sent items and returns nil.
type NoopSender[T any] struct{}

// NewNoopSender creates a NoopSender.
func NewNoopSender[T any]() *NoopSender[T] {
	return &NoopSender[T]{}
}

// Send discards item and returns nil.
func (s *NoopSender[T]) Send(ctx context.Context, item T) error {
	return nil
}

// NoopReceiver never yields items to handler and returns nil immediately on Receive.
type NoopReceiver[T any] struct{}

// NewNoopReceiver creates a NoopReceiver.
func NewNoopReceiver[T any]() *NoopReceiver[T] {
	return &NoopReceiver[T]{}
}

// Receive immediately returns nil without invoking handler.
func (s *NoopReceiver[T]) Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error {
	return nil
}

// EOFSender always returns io.EOF on Send without accepting the item.
type EOFSender[T any] struct{}

// NewEOFSender creates an EOFSender.
func NewEOFSender[T any]() *EOFSender[T] {
	return &EOFSender[T]{}
}

// Send always returns io.EOF.
func (s *EOFSender[T]) Send(ctx context.Context, item T) error {
	return io.EOF
}

// EOFReceiver always returns io.EOF immediately on Receive without calling handler.
type EOFReceiver[T any] struct{}

// NewEOFReceiver creates an EOFReceiver.
func NewEOFReceiver[T any]() *EOFReceiver[T] {
	return &EOFReceiver[T]{}
}

// Receive always returns io.EOF without invoking handler.
func (s *EOFReceiver[T]) Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error {
	return io.EOF
}

// ChanStream adapts Go channels into a Stream[T].
//
// Lifecycle & Channel Ownership:
// Following standard Go channel idioms, the creator/sender owns channel lifecycle:
//   - Ingest receiver channel (`<-chan T`) is closed by its producer to signal end-of-stream.
//   - Egress sender channel (`chan<- T`) lifecycle is owned by the creator. Sending to a closed
//     channel panics in Go; callers should ensure the destination channel remains open for the lifetime
//     of the sender pipeline or use ChanStreamCloser when ownership is transferred.
type ChanStream[T any] struct {
	receiver <-chan T
	sender   chan<- T
}

// NewChanStream creates a ChanStream wrapping an ingest receiver channel and an egress sender channel.
// Either channel may be nil if only half-duplex behavior is desired.
func NewChanStream[T any](receiver <-chan T, sender chan<- T) *ChanStream[T] {
	return &ChanStream[T]{
		receiver: receiver,
		sender:   sender,
	}
}

// Receive drains items from the underlying channel, invoking handler for each item
// until the channel is closed, ctx is canceled, or handler returns an error.
func (s *ChanStream[T]) Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error {
	if s.receiver == nil {
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
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

// Send pushes item into the underlying channel, selecting on ctx.Done() to prevent blocking leaks.
func (s *ChanStream[T]) Send(ctx context.Context, item T) error {
	if s.sender == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.sender <- item:
		return nil
	}
}

// ChanStreamCloser extends ChanStream with explicit io.Closer semantics.
// It is useful when stream producer ownership is transferred and the underlying
// channel must be closed upon pipeline termination.
type ChanStreamCloser[T any] struct {
	*ChanStream[T]
	closeSender func() error
}

// NewChanStreamCloser creates a ChanStreamCloser wrapping an ingest receiver channel and an egress sender channel.
// Calling Close() closes the sender channel if non-nil.
func NewChanStreamCloser[T any](receiver <-chan T, sender chan T) *ChanStreamCloser[T] {
	return &ChanStreamCloser[T]{
		ChanStream: NewChanStream[T](receiver, sender),
		closeSender: func() error {
			if sender != nil {
				close(sender)
			}
			return nil
		},
	}
}

// Close closes the underlying sender channel.
func (c *ChanStreamCloser[T]) Close() error {
	if c.closeSender != nil {
		return c.closeSender()
	}
	return nil
}
