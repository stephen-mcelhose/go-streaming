package streaming

import (
	"context"
	"io"
)

// NoopSender discards all sent items and always returns nil.
type NoopSender[T any] struct{}

// NewNoopSender creates a NoopSender that discards all outgoing items.
func NewNoopSender[T any]() *NoopSender[T] {
	return &NoopSender[T]{}
}

// Send discards item and returns nil immediately.
func (s *NoopSender[T]) Send(ctx context.Context, item T) error {
	return nil
}

// NoopReceiver emits nothing and terminates cleanly without invoking the handler.
type NoopReceiver[T any] struct{}

// NewNoopReceiver creates a NoopReceiver that terminates immediately with nil.
func NewNoopReceiver[T any]() *NoopReceiver[T] {
	return &NoopReceiver[T]{}
}

// Receive immediately returns nil without calling handler.
func (s *NoopReceiver[T]) Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error {
	return nil
}

// EOFSender always returns io.EOF on any Send call.
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
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.sender <- item:
		return nil
	}
}

// ChanStreamCloser extends ChanStream with io.Closer to close the sender channel.
type ChanStreamCloser[T any] struct {
	*ChanStream[T]
	closeSender chan<- T
}

// NewChanStreamCloser creates a ChanStreamCloser that closes the sender channel upon Close().
func NewChanStreamCloser[T any](receiver <-chan T, sender chan<- T) *ChanStreamCloser[T] {
	return &ChanStreamCloser[T]{
		ChanStream:  NewChanStream(receiver, sender),
		closeSender: sender,
	}
}

// Close closes the underlying sender channel if present.
func (s *ChanStreamCloser[T]) Close() error {
	if s.closeSender != nil {
		close(s.closeSender)
		s.closeSender = nil
	}
	return nil
}
