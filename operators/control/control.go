// Package control provides flow control, rate limiting, debouncing, and retry operators.
package control

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/stephen-mcelhose/go-streaming"
)

// Throttle limits the throughput of items emitted to out to at most one item per interval.
func Throttle[T any](
	ctx context.Context,
	interval time.Duration,
	in streaming.Receiver[T],
	out streaming.Sender[T],
) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	return in.Receive(ctx, func(ctx context.Context, item T) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			return out.Send(ctx, item)
		}
	})
}

// Debounce emits an item to out only after quietDuration has elapsed without any newer item arriving.
func Debounce[T any](
	ctx context.Context,
	quietDuration time.Duration,
	in streaming.Receiver[T],
	out streaming.Sender[T],
) error {
	itemCh := make(chan T)
	errCh := make(chan error, 1)

	go func() {
		defer close(itemCh)
		err := in.Receive(ctx, func(ctx context.Context, item T) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case itemCh <- item:
				return nil
			}
		})
		errCh <- err
	}()

	var latest T
	var hasLatest bool
	var timer *time.Timer
	var timerC <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return ctx.Err()

		case <-timerC:
			if hasLatest {
				if err := out.Send(ctx, latest); err != nil {
					return err
				}
				hasLatest = false
				timerC = nil
			}

		case item, ok := <-itemCh:
			if !ok {
				if hasLatest {
					if err := out.Send(ctx, latest); err != nil {
						return err
					}
				}
				return <-errCh
			}

			latest = item
			hasLatest = true
			if timer != nil {
				timer.Stop()
			}
			timer = time.NewTimer(quietDuration)
			timerC = timer.C
		}
	}
}

// RetrySender wraps an underlying Sender and retries failed sends up to maxAttempts with exponential or constant backoff delay.
type RetrySender[T any] struct {
	underlying  streaming.Sender[T]
	maxAttempts int
	backoff     time.Duration
	mu          sync.Mutex
}

// NewRetrySender creates a resilient Sender configured to retry failed Send operations.
func NewRetrySender[T any](underlying streaming.Sender[T], maxAttempts int, backoff time.Duration) *RetrySender[T] {
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	return &RetrySender[T]{
		underlying:  underlying,
		maxAttempts: maxAttempts,
		backoff:     backoff,
	}
}

// Send attempts to deliver item to the underlying Sender, retrying up to maxAttempts on failure.
func (r *RetrySender[T]) Send(ctx context.Context, item T) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var lastErr error
	for attempt := 1; attempt <= r.maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := r.underlying.Send(ctx, item)
		if err == nil {
			return nil
		}
		lastErr = err

		if attempt < r.maxAttempts && r.backoff > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(r.backoff):
			}
		}
	}

	if lastErr != nil {
		return lastErr
	}
	return errors.New("send failed after retries")
}
