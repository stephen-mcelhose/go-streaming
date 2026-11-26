package control

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/stephen-mcelhose/go-streaming"
)

// RetryPolicy defines configurable backoff parameters and optional fallback routing for RetrySender.
type RetryPolicy[T any] struct {
	// MaxAttempts is the total number of attempts (initial attempt + retries). Defaults to 3.
	MaxAttempts int
	// InitialBackoff is the base delay duration before the first retry.
	InitialBackoff time.Duration
	// MaxBackoff caps the maximum backoff duration across exponential increases.
	MaxBackoff time.Duration
	// Multiplier scales InitialBackoff exponentially between attempts (e.g. 2.0).
	Multiplier float64
	// FallbackSender is an optional downstream destination (such as a Dead Letter Queue)
	// that receives items when all retry attempts fail.
	FallbackSender streaming.Sender[T]
}

// RetrySender wraps an underlying Sender with exponential retry backoff and fallback routing.
type RetrySender[T any] struct {
	underlying streaming.Sender[T]
	policy     RetryPolicy[T]
	mu         sync.Mutex
}

// NewRetrySender creates a resilient Sender configured with the specified RetryPolicy.
func NewRetrySender[T any](underlying streaming.Sender[T], policy RetryPolicy[T]) *RetrySender[T] {
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 3
	}
	if policy.Multiplier < 1.0 {
		policy.Multiplier = 1.0
	}
	return &RetrySender[T]{
		underlying: underlying,
		policy:     policy,
	}
}

// Send attempts to deliver item to the underlying Sender, retrying according to RetryPolicy.
// If all retries fail and FallbackSender is configured, the item is forwarded to FallbackSender.
func (r *RetrySender[T]) Send(ctx context.Context, item T) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var lastErr error
	backoff := r.policy.InitialBackoff

	for attempt := 1; attempt <= r.policy.MaxAttempts; attempt++ {
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

		if attempt < r.policy.MaxAttempts && backoff > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}

			if r.policy.Multiplier > 1.0 {
				nextBackoff := time.Duration(float64(backoff) * r.policy.Multiplier)
				if r.policy.MaxBackoff > 0 && nextBackoff > r.policy.MaxBackoff {
					backoff = r.policy.MaxBackoff
				} else {
					backoff = nextBackoff
				}
			}
		}
	}

	// If fallback is configured, forward exhausted item
	if r.policy.FallbackSender != nil {
		if fbErr := r.policy.FallbackSender.Send(ctx, item); fbErr != nil {
			return errors.Join(lastErr, fbErr)
		}
		return nil
	}

	if lastErr != nil {
		return lastErr
	}
	return errors.New("send failed after retry exhaustion")
}
