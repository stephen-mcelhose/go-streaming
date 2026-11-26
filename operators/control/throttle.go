package control

import (
	"context"
	"time"

	"github.com/stephen-mcelhose/go-streaming"
)

// Throttle limits the throughput of items emitted to out to at most one item per interval.
//
// Throttle uses a fixed time.Ticker to guarantee minimum spacing between elements.
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
