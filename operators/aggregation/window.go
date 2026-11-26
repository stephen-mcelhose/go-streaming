package aggregation

import (
	"context"
	"time"

	"github.com/stephen-mcelhose/go-streaming"
)

// TumblingWindow groups stream items into non-overlapping, continuous time windows of fixed duration.
func TumblingWindow[T any](
	ctx context.Context,
	windowSize time.Duration,
	in streaming.Receiver[T],
	out streaming.Sender[[]T],
) error {
	return Batch(ctx, BatchConfig{MaxWait: windowSize}, in, out)
}
