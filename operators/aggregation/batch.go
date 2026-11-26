package aggregation

import (
	"context"
	"time"

	"github.com/stephen-mcelhose/go-streaming"
)

// BatchConfig configures count and time thresholds for stream batching.
type BatchConfig struct {
	// MaxSize is the maximum number of items before a batch is emitted.
	MaxSize int
	// MaxWait is the maximum duration to wait before emitting a partial batch.
	MaxWait time.Duration
}

// Batch groups items into slices using combined count and time window triggers.
func Batch[T any](
	ctx context.Context,
	cfg BatchConfig,
	in streaming.Receiver[T],
	out streaming.Sender[[]T],
) error {
	if cfg.MaxSize <= 0 && cfg.MaxWait <= 0 {
		cfg.MaxSize = 1
	}

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

	var batch []T
	var timer *time.Timer
	var timerCh <-chan time.Time

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		toSend := batch
		batch = nil
		if timer != nil {
			timer.Stop()
			timerCh = nil
		}
		return out.Send(ctx, toSend)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-timerCh:
			if err := flush(); err != nil {
				return err
			}

		case item, ok := <-itemCh:
			if !ok {
				if err := flush(); err != nil {
					return err
				}
				return <-errCh
			}
			batch = append(batch, item)
			if cfg.MaxSize > 0 && len(batch) >= cfg.MaxSize {
				if err := flush(); err != nil {
					return err
				}
			} else if cfg.MaxWait > 0 && timerCh == nil {
				timer = time.NewTimer(cfg.MaxWait)
				timerCh = timer.C
			}
		}
	}
}

// BatchCount batches items strictly by maximum count.
func BatchCount[T any](ctx context.Context, maxSize int, in streaming.Receiver[T], out streaming.Sender[[]T]) error {
	return Batch(ctx, BatchConfig{MaxSize: maxSize}, in, out)
}

// BatchTime batches items strictly by time duration.
func BatchTime[T any](ctx context.Context, maxWait time.Duration, in streaming.Receiver[T], out streaming.Sender[[]T]) error {
	return Batch(ctx, BatchConfig{MaxWait: maxWait}, in, out)
}
