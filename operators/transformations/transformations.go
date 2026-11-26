// Package transformations provides context-aware stream transformation operators.
package transformations

import (
	"context"
	"time"

	"github.com/stephen-mcelhose/go-streaming"
)

// Map transforms items of type I into type O using a context and error-aware mapping function,
// streaming each mapped output into the out Sender.
func Map[I any, O any](
	ctx context.Context,
	mapper func(ctx context.Context, item I) (O, error),
	in streaming.Receiver[I],
	out streaming.Sender[O],
) error {
	return in.Receive(ctx, func(ctx context.Context, item I) error {
		mapped, err := mapper(ctx, item)
		if err != nil {
			return err
		}
		return out.Send(ctx, mapped)
	})
}

// FlatMap transforms items of type I into slices of O and sends each element individually to out.
func FlatMap[I any, O any](
	ctx context.Context,
	mapper func(ctx context.Context, item I) ([]O, error),
	in streaming.Receiver[I],
	out streaming.Sender[O],
) error {
	return in.Receive(ctx, func(ctx context.Context, item I) error {
		mapped, err := mapper(ctx, item)
		if err != nil {
			return err
		}
		for _, o := range mapped {
			if err := out.Send(ctx, o); err != nil {
				return err
			}
		}
		return nil
	})
}

// Filter passes items that satisfy the predicate function to out, discarding the rest.
func Filter[T any](
	ctx context.Context,
	predicate func(ctx context.Context, item T) (bool, error),
	in streaming.Receiver[T],
	out streaming.Sender[T],
) error {
	return in.Receive(ctx, func(ctx context.Context, item T) error {
		keep, err := predicate(ctx, item)
		if err != nil {
			return err
		}
		if keep {
			return out.Send(ctx, item)
		}
		return nil
	})
}

// Reduce aggregates items in stream using reducer function and sends the final accumulated result to out.
func Reduce[T any](
	ctx context.Context,
	initial T,
	reducer func(ctx context.Context, acc T, item T) (T, error),
	in streaming.Receiver[T],
	out streaming.Sender[T],
) error {
	acc := initial
	hasItems := false
	err := in.Receive(ctx, func(ctx context.Context, item T) error {
		var rErr error
		acc, rErr = reducer(ctx, acc, item)
		if rErr != nil {
			return rErr
		}
		hasItems = true
		return nil
	})
	if err != nil {
		return err
	}
	if hasItems {
		return out.Send(ctx, acc)
	}
	return nil
}

// Batch groups received items into slices of up to maxBatchSize or flushes when maxWait duration elapses.
func Batch[T any](
	ctx context.Context,
	maxBatchSize int,
	maxWait time.Duration,
	in streaming.Receiver[T],
	out streaming.Sender[[]T],
) error {
	if maxBatchSize <= 0 {
		maxBatchSize = 1
	}

	itemCh := make(chan T)
	errCh := make(chan error, 1)

	// Ingest routine pushes from Receiver into channel
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
			if len(batch) >= maxBatchSize {
				if err := flush(); err != nil {
					return err
				}
			} else if maxWait > 0 && timerCh == nil {
				timer = time.NewTimer(maxWait)
				timerCh = timer.C
			}
		}
	}
}
