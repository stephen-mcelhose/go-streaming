package routing

import (
	"context"
	"sync"

	"github.com/stephen-mcelhose/go-streaming"
)

// Broadcast replicates each received item to every target sender concurrently.
// If any target fails to send, Broadcast collects and returns the first encountered error.
func Broadcast[T any](
	ctx context.Context,
	in streaming.Receiver[T],
	targets ...streaming.Sender[T],
) error {
	if len(targets) == 0 {
		return nil
	}

	return in.Receive(ctx, func(ctx context.Context, item T) error {
		var wg sync.WaitGroup
		errCh := make(chan error, len(targets))

		for _, t := range targets {
			wg.Add(1)
			go func(sender streaming.Sender[T]) {
				defer wg.Done()
				if err := sender.Send(ctx, item); err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
			}(t)
		}

		wg.Wait()
		close(errCh)

		if len(errCh) > 0 {
			return <-errCh
		}
		return nil
	})
}

// BroadcastBestEffort replicates each received item to every target sender concurrently,
// continuing delivery to all destinations even if one or more targets encounter errors.
func BroadcastBestEffort[T any](
	ctx context.Context,
	in streaming.Receiver[T],
	targets ...streaming.Sender[T],
) error {
	if len(targets) == 0 {
		return nil
	}

	return in.Receive(ctx, func(ctx context.Context, item T) error {
		var wg sync.WaitGroup
		for _, t := range targets {
			wg.Add(1)
			go func(sender streaming.Sender[T]) {
				defer wg.Done()
				_ = sender.Send(ctx, item)
			}(t)
		}
		wg.Wait()
		return nil
	})
}
