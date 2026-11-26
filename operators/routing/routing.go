// Package routing provides fan-out, round-robin, and key-partitioned stream routing operators.
package routing

import (
	"context"
	"fmt"
	"sync"

	"github.com/stephen-mcelhose/go-streaming"
)

// Broadcast replicates each received item to every target sender concurrently.
// If any sender returns an error, the error is collected and returned.
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
					errCh <- err
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

// RoundRobin distributes incoming items cyclically across the provided target senders.
func RoundRobin[T any](
	ctx context.Context,
	in streaming.Receiver[T],
	targets ...streaming.Sender[T],
) error {
	if len(targets) == 0 {
		return nil
	}

	idx := 0
	return in.Receive(ctx, func(ctx context.Context, item T) error {
		target := targets[idx%len(targets)]
		idx++
		return target.Send(ctx, item)
	})
}

// Partition routes incoming items to keyed senders using a key extractor function.
// Returns an error if an item resolves to a key without an assigned sender route.
func Partition[T any, K comparable](
	ctx context.Context,
	keyFn func(item T) (K, error),
	routes map[K]streaming.Sender[T],
	in streaming.Receiver[T],
) error {
	return in.Receive(ctx, func(ctx context.Context, item T) error {
		k, err := keyFn(item)
		if err != nil {
			return err
		}

		sender, ok := routes[k]
		if !ok {
			return fmt.Errorf("no partition sender for key %v", k)
		}
		return sender.Send(ctx, item)
	})
}
