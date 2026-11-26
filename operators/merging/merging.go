package merging

import (
	"context"
	"errors"
	"sync"

	"github.com/stephen-mcelhose/go-streaming"
)

// Merge combines multiple Receiver[T] inputs concurrently into a single Sender[T].
// Individual receivers completing cleanly (nil, io.EOF, or ErrStreamStop) do not cancel other receivers.
// If any receiver returns an unexpected error, Merge cancels remaining receivers and aborts with that error.
func Merge[T any](
	ctx context.Context,
	sender streaming.Sender[T],
	receivers ...streaming.Receiver[T],
) error {
	if len(receivers) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(receivers))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, r := range receivers {
		wg.Add(1)
		go func(rx streaming.Receiver[T]) {
			defer wg.Done()
			err := rx.Receive(ctx, func(ctx context.Context, item T) error {
				return sender.Send(ctx, item)
			})
			if !streaming.IsCleanTermination(err) && !errors.Is(err, context.Canceled) {
				select {
				case errCh <- err:
					cancel()
				default:
				}
			}
		}(r)
	}

	wg.Wait()
	close(errCh)

	if len(errCh) > 0 {
		return <-errCh
	}
	return nil
}

// Concat consumes multiple receivers serially in order. It exhausts receiver[0] completely,
// then receiver[1], and so on, emitting all items to sender.
//
// Concat requires bounded streams that cleanly terminate (via nil or io.EOF). If an unbounded
// stream (such as a continuous Kafka subscription) is provided as receiver[i], subsequent
// receivers will never be reached unless the prior stream is explicitly stopped.
func Concat[T any](
	ctx context.Context,
	sender streaming.Sender[T],
	receivers ...streaming.Receiver[T],
) error {
	for _, r := range receivers {
		err := r.Receive(ctx, func(ctx context.Context, item T) error {
			return sender.Send(ctx, item)
		})
		if !streaming.IsCleanTermination(err) {
			return err
		}
	}
	return nil
}
