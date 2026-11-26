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

// Interleave reads from receivers in a round-robin interleaved sequence, emitting one available
// item from each active receiver cyclically until all receivers are exhausted.
func Interleave[T any](
	ctx context.Context,
	sender streaming.Sender[T],
	receivers ...streaming.Receiver[T],
) error {
	if len(receivers) == 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type streamQueue struct {
		ch   chan T
		done chan struct{}
		err  error
	}

	queues := make([]*streamQueue, len(receivers))
	for i, r := range receivers {
		sq := &streamQueue{
			ch:   make(chan T),
			done: make(chan struct{}),
		}
		queues[i] = sq

		go func(rx streaming.Receiver[T], q *streamQueue) {
			defer close(q.ch)
			defer close(q.done)
			err := rx.Receive(ctx, func(ctx context.Context, item T) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case q.ch <- item:
					return nil
				}
			})
			if !streaming.IsCleanTermination(err) && !errors.Is(err, context.Canceled) {
				q.err = err
				cancel()
			}
		}(r, sq)
	}

	active := queues
	for len(active) > 0 {
		var nextActive []*streamQueue
		for _, q := range active {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case item, ok := <-q.ch:
				if ok {
					if err := sender.Send(ctx, item); err != nil {
						return err
					}
					nextActive = append(nextActive, q)
				} else {
					if q.err != nil {
						return q.err
					}
				}
			}
		}
		active = nextActive
	}

	return nil
}
