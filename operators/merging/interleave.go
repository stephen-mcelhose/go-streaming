package merging

import (
	"context"

	"github.com/stephen-mcelhose/go-streaming"
)

// receiverQueue tracks an in-flight buffered worker channel and error status for a single receiver.
type receiverQueue[T any] struct {
	items chan T
	done  chan struct{}
	err   error
}

// Interleave reads from receivers in a round-robin interleaved sequence, emitting one available
// item from each active receiver cyclically until all receivers are exhausted.
//
// Unlike non-deterministic Merge, Interleave guarantees fair turn-taking across streams (e.g. A1, B1, A2, B2).
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

	queues := make([]*receiverQueue[T], len(receivers))
	for i, r := range receivers {
		q := &receiverQueue[T]{
			items: make(chan T),
			done:  make(chan struct{}),
		}
		queues[i] = q

		go func(rx streaming.Receiver[T], rq *receiverQueue[T]) {
			defer close(rq.items)
			defer close(rq.done)
			err := rx.Receive(ctx, func(ctx context.Context, item T) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case rq.items <- item:
					return nil
				}
			})
			if !streaming.IsCleanTermination(err) && err != context.Canceled {
				rq.err = err
				cancel()
			}
		}(r, q)
	}

	active := queues
	for len(active) > 0 {
		var nextActive []*receiverQueue[T]
		for _, q := range active {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case item, ok := <-q.items:
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
