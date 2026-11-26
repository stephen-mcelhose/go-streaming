package control

import (
	"context"
	"time"

	"github.com/stephen-mcelhose/go-streaming"
)

// Debounce emits an item to out only after quietDuration has elapsed without any newer item arriving.
// If the stream ends while a pending item is waiting, Debounce flushes the trailing item before terminating.
func Debounce[T any](
	ctx context.Context,
	quietDuration time.Duration,
	in streaming.Receiver[T],
	out streaming.Sender[T],
) error {
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

	var latest T
	var hasLatest bool
	var timer *time.Timer
	var timerC <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return ctx.Err()

		case <-timerC:
			if hasLatest {
				if err := out.Send(ctx, latest); err != nil {
					return err
				}
				hasLatest = false
				timerC = nil
			}

		case item, ok := <-itemCh:
			if !ok {
				if hasLatest {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						if err := out.Send(ctx, latest); err != nil {
							return err
						}
					}
				}
				return <-errCh
			}

			latest = item
			hasLatest = true
			if timer != nil {
				timer.Stop()
			}
			timer = time.NewTimer(quietDuration)
			timerC = timer.C
		}
	}
}
