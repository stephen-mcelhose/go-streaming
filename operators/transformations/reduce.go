package transformations

import (
	"context"

	"github.com/stephen-mcelhose/go-streaming"
)

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
