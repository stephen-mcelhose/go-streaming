package transformations

import (
	"context"

	"github.com/stephen-mcelhose/go-streaming"
)

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
