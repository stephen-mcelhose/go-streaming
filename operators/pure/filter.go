package pure

import (
	"context"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

// Filter forwards elements that satisfy the pure predicate function func(item T) bool.
func Filter[T any](
	ctx context.Context,
	predicate func(item T) bool,
	in streaming.Receiver[T],
	out streaming.Sender[T],
) error {
	return transformations.Filter(ctx, func(ctx context.Context, item T) (bool, error) {
		return predicate(item), nil
	}, in, out)
}
