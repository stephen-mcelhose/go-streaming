package math

import (
	"context"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

// Filter wraps transformations.Filter for pure predicate func(item T) bool.
// Panics if any underlying stream error occurs (except context.Canceled).
func Filter[T any](
	ctx context.Context,
	predicate func(item T) bool,
	in streaming.Receiver[T],
	out streaming.Sender[T],
) {
	err := transformations.Filter(ctx, func(ctx context.Context, item T) (bool, error) {
		return predicate(item), nil
	}, in, out)
	if err != nil && err != context.Canceled {
		panic(err)
	}
}
