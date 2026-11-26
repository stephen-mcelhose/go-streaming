package math

import (
	"context"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

// Map wraps transformations.Map for pure infallible functions func(item I) O.
// Panics if any underlying stream error occurs (except context.Canceled).
func Map[I any, O any](
	ctx context.Context,
	mapper func(item I) O,
	in streaming.Receiver[I],
	out streaming.Sender[O],
) {
	err := transformations.Map(ctx, func(ctx context.Context, item I) (O, error) {
		return mapper(item), nil
	}, in, out)
	if err != nil && err != context.Canceled {
		panic(err)
	}
}

// MapWithContext wraps transformations.Map for pure functions requiring context inspection func(ctx, item I) O.
// Panics if any underlying stream error occurs (except context.Canceled).
func MapWithContext[I any, O any](
	ctx context.Context,
	mapper func(ctx context.Context, item I) O,
	in streaming.Receiver[I],
	out streaming.Sender[O],
) {
	err := transformations.Map(ctx, func(ctx context.Context, item I) (O, error) {
		return mapper(ctx, item), nil
	}, in, out)
	if err != nil && err != context.Canceled {
		panic(err)
	}
}
