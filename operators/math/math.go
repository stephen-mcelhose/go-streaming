package math

import (
	"context"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

// Map wraps transformations.Map for pure infallible functions func(I) O.
// Panics if any underlying stream error occurs.
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

// MapWithContext wraps transformations.Map with context awareness for pure functions func(ctx, I) O.
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

// Filter wraps transformations.Filter for pure predicate func(T) bool.
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

// Reduce wraps transformations.Reduce for pure reducer func(T, T) T.
func Reduce[T any](
	ctx context.Context,
	initial T,
	reducer func(acc T, item T) T,
	in streaming.Receiver[T],
	out streaming.Sender[T],
) {
	err := transformations.Reduce(ctx, initial, func(ctx context.Context, acc, item T) (T, error) {
		return reducer(acc, item), nil
	}, in, out)
	if err != nil && err != context.Canceled {
		panic(err)
	}
}
