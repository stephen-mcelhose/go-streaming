// Package math provides ergonomic streaming wrappers for pure, infallible mathematical functions.
//
// These operators assume pipeline transformations cannot fail; any underlying stream errors
// encountered will trigger a panic.
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

// Reduce wraps transformations.Reduce for pure reducer func(acc, item T) T.
// Panics if any underlying stream error occurs (except context.Canceled).
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
