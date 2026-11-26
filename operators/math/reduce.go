package math

import (
	"context"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

// Reduce wraps transformations.Reduce for pure reducer functions func(state O, item I) O.
// Consumes a bounded stream to completion and emits a single accumulated state O to out.
// Panics if any underlying stream error occurs (except context.Canceled).
func Reduce[I any, O any](
	ctx context.Context,
	initialState O,
	reducer func(state O, item I) O,
	in streaming.Receiver[I],
	out streaming.Sender[O],
) {
	err := transformations.Reduce(ctx, initialState, func(ctx context.Context, state O, item I) (O, error) {
		return reducer(state, item), nil
	}, in, out)
	if err != nil && err != context.Canceled {
		panic(err)
	}
}

// Scan wraps transformations.Scan for pure accumulator functions func(state O, item I) O.
// Emits the updated accumulated state O after each incoming item I.
// Panics if any underlying stream error occurs (except context.Canceled).
func Scan[I any, O any](
	ctx context.Context,
	initialState O,
	accumulator func(state O, item I) O,
	in streaming.Receiver[I],
	out streaming.Sender[O],
) {
	err := transformations.Scan(ctx, initialState, func(ctx context.Context, state O, item I) (O, error) {
		return accumulator(state, item), nil
	}, in, out)
	if err != nil && err != context.Canceled {
		panic(err)
	}
}
