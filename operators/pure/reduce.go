package pure

import (
	"context"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

// Reduce aggregates elements of type I into a terminal state object O using a pure reducer func(state O, item I) O.
func Reduce[I any, O any](
	ctx context.Context,
	initialState O,
	reducer func(state O, item I) O,
	in streaming.Receiver[I],
	out streaming.Sender[O],
) error {
	return transformations.Reduce(ctx, initialState, func(ctx context.Context, state O, item I) (O, error) {
		return reducer(state, item), nil
	}, in, out)
}

// Scan continuously accumulates state O across a stream using a pure accumulator func(state O, item I) O.
// Emits the updated state object O after each incoming item I.
func Scan[I any, O any](
	ctx context.Context,
	initialState O,
	accumulator func(state O, item I) O,
	in streaming.Receiver[I],
	out streaming.Sender[O],
) error {
	return transformations.Scan(ctx, initialState, func(ctx context.Context, state O, item I) (O, error) {
		return accumulator(state, item), nil
	}, in, out)
}
