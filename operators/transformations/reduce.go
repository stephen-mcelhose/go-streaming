package transformations

import (
	"context"

	"github.com/stephen-mcelhose/go-streaming"
)

// Reduce aggregates elements of type I into a terminal state object of type O for bounded streams.
// It consumes the entire stream until exhaustion, and emits the final accumulated state to out.
func Reduce[I any, O any](
	ctx context.Context,
	initialState O,
	reducer func(ctx context.Context, state O, item I) (O, error),
	in streaming.Receiver[I],
	out streaming.Sender[O],
) error {
	state := initialState
	hasItems := false
	err := in.Receive(ctx, func(ctx context.Context, item I) error {
		var rErr error
		state, rErr = reducer(ctx, state, item)
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
		return out.Send(ctx, state)
	}
	return nil
}

// Scan continuously accumulates state O across an unbounded or bounded stream.
// After every incoming item I, it emits the newly updated state object O to out.
func Scan[I any, O any](
	ctx context.Context,
	initialState O,
	accumulator func(ctx context.Context, state O, item I) (O, error),
	in streaming.Receiver[I],
	out streaming.Sender[O],
) error {
	state := initialState
	return in.Receive(ctx, func(ctx context.Context, item I) error {
		var err error
		state, err = accumulator(ctx, state, item)
		if err != nil {
			return err
		}
		return out.Send(ctx, state)
	})
}
