package pure

import (
	"context"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

// Map transforms elements 1-to-1 using a pure, infallible mapping function func(item I) O.
func Map[I any, O any](
	ctx context.Context,
	mapper func(item I) O,
	in streaming.Receiver[I],
	out streaming.Sender[O],
) error {
	return transformations.Map(ctx, func(ctx context.Context, item I) (O, error) {
		return mapper(item), nil
	}, in, out)
}

// MapWithContext transforms elements using a pure mapping function that inspects context func(ctx, item I) O.
func MapWithContext[I any, O any](
	ctx context.Context,
	mapper func(ctx context.Context, item I) O,
	in streaming.Receiver[I],
	out streaming.Sender[O],
) error {
	return transformations.Map(ctx, func(ctx context.Context, item I) (O, error) {
		return mapper(ctx, item), nil
	}, in, out)
}
