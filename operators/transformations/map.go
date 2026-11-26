package transformations

import (
	"context"

	"github.com/stephen-mcelhose/go-streaming"
)

// Map transforms items of type I into type O using a context and error-aware mapping function,
// streaming each mapped output into the out Sender.
func Map[I any, O any](
	ctx context.Context,
	mapper func(ctx context.Context, item I) (O, error),
	in streaming.Receiver[I],
	out streaming.Sender[O],
) error {
	return in.Receive(ctx, func(ctx context.Context, item I) error {
		mapped, err := mapper(ctx, item)
		if err != nil {
			return err
		}
		return out.Send(ctx, mapped)
	})
}
