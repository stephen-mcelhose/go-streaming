package transformations

import (
	"context"

	"github.com/stephen-mcelhose/go-streaming"
)

// FlatMap transforms each item of type I into a sub-stream streaming.Receiver[O],
// forwarding all emitted items from the sub-stream into the out Sender.
func FlatMap[I any, O any](
	ctx context.Context,
	mapper func(ctx context.Context, item I) (streaming.Receiver[O], error),
	in streaming.Receiver[I],
	out streaming.Sender[O],
) error {
	return in.Receive(ctx, func(ctx context.Context, item I) error {
		subReceiver, err := mapper(ctx, item)
		if err != nil {
			return err
		}
		if subReceiver == nil {
			return nil
		}
		return subReceiver.Receive(ctx, func(ctx context.Context, o O) error {
			return out.Send(ctx, o)
		})
	})
}
