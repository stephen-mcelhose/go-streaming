package routing

import (
	"context"

	"github.com/stephen-mcelhose/go-streaming"
)

// RoundRobin distributes incoming items cyclically across the provided target senders.
func RoundRobin[T any](
	ctx context.Context,
	in streaming.Receiver[T],
	targets ...streaming.Sender[T],
) error {
	if len(targets) == 0 {
		return nil
	}

	idx := 0
	return in.Receive(ctx, func(ctx context.Context, item T) error {
		target := targets[idx%len(targets)]
		idx++
		return target.Send(ctx, item)
	})
}
