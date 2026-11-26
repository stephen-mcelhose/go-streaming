package routing

import (
	"context"
	"fmt"

	"github.com/stephen-mcelhose/go-streaming"
)

// Partition routes incoming items to keyed senders using a key extractor function.
// If an extracted key does not exist in routes:
//   - If defaultRoute is non-nil, the item is forwarded to defaultRoute (e.g. unclassified/DLQ).
//   - If defaultRoute is nil, Partition returns an error.
func Partition[T any, K comparable](
	ctx context.Context,
	keyFn func(item T) (K, error),
	routes map[K]streaming.Sender[T],
	defaultRoute streaming.Sender[T],
	in streaming.Receiver[T],
) error {
	return in.Receive(ctx, func(ctx context.Context, item T) error {
		k, err := keyFn(item)
		if err != nil {
			return err
		}

		sender, ok := routes[k]
		if ok {
			return sender.Send(ctx, item)
		}

		if defaultRoute != nil {
			return defaultRoute.Send(ctx, item)
		}

		return fmt.Errorf("no partition sender for key %v and no default route configured", k)
	})
}
