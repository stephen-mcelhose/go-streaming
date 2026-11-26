package routing_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/routing"
)

func TestPartition(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST route to matching keyed senders", func(t *testing.T) {
		inCh := make(chan string, 3)
		inCh <- "even"
		inCh <- "odd"
		inCh <- "even"
		close(inCh)

		evenCh := make(chan string, 2)
		oddCh := make(chan string, 1)

		routes := map[string]streaming.Sender[string]{
			"even": streaming.NewChanStream(nil, evenCh),
			"odd":  streaming.NewChanStream(nil, oddCh),
		}

		err := routing.Partition(ctx, func(item string) (string, error) {
			return item, nil
		}, routes, nil, streaming.NewChanStream(inCh, nil))

		require.NoError(t, err)
		close(evenCh)
		close(oddCh)

		var evenGot, oddGot []string
		for it := range evenCh {
			evenGot = append(evenGot, it)
		}
		for it := range oddCh {
			oddGot = append(oddGot, it)
		}

		require.Equal(t, []string{"even", "even"}, evenGot)
		require.Equal(t, []string{"odd"}, oddGot)
	})

	t.Run("MUST fallback to defaultRoute when key is missing", func(t *testing.T) {
		inCh := make(chan string, 2)
		inCh <- "known"
		inCh <- "unknown"
		close(inCh)

		knownCh := make(chan string, 1)
		dlqCh := make(chan string, 1)

		routes := map[string]streaming.Sender[string]{
			"known": streaming.NewChanStream(nil, knownCh),
		}

		err := routing.Partition(ctx, func(item string) (string, error) {
			return item, nil
		}, routes, streaming.NewChanStream(nil, dlqCh), streaming.NewChanStream(inCh, nil))

		require.NoError(t, err)
		close(knownCh)
		close(dlqCh)

		require.Equal(t, "known", <-knownCh)
		require.Equal(t, "unknown", <-dlqCh)
	})

	t.Run("MUST error when key is missing and defaultRoute is nil", func(t *testing.T) {
		inCh := make(chan string, 1)
		inCh <- "unknown"
		close(inCh)

		routes := map[string]streaming.Sender[string]{}

		err := routing.Partition(ctx, func(item string) (string, error) {
			return item, nil
		}, routes, nil, streaming.NewChanStream(inCh, nil))

		require.Error(t, err)
		require.Contains(t, err.Error(), "no partition sender for key unknown")
	})
}
