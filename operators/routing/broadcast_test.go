package routing_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/routing"
)

type failOnSecondSend struct {
	sent []string
}

func (f *failOnSecondSend) Send(ctx context.Context, item string) error {
	if len(f.sent) >= 1 {
		return errors.New("destination disk full")
	}
	f.sent = append(f.sent, item)
	return nil
}

func TestBroadcast(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST fan out items to all targets", func(t *testing.T) {
		inCh := make(chan string, 2)
		inCh <- "msg1"
		inCh <- "msg2"
		close(inCh)

		out1 := make(chan string, 2)
		out2 := make(chan string, 2)

		err := routing.Broadcast(ctx, streaming.NewChanStream(inCh, nil),
			streaming.NewChanStream(nil, out1),
			streaming.NewChanStream(nil, out2),
		)
		require.NoError(t, err)

		close(out1)
		close(out2)

		var got1, got2 []string
		for it := range out1 {
			got1 = append(got1, it)
		}
		for it := range out2 {
			got2 = append(got2, it)
		}

		require.Equal(t, []string{"msg1", "msg2"}, got1)
		require.Equal(t, []string{"msg1", "msg2"}, got2)
	})

	t.Run("MUST return error on failure in Broadcast", func(t *testing.T) {
		inCh := make(chan string, 2)
		inCh <- "item-1"
		inCh <- "item-2"
		close(inCh)

		failer := &failOnSecondSend{}
		okCh := make(chan string, 2)

		err := routing.Broadcast(ctx, streaming.NewChanStream(inCh, nil),
			streaming.NewChanStream(nil, okCh),
			failer,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "destination disk full")
	})

	t.Run("BroadcastBestEffort MUST continue despite single target failure", func(t *testing.T) {
		inCh := make(chan string, 2)
		inCh <- "item-1"
		inCh <- "item-2"
		close(inCh)

		failer := &failOnSecondSend{}
		okCh := make(chan string, 2)

		err := routing.BroadcastBestEffort(ctx, streaming.NewChanStream(inCh, nil),
			streaming.NewChanStream(nil, okCh),
			failer,
		)
		require.NoError(t, err)

		close(okCh)
		var gotOk []string
		for it := range okCh {
			gotOk = append(gotOk, it)
		}
		require.Equal(t, []string{"item-1", "item-2"}, gotOk)
	})
}
