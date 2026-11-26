package routing_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/routing"
)

func TestRoundRobin(t *testing.T) {
	ctx := context.Background()

	inCh := make(chan int, 4)
	for i := 1; i <= 4; i++ {
		inCh <- i
	}
	close(inCh)

	outCh1 := make(chan int, 2)
	outCh2 := make(chan int, 2)

	err := routing.RoundRobin(ctx, streaming.NewChanStream(inCh, nil),
		streaming.NewChanStream(nil, outCh1),
		streaming.NewChanStream(nil, outCh2),
	)
	require.NoError(t, err)

	close(outCh1)
	close(outCh2)

	var got1, got2 []int
	for it := range outCh1 {
		got1 = append(got1, it)
	}
	for it := range outCh2 {
		got2 = append(got2, it)
	}

	require.Equal(t, []int{1, 3}, got1)
	require.Equal(t, []int{2, 4}, got2)
}
