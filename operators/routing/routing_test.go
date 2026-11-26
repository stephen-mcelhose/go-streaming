package routing_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/routing"
)

func TestBroadcast(t *testing.T) {
	ctx := context.Background()

	inCh := make(chan string, 2)
	inCh <- "msg1"
	inCh <- "msg2"
	close(inCh)

	outCh1 := make(chan string, 2)
	outCh2 := make(chan string, 2)

	inStream := streaming.NewChanStream(inCh, nil)
	outStream1 := streaming.NewChanStream(nil, outCh1)
	outStream2 := streaming.NewChanStream(nil, outCh2)

	err := routing.Broadcast(ctx, inStream, outStream1, outStream2)
	require.NoError(t, err)

	close(outCh1)
	close(outCh2)

	var got1, got2 []string
	for it := range outCh1 {
		got1 = append(got1, it)
	}
	for it := range outCh2 {
		got2 = append(got2, it)
	}

	require.Equal(t, []string{"msg1", "msg2"}, got1)
	require.Equal(t, []string{"msg1", "msg2"}, got2)
}

func TestRoundRobin(t *testing.T) {
	ctx := context.Background()

	inCh := make(chan int, 4)
	for i := 1; i <= 4; i++ {
		inCh <- i
	}
	close(inCh)

	outCh1 := make(chan int, 2)
	outCh2 := make(chan int, 2)

	inStream := streaming.NewChanStream(inCh, nil)
	out1 := streaming.NewChanStream(nil, outCh1)
	out2 := streaming.NewChanStream(nil, outCh2)

	err := routing.RoundRobin(ctx, inStream, out1, out2)
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

func TestPartition(t *testing.T) {
	ctx := context.Background()

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

	inStream := streaming.NewChanStream(inCh, nil)
	err := routing.Partition(ctx, func(item string) (string, error) {
		return item, nil
	}, routes, inStream)

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
}
