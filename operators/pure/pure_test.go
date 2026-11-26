package pure_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/pure"
)

func TestPureMap(t *testing.T) {
	ctx := context.Background()

	inCh := make(chan int, 3)
	inCh <- 2
	inCh <- 3
	inCh <- 4
	close(inCh)

	outCh := make(chan int, 3)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	err := pure.Map(ctx, func(i int) int {
		return i * i
	}, inStream, outStream)
	require.NoError(t, err)
	close(outCh)

	var got []int
	for it := range outCh {
		got = append(got, it)
	}
	require.Equal(t, []int{4, 9, 16}, got)
}

func TestPureFilter(t *testing.T) {
	ctx := context.Background()

	inCh := make(chan int, 4)
	inCh <- 1
	inCh <- 2
	inCh <- 3
	inCh <- 4
	close(inCh)

	outCh := make(chan int, 4)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	err := pure.Filter(ctx, func(i int) bool {
		return i > 2
	}, inStream, outStream)
	require.NoError(t, err)
	close(outCh)

	var got []int
	for it := range outCh {
		got = append(got, it)
	}
	require.Equal(t, []int{3, 4}, got)
}

func TestPureReduce(t *testing.T) {
	ctx := context.Background()

	inCh := make(chan int, 3)
	inCh <- 2
	inCh <- 3
	inCh <- 4
	close(inCh)

	outCh := make(chan int, 1)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	err := pure.Reduce(ctx, 1, func(acc, item int) int {
		return acc * item
	}, inStream, outStream)
	require.NoError(t, err)
	close(outCh)

	require.Equal(t, 24, <-outCh)
}

func TestPureScan(t *testing.T) {
	ctx := context.Background()

	inCh := make(chan int, 3)
	inCh <- 1
	inCh <- 2
	inCh <- 3
	close(inCh)

	outCh := make(chan int, 3)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	err := pure.Scan(ctx, 0, func(runningSum, item int) int {
		return runningSum + item
	}, inStream, outStream)
	require.NoError(t, err)
	close(outCh)

	var got []int
	for it := range outCh {
		got = append(got, it)
	}
	require.Equal(t, []int{1, 3, 6}, got)
}
