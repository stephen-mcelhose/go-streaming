package math_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	mathops "github.com/stephen-mcelhose/go-streaming/operators/math"
)

func TestMathMap(t *testing.T) {
	ctx := context.Background()

	inCh := make(chan int, 3)
	inCh <- 2
	inCh <- 3
	inCh <- 4
	close(inCh)

	outCh := make(chan int, 3)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	mathops.Map(ctx, func(i int) int {
		return i * i
	}, inStream, outStream)
	close(outCh)

	var got []int
	for it := range outCh {
		got = append(got, it)
	}
	require.Equal(t, []int{4, 9, 16}, got)
}

func TestMathFilter(t *testing.T) {
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

	mathops.Filter(ctx, func(i int) bool {
		return i > 2
	}, inStream, outStream)
	close(outCh)

	var got []int
	for it := range outCh {
		got = append(got, it)
	}
	require.Equal(t, []int{3, 4}, got)
}

func TestMathReduce(t *testing.T) {
	ctx := context.Background()

	inCh := make(chan int, 3)
	inCh <- 2
	inCh <- 3
	inCh <- 4
	close(inCh)

	outCh := make(chan int, 1)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	mathops.Reduce(ctx, 1, func(acc, item int) int {
		return acc * item
	}, inStream, outStream)
	close(outCh)

	require.Equal(t, 24, <-outCh)
}
