package math_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	mathops "github.com/stephen-mcelhose/go-streaming/operators/math"
)

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
