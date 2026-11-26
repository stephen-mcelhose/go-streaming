package aggregation_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/aggregation"
)

func TestBuffer(t *testing.T) {
	ctx := context.Background()

	inCh := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		inCh <- i
	}
	close(inCh)

	outCh := make(chan []int, 5)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	err := aggregation.Buffer(ctx, 2, inStream, outStream)
	require.NoError(t, err)
	close(outCh)

	var got [][]int
	for b := range outCh {
		got = append(got, b)
	}
	require.Equal(t, [][]int{{1, 2}, {3, 4}, {5}}, got)
}
