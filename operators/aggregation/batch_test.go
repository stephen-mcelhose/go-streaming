package aggregation_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/aggregation"
)

func TestBatch(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST batch by count", func(t *testing.T) {
		inCh := make(chan int, 5)
		for i := 1; i <= 5; i++ {
			inCh <- i
		}
		close(inCh)

		outCh := make(chan []int, 5)
		inStream := streaming.NewChanStream(inCh, nil)
		outStream := streaming.NewChanStream(nil, outCh)

		err := aggregation.BatchCount(ctx, 2, inStream, outStream)
		require.NoError(t, err)
		close(outCh)

		var batches [][]int
		for b := range outCh {
			batches = append(batches, b)
		}
		require.Equal(t, [][]int{{1, 2}, {3, 4}, {5}}, batches)
	})

	t.Run("MUST batch by time", func(t *testing.T) {
		inCh := make(chan int)
		outCh := make(chan []int, 5)

		inStream := streaming.NewChanStream(inCh, nil)
		outStream := streaming.NewChanStream(nil, outCh)

		done := make(chan error, 1)
		go func() {
			done <- aggregation.BatchTime(ctx, 50*time.Millisecond, inStream, outStream)
		}()

		inCh <- 10
		inCh <- 20
		time.Sleep(100 * time.Millisecond)
		inCh <- 30
		close(inCh)

		require.NoError(t, <-done)
		close(outCh)

		var batches [][]int
		for b := range outCh {
			batches = append(batches, b)
		}
		require.Equal(t, [][]int{{10, 20}, {30}}, batches)
	})
}
