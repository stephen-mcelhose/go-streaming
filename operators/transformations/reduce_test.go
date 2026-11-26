package transformations_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

type orderSummary struct {
	TotalCount  int
	TotalAmount float64
}

type lineItem struct {
	Price float64
}

func TestReduce(t *testing.T) {
	ctx := context.Background()
	inCh := make(chan lineItem, 3)
	inCh <- lineItem{Price: 10.50}
	inCh <- lineItem{Price: 20.00}
	inCh <- lineItem{Price: 4.50}
	close(inCh)

	outCh := make(chan orderSummary, 1)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	err := transformations.Reduce(ctx, orderSummary{}, func(ctx context.Context, state orderSummary, item lineItem) (orderSummary, error) {
		state.TotalCount++
		state.TotalAmount += item.Price
		return state, nil
	}, inStream, outStream)

	require.NoError(t, err)
	close(outCh)

	got := <-outCh
	require.Equal(t, 3, got.TotalCount)
	require.Equal(t, 35.00, got.TotalAmount)
}

func TestScan(t *testing.T) {
	ctx := context.Background()
	inCh := make(chan int, 3)
	inCh <- 10
	inCh <- 20
	inCh <- 30
	close(inCh)

	outCh := make(chan int, 3)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	err := transformations.Scan(ctx, 0, func(ctx context.Context, runningTotal int, delta int) (int, error) {
		return runningTotal + delta, nil
	}, inStream, outStream)

	require.NoError(t, err)
	close(outCh)

	var runningTotals []int
	for it := range outCh {
		runningTotals = append(runningTotals, it)
	}
	require.Equal(t, []int{10, 30, 60}, runningTotals)
}
