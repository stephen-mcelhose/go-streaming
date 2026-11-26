package transformations_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

func TestFilter(t *testing.T) {
	ctx := context.Background()
	inCh := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		inCh <- i
	}
	close(inCh)

	outCh := make(chan int, 5)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	err := transformations.Filter(ctx, func(ctx context.Context, i int) (bool, error) {
		return i%2 == 0, nil
	}, inStream, outStream)

	require.NoError(t, err)
	close(outCh)

	var evens []int
	for it := range outCh {
		evens = append(evens, it)
	}
	require.Equal(t, []int{2, 4}, evens)
}
