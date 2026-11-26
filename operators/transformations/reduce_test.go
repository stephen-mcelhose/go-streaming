package transformations_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

func TestReduce(t *testing.T) {
	ctx := context.Background()
	inCh := make(chan int, 4)
	for i := 1; i <= 4; i++ {
		inCh <- i
	}
	close(inCh)

	outCh := make(chan int, 1)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	err := transformations.Reduce(ctx, 0, func(ctx context.Context, acc, item int) (int, error) {
		return acc + item, nil
	}, inStream, outStream)

	require.NoError(t, err)
	close(outCh)

	require.Equal(t, 10, <-outCh)
}
