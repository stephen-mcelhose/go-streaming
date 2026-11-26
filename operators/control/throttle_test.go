package control_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/control"
)

func TestThrottle(t *testing.T) {
	ctx := context.Background()

	inCh := make(chan int, 3)
	inCh <- 1
	inCh <- 2
	close(inCh)

	outCh := make(chan int, 3)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	start := time.Now()
	err := control.Throttle(ctx, 20*time.Millisecond, inStream, outStream)
	require.NoError(t, err)
	elapsed := time.Since(start)

	close(outCh)
	require.True(t, elapsed >= 30*time.Millisecond)
}
