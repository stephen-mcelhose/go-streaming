package control_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/control"
)

func TestDebounce(t *testing.T) {
	ctx := context.Background()

	inCh := make(chan int)
	outCh := make(chan int, 5)

	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	done := make(chan error, 1)
	go func() {
		done <- control.Debounce(ctx, 50*time.Millisecond, inStream, outStream)
	}()

	// Send burst
	inCh <- 1
	inCh <- 2
	inCh <- 3
	time.Sleep(100 * time.Millisecond)

	// Send another single
	inCh <- 4
	time.Sleep(100 * time.Millisecond)
	close(inCh)

	require.NoError(t, <-done)
	close(outCh)

	var got []int
	for it := range outCh {
		got = append(got, it)
	}
	require.Equal(t, []int{3, 4}, got)
}
