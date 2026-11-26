package control_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/control"
)

type failSender struct {
	attempts int
	succeedOn int
}

func (f *failSender) Send(ctx context.Context, item string) error {
	f.attempts++
	if f.attempts >= f.succeedOn {
		return nil
	}
	return errors.New("transient error")
}

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

func TestRetrySender(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST succeed when within retry attempts", func(t *testing.T) {
		fs := &failSender{succeedOn: 3}
		retryer := control.NewRetrySender[string](fs, 3, 5*time.Millisecond)

		err := retryer.Send(ctx, "data")
		require.NoError(t, err)
		require.Equal(t, 3, fs.attempts)
	})

	t.Run("MUST fail when exceeding max attempts", func(t *testing.T) {
		fs := &failSender{succeedOn: 5}
		retryer := control.NewRetrySender[string](fs, 2, 5*time.Millisecond)

		err := retryer.Send(ctx, "data")
		require.Error(t, err)
		require.Equal(t, 2, fs.attempts)
	})
}
