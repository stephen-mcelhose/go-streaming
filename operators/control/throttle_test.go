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
	type testCase struct {
		name              string
		setup             func() (context.Context, time.Duration, streaming.Receiver[int], streaming.Sender[int], <-chan int)
		wantErr           error
		assertSideEffects func(t *testing.T, outCh <-chan int, elapsed time.Duration)
	}

	testCases := []testCase{
		// 1. Errors & Guards
		{
			name: "MUST return ctx.Err() when context is canceled while throttling",
			setup: func() (context.Context, time.Duration, streaming.Receiver[int], streaming.Sender[int], <-chan int) {
				ctx, cancel := context.WithCancel(context.Background())
				inCh := make(chan int, 2)
				inCh <- 1
				inCh <- 2
				outCh := make(chan int, 2)

				// Cancel context after small delay
				go func() {
					time.Sleep(10 * time.Millisecond)
					cancel()
				}()

				return ctx, 50 * time.Millisecond, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh), outCh
			},
			wantErr: context.Canceled,
		},
		// 2. Core Domain Happy Path
		{
			name: "MUST space emitted elements by at least the throttle interval",
			setup: func() (context.Context, time.Duration, streaming.Receiver[int], streaming.Sender[int], <-chan int) {
				inCh := make(chan int, 3)
				inCh <- 1
				inCh <- 2
				close(inCh)

				outCh := make(chan int, 3)
				return context.Background(), 20 * time.Millisecond, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh), outCh
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, outCh <-chan int, elapsed time.Duration) {
				var got []int
				for len(outCh) > 0 {
					got = append(got, <-outCh)
				}
				require.Equal(t, []int{1, 2}, got)
				require.True(t, elapsed >= 30*time.Millisecond)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, interval, in, out, outCh := tc.setup()
			start := time.Now()
			err := control.Throttle(ctx, interval, in, out)
			elapsed := time.Since(start)

			if tc.wantErr != nil {
				require.Error(t, err)
				require.Equal(t, tc.wantErr.Error(), err.Error())
				return
			}
			require.NoError(t, err)
			if tc.assertSideEffects != nil {
				tc.assertSideEffects(t, outCh, elapsed)
			}
		})
	}
}
