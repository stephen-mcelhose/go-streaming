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
	type testCase struct {
		name              string
		setup             func() (context.Context, time.Duration, streaming.Receiver[int], streaming.Sender[int], <-chan int)
		wantErr           error
		assertSideEffects func(t *testing.T, outCh <-chan int)
	}

	testCases := []testCase{
		// 1. Errors & Guards
		{
			name: "MUST return ctx.Err() when context is canceled during quiet window",
			setup: func() (context.Context, time.Duration, streaming.Receiver[int], streaming.Sender[int], <-chan int) {
				ctx, cancel := context.WithCancel(context.Background())
				inCh := make(chan int)
				outCh := make(chan int, 5)

				go func() {
					inCh <- 1
					time.Sleep(10 * time.Millisecond)
					cancel()
				}()

				return ctx, 50 * time.Millisecond, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh), outCh
			},
			wantErr: context.Canceled,
		},
		// 2. Core Domain Happy Path
		{
			name: "MUST suppress burst items and emit only settled item after quiet window",
			setup: func() (context.Context, time.Duration, streaming.Receiver[int], streaming.Sender[int], <-chan int) {
				inCh := make(chan int)
				outCh := make(chan int, 5)

				go func() {
					// Rapid burst
					inCh <- 1
					inCh <- 2
					inCh <- 3
					time.Sleep(80 * time.Millisecond)

					// Second burst after pause
					inCh <- 4
					time.Sleep(80 * time.Millisecond)
					close(inCh)
				}()

				return context.Background(), 40 * time.Millisecond, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh), outCh
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, outCh <-chan int) {
				var got []int
				for len(outCh) > 0 {
					got = append(got, <-outCh)
				}
				require.Equal(t, []int{3, 4}, got)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, quietDuration, in, out, outCh := tc.setup()
			err := control.Debounce(ctx, quietDuration, in, out)

			if tc.wantErr != nil {
				require.Error(t, err)
				require.Equal(t, tc.wantErr.Error(), err.Error())
				return
			}
			require.NoError(t, err)
			if tc.assertSideEffects != nil {
				tc.assertSideEffects(t, outCh)
			}
		})
	}
}
