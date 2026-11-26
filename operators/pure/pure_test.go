package pure_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/pure"
)

func TestPureMap(t *testing.T) {
	type testCase struct {
		name              string
		setup             func() (context.Context, func(int) int, streaming.Receiver[int], streaming.Sender[int], <-chan int)
		wantErr           error
		assertSideEffects func(t *testing.T, outCh <-chan int)
	}

	testCases := []testCase{
		// 1. Errors & Guards
		{
			name: "MUST return ctx.Err() when context is canceled",
			setup: func() (context.Context, func(int) int, streaming.Receiver[int], streaming.Sender[int], <-chan int) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				inCh := make(chan int, 1)
				inCh <- 10
				close(inCh)
				outCh := make(chan int, 1)
				return ctx, func(i int) int { return i * 2 }, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh), outCh
			},
			wantErr: context.Canceled,
		},
		// 2. Core Domain Happy Path
		{
			name: "MUST apply pure mapping function to each stream element",
			setup: func() (context.Context, func(int) int, streaming.Receiver[int], streaming.Sender[int], <-chan int) {
				inCh := make(chan int, 3)
				inCh <- 2
				inCh <- 3
				inCh <- 4
				close(inCh)
				outCh := make(chan int, 3)
				return context.Background(), func(i int) int { return i * i }, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh), outCh
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, outCh <-chan int) {
				var got []int
				for len(outCh) > 0 {
					got = append(got, <-outCh)
				}
				require.Equal(t, []int{4, 9, 16}, got)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, mapper, in, out, outCh := tc.setup()
			err := pure.Map(ctx, mapper, in, out)

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

func TestPureFilter(t *testing.T) {
	type testCase struct {
		name              string
		setup             func() (context.Context, func(int) bool, streaming.Receiver[int], streaming.Sender[int], <-chan int)
		wantErr           error
		assertSideEffects func(t *testing.T, outCh <-chan int)
	}

	testCases := []testCase{
		// 1. Core Domain Happy Path
		{
			name: "MUST forward elements matching pure predicate",
			setup: func() (context.Context, func(int) bool, streaming.Receiver[int], streaming.Sender[int], <-chan int) {
				inCh := make(chan int, 4)
				inCh <- 1
				inCh <- 2
				inCh <- 3
				inCh <- 4
				close(inCh)
				outCh := make(chan int, 4)
				return context.Background(), func(i int) bool { return i > 2 }, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh), outCh
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
			ctx, predicate, in, out, outCh := tc.setup()
			err := pure.Filter(ctx, predicate, in, out)

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

func TestPureReduce(t *testing.T) {
	type testCase struct {
		name              string
		setup             func() (context.Context, int, func(int, int) int, streaming.Receiver[int], streaming.Sender[int], <-chan int)
		wantErr           error
		assertSideEffects func(t *testing.T, outCh <-chan int)
	}

	testCases := []testCase{
		// 1. Core Domain Happy Path
		{
			name: "MUST reduce elements to scalar terminal value",
			setup: func() (context.Context, int, func(int, int) int, streaming.Receiver[int], streaming.Sender[int], <-chan int) {
				inCh := make(chan int, 3)
				inCh <- 2
				inCh <- 3
				inCh <- 4
				close(inCh)
				outCh := make(chan int, 1)
				return context.Background(), 1, func(acc, item int) int { return acc * item }, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh), outCh
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, outCh <-chan int) {
				require.Equal(t, 24, <-outCh)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, init, reducer, in, out, outCh := tc.setup()
			err := pure.Reduce(ctx, init, reducer, in, out)

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

func TestPureScan(t *testing.T) {
	type testCase struct {
		name              string
		setup             func() (context.Context, int, func(int, int) int, streaming.Receiver[int], streaming.Sender[int], <-chan int)
		wantErr           error
		assertSideEffects func(t *testing.T, outCh <-chan int)
	}

	testCases := []testCase{
		// 1. Core Domain Happy Path
		{
			name: "MUST continuously emit running accumulated state after each element",
			setup: func() (context.Context, int, func(int, int) int, streaming.Receiver[int], streaming.Sender[int], <-chan int) {
				inCh := make(chan int, 3)
				inCh <- 1
				inCh <- 2
				inCh <- 3
				close(inCh)
				outCh := make(chan int, 3)
				return context.Background(), 0, func(runningSum, item int) int { return runningSum + item }, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh), outCh
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, outCh <-chan int) {
				var got []int
				for len(outCh) > 0 {
					got = append(got, <-outCh)
				}
				require.Equal(t, []int{1, 3, 6}, got)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, init, acc, in, out, outCh := tc.setup()
			err := pure.Scan(ctx, init, acc, in, out)

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
