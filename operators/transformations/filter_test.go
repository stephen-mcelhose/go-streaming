package transformations_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

func TestFilter(t *testing.T) {
	type testCase struct {
		name              string
		setup             func() (context.Context, streaming.Receiver[int], func(ctx context.Context, item int) (bool, error), streaming.Sender[int], <-chan int)
		wantErr           error
		assertSideEffects func(t *testing.T, outCh <-chan int)
	}

	testCases := []testCase{
		// 1. Errors & Guards
		{
			name: "MUST fail fast and return error when predicate returns error",
			setup: func() (context.Context, streaming.Receiver[int], func(ctx context.Context, item int) (bool, error), streaming.Sender[int], <-chan int) {
				inCh := make(chan int, 3)
				inCh <- 1
				inCh <- -5
				inCh <- 2
				close(inCh)

				outCh := make(chan int, 3)
				predicate := func(ctx context.Context, i int) (bool, error) {
					if i < 0 {
						return false, errors.New("negative value not allowed")
					}
					return true, nil
				}
				return context.Background(), streaming.NewChanStream(inCh, nil), predicate, streaming.NewChanStream(nil, outCh), outCh
			},
			wantErr: errors.New("negative value not allowed"),
		},
		{
			name: "MUST return ctx.Err() when context is canceled",
			setup: func() (context.Context, streaming.Receiver[int], func(ctx context.Context, item int) (bool, error), streaming.Sender[int], <-chan int) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				inCh := make(chan int, 2)
				inCh <- 10
				inCh <- 20
				close(inCh)

				outCh := make(chan int, 2)
				predicate := func(ctx context.Context, i int) (bool, error) { return true, nil }
				return ctx, streaming.NewChanStream(inCh, nil), predicate, streaming.NewChanStream(nil, outCh), outCh
			},
			wantErr: context.Canceled,
		},
		// 2. Core Domain Happy Path
		{
			name: "MUST forward elements satisfying predicate and drop others",
			setup: func() (context.Context, streaming.Receiver[int], func(ctx context.Context, item int) (bool, error), streaming.Sender[int], <-chan int) {
				inCh := make(chan int, 5)
				for i := 1; i <= 5; i++ {
					inCh <- i
				}
				close(inCh)

				outCh := make(chan int, 5)
				predicate := func(ctx context.Context, i int) (bool, error) {
					return i%2 == 0, nil
				}
				return context.Background(), streaming.NewChanStream(inCh, nil), predicate, streaming.NewChanStream(nil, outCh), outCh
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, outCh <-chan int) {
				var evens []int
				for len(outCh) > 0 {
					evens = append(evens, <-outCh)
				}
				require.Equal(t, []int{2, 4}, evens)
			},
		},
		// 3. Edge Cases
		{
			name: "MUST emit nothing when all elements fail predicate",
			setup: func() (context.Context, streaming.Receiver[int], func(ctx context.Context, item int) (bool, error), streaming.Sender[int], <-chan int) {
				inCh := make(chan int, 3)
				inCh <- 1
				inCh <- 3
				inCh <- 5
				close(inCh)

				outCh := make(chan int, 3)
				predicate := func(ctx context.Context, i int) (bool, error) { return false, nil }
				return context.Background(), streaming.NewChanStream(inCh, nil), predicate, streaming.NewChanStream(nil, outCh), outCh
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, outCh <-chan int) {
				require.Empty(t, outCh)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, inStream, predicate, outStream, outCh := tc.setup()
			err := transformations.Filter(ctx, predicate, inStream, outStream)

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
