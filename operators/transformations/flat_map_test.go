package transformations_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

func TestFlatMap(t *testing.T) {
	type testCase struct {
		name              string
		setup             func() (context.Context, streaming.Receiver[string], func(ctx context.Context, item string) (streaming.Receiver[rune], error), streaming.Sender[rune], <-chan rune)
		wantErr           error
		assertSideEffects func(t *testing.T, outCh <-chan rune)
	}

	testCases := []testCase{
		// 1. Errors & Guards
		{
			name: "MUST fail fast and return error when mapper function fails",
			setup: func() (context.Context, streaming.Receiver[string], func(ctx context.Context, item string) (streaming.Receiver[rune], error), streaming.Sender[rune], <-chan rune) {
				inCh := make(chan string, 2)
				inCh <- "bad"
				inCh <- "ok"
				close(inCh)

				outCh := make(chan rune, 10)
				mapper := func(ctx context.Context, item string) (streaming.Receiver[rune], error) {
					if item == "bad" {
						return nil, errors.New("mapper expansion failed")
					}
					return nil, nil
				}
				return context.Background(), streaming.NewChanStream(inCh, nil), mapper, streaming.NewChanStream(nil, outCh), outCh
			},
			wantErr: errors.New("mapper expansion failed"),
		},
		{
			name: "MUST return ctx.Err() when context is canceled during ingestion",
			setup: func() (context.Context, streaming.Receiver[string], func(ctx context.Context, item string) (streaming.Receiver[rune], error), streaming.Sender[rune], <-chan rune) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				inCh := make(chan string, 1)
				inCh <- "hello"
				close(inCh)

				outCh := make(chan rune, 10)
				mapper := func(ctx context.Context, item string) (streaming.Receiver[rune], error) {
					subCh := make(chan rune, len(item))
					for _, r := range item {
						subCh <- r
					}
					close(subCh)
					return streaming.NewChanStream(subCh, nil), nil
				}
				return ctx, streaming.NewChanStream(inCh, nil), mapper, streaming.NewChanStream(nil, outCh), outCh
			},
			wantErr: context.Canceled,
		},
		// 2. Core Domain Happy Path
		{
			name: "MUST expand each input element into its corresponding sub-stream",
			setup: func() (context.Context, streaming.Receiver[string], func(ctx context.Context, item string) (streaming.Receiver[rune], error), streaming.Sender[rune], <-chan rune) {
				inCh := make(chan string, 2)
				inCh <- "hi"
				inCh <- "go"
				close(inCh)

				outCh := make(chan rune, 10)
				mapper := func(ctx context.Context, item string) (streaming.Receiver[rune], error) {
					subCh := make(chan rune, len(item))
					for _, r := range item {
						subCh <- r
					}
					close(subCh)
					return streaming.NewChanStream(subCh, nil), nil
				}
				return context.Background(), streaming.NewChanStream(inCh, nil), mapper, streaming.NewChanStream(nil, outCh), outCh
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, outCh <-chan rune) {
				var collected []rune
				for len(outCh) > 0 {
					collected = append(collected, <-outCh)
				}
				require.Equal(t, []rune("higo"), collected)
			},
		},
		// 3. Edge Cases
		{
			name: "MUST skip emission when mapper returns nil sub-receiver",
			setup: func() (context.Context, streaming.Receiver[string], func(ctx context.Context, item string) (streaming.Receiver[rune], error), streaming.Sender[rune], <-chan rune) {
				inCh := make(chan string, 2)
				inCh <- "skip"
				inCh <- "a"
				close(inCh)

				outCh := make(chan rune, 10)
				mapper := func(ctx context.Context, item string) (streaming.Receiver[rune], error) {
					if item == "skip" {
						return nil, nil
					}
					subCh := make(chan rune, 1)
					subCh <- 'A'
					close(subCh)
					return streaming.NewChanStream(subCh, nil), nil
				}
				return context.Background(), streaming.NewChanStream(inCh, nil), mapper, streaming.NewChanStream(nil, outCh), outCh
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, outCh <-chan rune) {
				var collected []rune
				for len(outCh) > 0 {
					collected = append(collected, <-outCh)
				}
				require.Equal(t, []rune{'A'}, collected)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, inStream, mapper, outStream, outCh := tc.setup()
			err := transformations.FlatMap(ctx, mapper, inStream, outStream)

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
