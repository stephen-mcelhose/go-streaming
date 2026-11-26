package merging_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/merging"
)

func TestMerge(t *testing.T) {
	type testCase struct {
		name              string
		setup             func() (context.Context, streaming.Sender[int], []streaming.Receiver[int], <-chan int)
		wantErr           error
		assertSideEffects func(t *testing.T, outCh <-chan int)
	}

	testCases := []testCase{
		// 1. Errors & Guards
		{
			name: "MUST cancel and propagate error when one receiver fails",
			setup: func() (context.Context, streaming.Sender[int], []streaming.Receiver[int], <-chan int) {
				ch1 := make(chan int, 2)
				ch1 <- 1
				close(ch1)

				failingStream := &failingReceiver[int]{err: errors.New("stream 2 failed")}
				outCh := make(chan int, 5)

				return context.Background(), streaming.NewChanStream(nil, outCh), []streaming.Receiver[int]{
					streaming.NewChanStream(ch1, nil),
					failingStream,
				}, outCh
			},
			wantErr: errors.New("stream 2 failed"),
		},
		// 2. Core Domain Happy Path
		{
			name: "MUST merge all items concurrently without dropping",
			setup: func() (context.Context, streaming.Sender[int], []streaming.Receiver[int], <-chan int) {
				ch1 := make(chan int, 2)
				ch1 <- 1
				ch1 <- 3
				close(ch1)

				ch2 := make(chan int, 2)
				ch2 <- 2
				ch2 <- 4
				close(ch2)

				outCh := make(chan int, 4)
				return context.Background(), streaming.NewChanStream(nil, outCh), []streaming.Receiver[int]{
					streaming.NewChanStream(ch1, nil),
					streaming.NewChanStream(ch2, nil),
				}, outCh
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, outCh <-chan int) {
				var got []int
				for len(outCh) > 0 {
					got = append(got, <-outCh)
				}
				sort.Ints(got)
				require.Equal(t, []int{1, 2, 3, 4}, got)
			},
		},
		// 3. Edge Cases & Sentinels
		{
			name: "MUST ignore io.EOF and continue merging remaining active receivers",
			setup: func() (context.Context, streaming.Sender[int], []streaming.Receiver[int], <-chan int) {
				ch := make(chan int, 2)
				ch <- 10
				ch <- 20
				close(ch)

				outCh := make(chan int, 5)
				return context.Background(), streaming.NewChanStream(nil, outCh), []streaming.Receiver[int]{
					streaming.NewChanStream(ch, nil),
					streaming.NewEOFReceiver[int](),
				}, outCh
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, outCh <-chan int) {
				var got []int
				for len(outCh) > 0 {
					got = append(got, <-outCh)
				}
				require.Equal(t, []int{10, 20}, got)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, sender, receivers, outCh := tc.setup()
			err := merging.Merge(ctx, sender, receivers...)

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

func TestConcat(t *testing.T) {
	type testCase struct {
		name              string
		setup             func() (context.Context, streaming.Sender[string], []streaming.Receiver[string], <-chan string)
		wantErr           error
		assertSideEffects func(t *testing.T, outCh <-chan string)
	}

	testCases := []testCase{
		// 1. Errors & Guards
		{
			name: "MUST stop sequence immediately when a receiver returns error",
			setup: func() (context.Context, streaming.Sender[string], []streaming.Receiver[string], <-chan string) {
				ch1 := make(chan string, 1)
				ch1 <- "A1"
				close(ch1)

				failingStream := &failingReceiver[string]{err: errors.New("archive unreadable")}
				ch3 := make(chan string, 1)
				ch3 <- "C1"
				close(ch3)

				outCh := make(chan string, 5)
				return context.Background(), streaming.NewChanStream(nil, outCh), []streaming.Receiver[string]{
					streaming.NewChanStream(ch1, nil),
					failingStream,
					streaming.NewChanStream(ch3, nil),
				}, outCh
			},
			wantErr: errors.New("archive unreadable"),
		},
		// 2. Core Domain Happy Path
		{
			name: "MUST sequentially exhaust receivers in exact order",
			setup: func() (context.Context, streaming.Sender[string], []streaming.Receiver[string], <-chan string) {
				ch1 := make(chan string, 2)
				ch1 <- "A1"
				ch1 <- "A2"
				close(ch1)

				ch2 := make(chan string, 2)
				ch2 <- "B1"
				ch2 <- "B2"
				close(ch2)

				outCh := make(chan string, 4)
				return context.Background(), streaming.NewChanStream(nil, outCh), []streaming.Receiver[string]{
					streaming.NewChanStream(ch1, nil),
					streaming.NewChanStream(ch2, nil),
				}, outCh
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, outCh <-chan string) {
				var got []string
				for len(outCh) > 0 {
					got = append(got, <-outCh)
				}
				require.Equal(t, []string{"A1", "A2", "B1", "B2"}, got)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, sender, receivers, outCh := tc.setup()
			err := merging.Concat(ctx, sender, receivers...)

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

func TestInterleave(t *testing.T) {
	type testCase struct {
		name              string
		setup             func() (context.Context, streaming.Sender[int], []streaming.Receiver[int], <-chan int)
		wantErr           error
		assertSideEffects func(t *testing.T, outCh <-chan int)
	}

	testCases := []testCase{
		// 1. Errors & Guards
		{
			name: "MUST cancel interleave queue when worker fails",
			setup: func() (context.Context, streaming.Sender[int], []streaming.Receiver[int], <-chan int) {
				failingStream := &failingReceiver[int]{err: errors.New("queue error")}
				outCh := make(chan int, 5)
				return context.Background(), streaming.NewChanStream(nil, outCh), []streaming.Receiver[int]{
					failingStream,
				}, outCh
			},
			wantErr: errors.New("queue error"),
		},
		// 2. Core Domain Happy Path
		{
			name: "MUST cycle across active streams in fair round-robin order",
			setup: func() (context.Context, streaming.Sender[int], []streaming.Receiver[int], <-chan int) {
				ch1 := make(chan int, 3)
				ch1 <- 1
				ch1 <- 3
				ch1 <- 5
				close(ch1)

				ch2 := make(chan int, 3)
				ch2 <- 2
				ch2 <- 4
				close(ch2)

				outCh := make(chan int, 5)
				return context.Background(), streaming.NewChanStream(nil, outCh), []streaming.Receiver[int]{
					streaming.NewChanStream(ch1, nil),
					streaming.NewChanStream(ch2, nil),
				}, outCh
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, outCh <-chan int) {
				var got []int
				for len(outCh) > 0 {
					got = append(got, <-outCh)
				}
				require.Equal(t, []int{1, 2, 3, 4, 5}, got)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, sender, receivers, outCh := tc.setup()
			err := merging.Interleave(ctx, sender, receivers...)

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

func TestZip(t *testing.T) {
	type testCase struct {
		name              string
		setup             func() (context.Context, streaming.Receiver[string], streaming.Receiver[int], streaming.Sender[string], <-chan string)
		wantErr           error
		assertSideEffects func(t *testing.T, outCh <-chan string)
	}

	testCases := []testCase{
		// 1. Errors & Guards
		{
			name: "MUST propagate zipper combination error",
			setup: func() (context.Context, streaming.Receiver[string], streaming.Receiver[int], streaming.Sender[string], <-chan string) {
				ch1 := make(chan string, 1)
				ch1 <- "A"
				close(ch1)

				ch2 := make(chan int, 1)
				ch2 <- 1
				close(ch2)

				outCh := make(chan string, 1)
				return context.Background(), streaming.NewChanStream(ch1, nil), streaming.NewChanStream(ch2, nil), streaming.NewChanStream(nil, outCh), outCh
			},
			wantErr: errors.New("zipper error"),
		},
		// 2. Core Domain Happy Path
		{
			name: "MUST pair elements 1-to-1 until either stream exhausts",
			setup: func() (context.Context, streaming.Receiver[string], streaming.Receiver[int], streaming.Sender[string], <-chan string) {
				ch1 := make(chan string, 3)
				ch1 <- "A"
				ch1 <- "B"
				ch1 <- "C"
				close(ch1)

				ch2 := make(chan int, 2)
				ch2 <- 1
				ch2 <- 2
				close(ch2)

				outCh := make(chan string, 5)
				return context.Background(), streaming.NewChanStream(ch1, nil), streaming.NewChanStream(ch2, nil), streaming.NewChanStream(nil, outCh), outCh
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, outCh <-chan string) {
				var got []string
				for len(outCh) > 0 {
					got = append(got, <-outCh)
				}
				require.Equal(t, []string{"A:1", "B:2"}, got)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, aRx, bRx, out, outCh := tc.setup()
			err := merging.Zip(ctx, func(ctx context.Context, a string, b int) (string, error) {
				if tc.wantErr != nil {
					return "", tc.wantErr
				}
				return fmt.Sprintf("%s:%d", a, b), nil
			}, aRx, bRx, out)

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

func TestMergeSort(t *testing.T) {
	type testCase struct {
		name              string
		setup             func() (context.Context, func(a, b int) int, streaming.Sender[int], []streaming.Receiver[int], <-chan int)
		wantErr           error
		assertSideEffects func(t *testing.T, outCh <-chan int)
	}

	testCases := []testCase{
		// 1. Core Domain Happy Path
		{
			name: "MUST merge pre-sorted partitions into globally sorted stream",
			setup: func() (context.Context, func(a, b int) int, streaming.Sender[int], []streaming.Receiver[int], <-chan int) {
				p1 := make(chan int, 3)
				p1 <- 1
				p1 <- 4
				p1 <- 9
				close(p1)

				p2 := make(chan int, 3)
				p2 <- 2
				p2 <- 5
				p2 <- 8
				close(p2)

				p3 := make(chan int, 3)
				p3 <- 3
				p3 <- 6
				p3 <- 7
				close(p3)

				outCh := make(chan int, 10)
				return context.Background(), func(a, b int) int { return a - b }, streaming.NewChanStream(nil, outCh), []streaming.Receiver[int]{
					streaming.NewChanStream(p1, nil),
					streaming.NewChanStream(p2, nil),
					streaming.NewChanStream(p3, nil),
				}, outCh
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, outCh <-chan int) {
				var got []int
				for len(outCh) > 0 {
					got = append(got, <-outCh)
				}
				require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}, got)
			},
		},
		// 2. Edge Case
		{
			name: "MUST return nil immediately when partition list is empty",
			setup: func() (context.Context, func(a, b int) int, streaming.Sender[int], []streaming.Receiver[int], <-chan int) {
				outCh := make(chan int, 1)
				return context.Background(), func(a, b int) int { return a - b }, streaming.NewChanStream(nil, outCh), nil, outCh
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, outCh <-chan int) {
				require.Empty(t, outCh)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cmp, sender, partitions, outCh := tc.setup()
			err := merging.MergeSort(ctx, cmp, sender, partitions...)

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

type failingReceiver[T any] struct {
	err error
}

func (f *failingReceiver[T]) Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error {
	return f.err
}
