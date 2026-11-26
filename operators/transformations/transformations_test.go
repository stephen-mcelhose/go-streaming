package transformations_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

func TestMap(t *testing.T) {
	type testCase struct {
		name       string
		items      []int
		mapper     func(ctx context.Context, item int) (string, error)
		wantOutput []string
		wantErr    error
	}

	testCases := []testCase{
		{
			name:  "MUST map items correctly",
			items: []int{1, 2, 3},
			mapper: func(ctx context.Context, item int) (string, error) {
				return string(rune('A' + item - 1)), nil
			},
			wantOutput: []string{"A", "B", "C"},
			wantErr:    nil,
		},
		{
			name:  "MUST fail fast when mapper returns error",
			items: []int{1, 2, 3},
			mapper: func(ctx context.Context, item int) (string, error) {
				if item == 2 {
					return "", errors.New("mapper err")
				}
				return "ok", nil
			},
			wantErr: errors.New("mapper err"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			inCh := make(chan int, len(tc.items))
			for _, it := range tc.items {
				inCh <- it
			}
			close(inCh)

			outCh := make(chan string, len(tc.items)+1)
			inStream := streaming.NewChanStream(inCh, nil)
			outStream := streaming.NewChanStream(nil, outCh)

			err := transformations.Map(ctx, tc.mapper, inStream, outStream)
			if tc.wantErr != nil {
				require.Error(t, err)
				require.Equal(t, tc.wantErr.Error(), err.Error())
				return
			}
			require.NoError(t, err)
			close(outCh)

			var got []string
			for it := range outCh {
				got = append(got, it)
			}
			require.Equal(t, tc.wantOutput, got)
		})
	}
}

func TestFlatMap(t *testing.T) {
	ctx := context.Background()
	inCh := make(chan string, 2)
	inCh <- "hello"
	inCh <- "world"
	close(inCh)

	outCh := make(chan rune, 10)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	err := transformations.FlatMap(ctx, func(ctx context.Context, s string) ([]rune, error) {
		return []rune(s), nil
	}, inStream, outStream)

	require.NoError(t, err)
	close(outCh)

	var runes []rune
	for r := range outCh {
		runes = append(runes, r)
	}
	require.Equal(t, []rune("helloworld"), runes)
}

func TestFilter(t *testing.T) {
	ctx := context.Background()
	inCh := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		inCh <- i
	}
	close(inCh)

	outCh := make(chan int, 5)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	err := transformations.Filter(ctx, func(ctx context.Context, i int) (bool, error) {
		return i%2 == 0, nil
	}, inStream, outStream)

	require.NoError(t, err)
	close(outCh)

	var evens []int
	for it := range outCh {
		evens = append(evens, it)
	}
	require.Equal(t, []int{2, 4}, evens)
}

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

func TestBatch(t *testing.T) {
	t.Run("MUST batch by max size", func(t *testing.T) {
		ctx := context.Background()
		inCh := make(chan int, 5)
		for i := 1; i <= 5; i++ {
			inCh <- i
		}
		close(inCh)

		outCh := make(chan []int, 5)
		inStream := streaming.NewChanStream(inCh, nil)
		outStream := streaming.NewChanStream(nil, outCh)

		err := transformations.Batch(ctx, 2, time.Second, inStream, outStream)
		require.NoError(t, err)
		close(outCh)

		var batches [][]int
		for b := range outCh {
			batches = append(batches, b)
		}
		require.Equal(t, [][]int{{1, 2}, {3, 4}, {5}}, batches)
	})

	t.Run("MUST batch by timeout when buffer not full", func(t *testing.T) {
		ctx := context.Background()
		inCh := make(chan int)
		outCh := make(chan []int, 5)

		inStream := streaming.NewChanStream(inCh, nil)
		outStream := streaming.NewChanStream(nil, outCh)

		done := make(chan error, 1)
		go func() {
			done <- transformations.Batch(ctx, 10, 50*time.Millisecond, inStream, outStream)
		}()

		inCh <- 1
		inCh <- 2
		time.Sleep(100 * time.Millisecond)
		inCh <- 3
		close(inCh)

		require.NoError(t, <-done)
		close(outCh)

		var batches [][]int
		for b := range outCh {
			batches = append(batches, b)
		}
		require.Equal(t, [][]int{{1, 2}, {3}}, batches)
	})
}
