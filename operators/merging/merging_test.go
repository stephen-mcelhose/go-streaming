package merging_test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/merging"
)

func TestMerge(t *testing.T) {
	ctx := context.Background()

	ch1 := make(chan int, 2)
	ch1 <- 1
	ch1 <- 3
	close(ch1)

	ch2 := make(chan int, 2)
	ch2 <- 2
	ch2 <- 4
	close(ch2)

	outCh := make(chan int, 4)
	outStream := streaming.NewChanStream(nil, outCh)

	err := merging.Merge(ctx, outStream, streaming.NewChanStream(ch1, nil), streaming.NewChanStream(ch2, nil))
	require.NoError(t, err)
	close(outCh)

	var got []int
	for it := range outCh {
		got = append(got, it)
	}
	sort.Ints(got)
	require.Equal(t, []int{1, 2, 3, 4}, got)
}

func TestZip(t *testing.T) {
	ctx := context.Background()

	ch1 := make(chan string, 3)
	ch1 <- "A"
	ch1 <- "B"
	ch1 <- "C"
	close(ch1)

	ch2 := make(chan int, 3)
	ch2 <- 1
	ch2 <- 2
	close(ch2)

	outCh := make(chan string, 5)
	outStream := streaming.NewChanStream(nil, outCh)

	err := merging.Zip(ctx, func(ctx context.Context, a string, b int) (string, error) {
		return fmt.Sprintf("%s:%d", a, b), nil
	}, streaming.NewChanStream(ch1, nil), streaming.NewChanStream(ch2, nil), outStream)

	require.NoError(t, err)
	close(outCh)

	var got []string
	for it := range outCh {
		got = append(got, it)
	}
	require.Equal(t, []string{"A:1", "B:2"}, got)
}

func TestMergeSort(t *testing.T) {
	ctx := context.Background()

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
	collectStream := streaming.NewChanStream(nil, outCh)

	done := make(chan struct{})
	var got []int
	go func() {
		for v := range outCh {
			got = append(got, v)
		}
		close(done)
	}()

	err := merging.MergeSort(ctx, func(a, b int) int {
		return a - b
	}, collectStream, streaming.NewChanStream(p1, nil), streaming.NewChanStream(p2, nil), streaming.NewChanStream(p3, nil))

	require.NoError(t, err)
	close(outCh)
	<-done

	require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}, got)
}
