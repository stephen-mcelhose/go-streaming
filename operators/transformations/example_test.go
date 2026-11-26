package transformations_test

import (
	"context"
	"fmt"
	"strings"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

func ExampleMap() {
	ctx := context.Background()
	inCh := make(chan string, 3)
	inCh <- "foo"
	inCh <- "bar"
	inCh <- "baz"
	close(inCh)

	outCh := make(chan string, 3)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	go func() {
		defer close(outCh)
		_ = transformations.Map(ctx, func(ctx context.Context, item string) (string, error) {
			return strings.ToUpper(item), nil
		}, inStream, outStream)
	}()

	for s := range outCh {
		fmt.Println(s)
	}

	// Output:
	// FOO
	// BAR
	// BAZ
}

func ExampleFilter() {
	ctx := context.Background()
	inCh := make(chan int, 4)
	inCh <- 1
	inCh <- 2
	inCh <- 3
	inCh <- 4
	close(inCh)

	outCh := make(chan int, 4)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	go func() {
		defer close(outCh)
		_ = transformations.Filter(ctx, func(ctx context.Context, item int) (bool, error) {
			return item%2 == 0, nil
		}, inStream, outStream)
	}()

	for n := range outCh {
		fmt.Println(n)
	}

	// Output:
	// 2
	// 4
}
