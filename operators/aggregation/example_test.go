package aggregation_test

import (
	"context"
	"fmt"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/aggregation"
)

func ExampleBuffer() {
	ctx := context.Background()
	inCh := make(chan string, 5)
	inCh <- "a"
	inCh <- "b"
	inCh <- "c"
	inCh <- "d"
	inCh <- "e"
	close(inCh)

	outCh := make(chan []string, 5)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	go func() {
		defer close(outCh)
		_ = aggregation.Buffer(ctx, 2, inStream, outStream)
	}()

	for batch := range outCh {
		fmt.Printf("%v\n", batch)
	}

	// Output:
	// [a b]
	// [c d]
	// [e]
}
