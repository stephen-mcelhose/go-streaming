package merging_test

import (
	"context"
	"fmt"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/merging"
)

func ExampleConcat() {
	ctx := context.Background()

	ch1 := make(chan string, 2)
	ch1 <- "part1-a"
	ch1 <- "part1-b"
	close(ch1)

	ch2 := make(chan string, 2)
	ch2 <- "part2-a"
	ch2 <- "part2-b"
	close(ch2)

	outCh := make(chan string, 4)
	outStream := streaming.NewChanStream(nil, outCh)

	go func() {
		defer close(outCh)
		_ = merging.Concat(ctx, outStream, streaming.NewChanStream(ch1, nil), streaming.NewChanStream(ch2, nil))
	}()

	for s := range outCh {
		fmt.Println(s)
	}

	// Output:
	// part1-a
	// part1-b
	// part2-a
	// part2-b
}
