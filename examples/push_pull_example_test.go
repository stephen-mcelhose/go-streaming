package examples_test

import (
	"context"
	"fmt"
	"iter"

	"github.com/stephen-mcelhose/go-streaming"
)

// ToSeq2 converts a push Receiver[T] stream into a pull Go 1.23 iter.Seq2[T, error].
func ToSeq2[T any](ctx context.Context, r streaming.Receiver[T]) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		_ = r.Receive(ctx, func(ctx context.Context, item T) error {
			if !yield(item, nil) {
				return context.Canceled
			}
			return nil
		})
	}
}

// FromSeq2 pushes items from a pull iter.Seq2[T, error] into a Sender[T].
func FromSeq2[T any](ctx context.Context, seq iter.Seq2[T, error], s streaming.Sender[T]) error {
	for item, err := range seq {
		if err != nil {
			return err
		}
		if err := s.Send(ctx, item); err != nil {
			return err
		}
	}
	return nil
}

func ExampleToSeq2() {
	ctx := context.Background()

	ch := make(chan string, 3)
	ch <- "alpha"
	ch <- "beta"
	ch <- "gamma"
	close(ch)

	stream := streaming.NewChanStream(ch, nil)
	seq := ToSeq2[string](ctx, stream)

	for item, err := range seq {
		if err != nil {
			break
		}
		fmt.Println(item)
	}

	// Output:
	// alpha
	// beta
	// gamma
}
