package examples_test

import (
	"context"
	"fmt"
	"iter"

	"github.com/stephen-mcelhose/go-streaming"
)

// ToSeq2 converts a push Receiver[T] stream into a pull Go 1.23 iter.Seq2[T, error].
// When the caller breaks early from the iterator (yield returns false), ToSeq2 signals
// streaming.ErrStreamStop to halt the push loop gracefully.
func ToSeq2[T any](ctx context.Context, r streaming.Receiver[T]) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		err := r.Receive(ctx, func(ctx context.Context, item T) error {
			if !yield(item, nil) {
				return streaming.ErrStreamStop
			}
			return nil
		})
		if err != nil && !streaming.IsCleanTermination(err) {
			yield(*new(T), err)
		}
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

func ExampleToSeq2_earlyBreak() {
	ctx := context.Background()

	ch := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		ch <- i
	}
	close(ch)

	stream := streaming.NewChanStream(ch, nil)
	seq := ToSeq2[int](ctx, stream)

	// Breaking early after 2 items
	for item, err := range seq {
		if err != nil {
			break
		}
		fmt.Println(item)
		if item == 2 {
			break
		}
	}

	// Output:
	// 1
	// 2
}
