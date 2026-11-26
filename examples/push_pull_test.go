package examples_test

import (
	"context"
	"iter"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
)

// ToSeq2 converts a Receiver[T] push stream into a Go 1.23 iter.Seq2[T, error] pull iterator.
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

// FromSeq2 pushes items from a Go 1.23 pull iterator into a Sender[T].
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

func TestPushPullConversion(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST convert Receiver to iter.Seq2", func(t *testing.T) {
		ch := make(chan int, 3)
		ch <- 10
		ch <- 20
		ch <- 30
		close(ch)

		stream := streaming.NewChanStream(ch, nil)
		seq := ToSeq2[int](ctx, stream)

		var collected []int
		for val, err := range seq {
			require.NoError(t, err)
			collected = append(collected, val)
		}

		require.Equal(t, []int{10, 20, 30}, collected)
	})

	t.Run("MUST convert iter.Seq2 to Sender", func(t *testing.T) {
		seq := func(yield func(string, error) bool) {
			items := []string{"foo", "bar", "baz"}
			for _, it := range items {
				if !yield(it, nil) {
					return
				}
			}
		}

		outCh := make(chan string, 3)
		outStream := streaming.NewChanStream(nil, outCh)

		err := FromSeq2(ctx, seq, outStream)
		require.NoError(t, err)
		close(outCh)

		var collected []string
		for it := range outCh {
			collected = append(collected, it)
		}
		require.Equal(t, []string{"foo", "bar", "baz"}, collected)
	})
}
