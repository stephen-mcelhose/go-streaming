package examples_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
)

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
