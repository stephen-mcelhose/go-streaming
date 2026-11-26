package transformations_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

func TestFlatMap(t *testing.T) {
	ctx := context.Background()
	inCh := make(chan string, 2)
	inCh <- "hello"
	inCh <- "world"
	close(inCh)

	outCh := make(chan rune, 10)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	err := transformations.FlatMap(ctx, func(ctx context.Context, s string) (streaming.Receiver[rune], error) {
		subCh := make(chan rune, len(s))
		for _, r := range s {
			subCh <- r
		}
		close(subCh)
		return streaming.NewChanStream(subCh, nil), nil
	}, inStream, outStream)

	require.NoError(t, err)
	close(outCh)

	var runes []rune
	for r := range outCh {
		runes = append(runes, r)
	}
	require.Equal(t, []rune("helloworld"), runes)
}
