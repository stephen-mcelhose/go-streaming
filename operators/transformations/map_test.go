package transformations_test

import (
	"context"
	"errors"
	"testing"

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
