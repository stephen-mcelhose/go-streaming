package streaming_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
)

func TestCleanTermination(t *testing.T) {
	type testCase struct {
		name string
		err  error
		want bool
	}

	testCases := []testCase{
		{
			name: "MUST return true for nil error",
			err:  nil,
			want: true,
		},
		{
			name: "MUST return true for io.EOF",
			err:  io.EOF,
			want: true,
		},
		{
			name: "MUST return true for ErrStreamStop",
			err:  streaming.ErrStreamStop,
			want: true,
		},
		{
			name: "MUST return false for context.Canceled",
			err:  context.Canceled,
			want: false,
		},
		{
			name: "MUST return false for generic domain error",
			err:  errors.New("db connection failure"),
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := streaming.IsCleanTermination(tc.err)
			require.Equal(t, tc.want, got)
		})
	}
}
