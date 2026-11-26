package jsonl_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming/adapters/jsonl"
)

type sampleRecord struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestJsonlStream_Receive(t *testing.T) {
	type testCase struct {
		name              string
		setup             func() (context.Context, *jsonl.JsonlStream[sampleRecord])
		wantErr           error
		assertSideEffects func(t *testing.T, received []sampleRecord)
	}

	testCases := []testCase{
		// 1. Errors & Guards
		{
			name: "MUST return error when encountering malformed JSON line",
			setup: func() (context.Context, *jsonl.JsonlStream[sampleRecord]) {
				input := "{\"id\":1,\"name\":\"Alice\"}\n{INVALID_JSON}\n"
				return context.Background(), jsonl.NewJsonlStream[sampleRecord](strings.NewReader(input), nil)
			},
			wantErr: errors.New("invalid character 'I' looking for beginning of object key string"),
		},
		{
			name: "MUST return ctx.Err() when context is canceled mid-stream",
			setup: func() (context.Context, *jsonl.JsonlStream[sampleRecord]) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				input := "{\"id\":1,\"name\":\"Alice\"}\n"
				return ctx, jsonl.NewJsonlStream[sampleRecord](strings.NewReader(input), nil)
			},
			wantErr: context.Canceled,
		},
		// 2. Core Domain Happy Path
		{
			name: "MUST parse and deliver all valid JSON lines",
			setup: func() (context.Context, *jsonl.JsonlStream[sampleRecord]) {
				input := "{\"id\":1,\"name\":\"Alice\"}\n{\"id\":2,\"name\":\"Bob\"}\n{\"id\":3,\"name\":\"Charlie\"}\n"
				return context.Background(), jsonl.NewJsonlStream[sampleRecord](strings.NewReader(input), nil)
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, received []sampleRecord) {
				require.Len(t, received, 3)
				require.Equal(t, "Alice", received[0].Name)
				require.Equal(t, "Bob", received[1].Name)
				require.Equal(t, "Charlie", received[2].Name)
			},
		},
		// 3. Edge Cases
		{
			name: "MUST return nil cleanly without error when reader is nil",
			setup: func() (context.Context, *jsonl.JsonlStream[sampleRecord]) {
				return context.Background(), jsonl.NewJsonlStream[sampleRecord](nil, nil)
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, received []sampleRecord) {
				require.Empty(t, received)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, stream := tc.setup()
			var received []sampleRecord
			err := stream.Receive(ctx, func(ctx context.Context, item sampleRecord) error {
				received = append(received, item)
				return nil
			})

			if tc.wantErr != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr.Error())
				return
			}
			require.NoError(t, err)
			if tc.assertSideEffects != nil {
				tc.assertSideEffects(t, received)
			}
		})
	}
}

func TestJsonlStream_Send(t *testing.T) {
	type testCase struct {
		name              string
		setup             func() (context.Context, *bytes.Buffer, *jsonl.JsonlStream[sampleRecord])
		items             []sampleRecord
		wantErr           error
		assertSideEffects func(t *testing.T, buf *bytes.Buffer)
	}

	testCases := []testCase{
		// 1. Errors & Guards
		{
			name: "MUST return ctx.Err() when context is canceled before Send",
			setup: func() (context.Context, *bytes.Buffer, *jsonl.JsonlStream[sampleRecord]) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				buf := new(bytes.Buffer)
				return ctx, buf, jsonl.NewJsonlStream[sampleRecord](nil, buf)
			},
			items:   []sampleRecord{{ID: 1, Name: "Dave"}},
			wantErr: context.Canceled,
		},
		// 2. Core Domain Happy Path
		{
			name: "MUST serialize and append newline for every sent item",
			setup: func() (context.Context, *bytes.Buffer, *jsonl.JsonlStream[sampleRecord]) {
				buf := new(bytes.Buffer)
				return context.Background(), buf, jsonl.NewJsonlStream[sampleRecord](nil, buf)
			},
			items: []sampleRecord{
				{ID: 10, Name: "Dave"},
				{ID: 20, Name: "Eve"},
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, buf *bytes.Buffer) {
				expected := "{\"id\":10,\"name\":\"Dave\"}\n{\"id\":20,\"name\":\"Eve\"}\n"
				require.Equal(t, expected, buf.String())
			},
		},
		// 3. Edge Cases
		{
			name: "MUST succeed as no-op when writer is nil",
			setup: func() (context.Context, *bytes.Buffer, *jsonl.JsonlStream[sampleRecord]) {
				return context.Background(), nil, jsonl.NewJsonlStream[sampleRecord](nil, nil)
			},
			items:   []sampleRecord{{ID: 99, Name: "Ghost"}},
			wantErr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, buf, stream := tc.setup()
			var err error
			for _, it := range tc.items {
				if sendErr := stream.Send(ctx, it); sendErr != nil {
					err = sendErr
					break
				}
			}

			if tc.wantErr != nil {
				require.Error(t, err)
				require.Equal(t, tc.wantErr.Error(), err.Error())
				return
			}
			require.NoError(t, err)
			if tc.assertSideEffects != nil {
				tc.assertSideEffects(t, buf)
			}
		})
	}
}
