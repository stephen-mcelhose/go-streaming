package jsonl_test

import (
	"bytes"
	"context"
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
	ctx := context.Background()

	input := `{"id":1,"name":"Alice"}
{"id":2,"name":"Bob"}
{"id":3,"name":"Charlie"}
`

	reader := strings.NewReader(input)
	stream := jsonl.NewJsonlStream[sampleRecord](reader, nil)

	var got []sampleRecord
	err := stream.Receive(ctx, func(ctx context.Context, item sampleRecord) error {
		got = append(got, item)
		return nil
	})

	require.NoError(t, err)
	require.Len(t, got, 3)
	require.Equal(t, "Alice", got[0].Name)
	require.Equal(t, "Bob", got[1].Name)
	require.Equal(t, "Charlie", got[2].Name)
}

func TestJsonlStream_Send(t *testing.T) {
	ctx := context.Background()

	var buf bytes.Buffer
	stream := jsonl.NewJsonlStream[sampleRecord](nil, &buf)

	err1 := stream.Send(ctx, sampleRecord{ID: 10, Name: "Dave"})
	err2 := stream.Send(ctx, sampleRecord{ID: 20, Name: "Eve"})

	require.NoError(t, err1)
	require.NoError(t, err2)

	expected := "{\"id\":10,\"name\":\"Dave\"}\n{\"id\":20,\"name\":\"Eve\"}\n"
	require.Equal(t, expected, buf.String())
}
