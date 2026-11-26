package grpcstream_test

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	grpcstream "github.com/stephen-mcelhose/go-streaming/adapters/grpc"
)

type grpcMockServerStream struct {
	items []string
	idx   int
}

func (m *grpcMockServerStream) Recv() (*string, error) {
	if m.idx >= len(m.items) {
		return nil, io.EOF
	}
	item := m.items[m.idx]
	m.idx++
	return &item, nil
}

type grpcMockClientStream struct {
	sent []string
}

func (m *grpcMockClientStream) Send(item *string) error {
	m.sent = append(m.sent, *item)
	return nil
}

func (m *grpcMockClientStream) CloseAndRecv() error {
	return nil
}

func TestGRPCAdapters(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST receive from server stream", func(t *testing.T) {
		mockServer := &grpcMockServerStream{items: []string{"g1", "g2", "g3"}}
		rx := grpcstream.NewStreamReceiver[string](mockServer)

		var got []string
		err := rx.Receive(ctx, func(ctx context.Context, item string) error {
			got = append(got, item)
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, []string{"g1", "g2", "g3"}, got)
	})

	t.Run("MUST send to client stream", func(t *testing.T) {
		mockClient := &grpcMockClientStream{}
		tx := grpcstream.NewStreamSender[string](mockClient)

		err1 := tx.Send(ctx, "req1")
		err2 := tx.Send(ctx, "req2")

		require.NoError(t, err1)
		require.NoError(t, err2)
		require.Equal(t, []string{"req1", "req2"}, mockClient.sent)
	})
}
