package grpcstream_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	grpcstream "github.com/stephen-mcelhose/go-streaming/adapters/grpc"
)

type grpcMockServerStream struct {
	items []string
	idx   int
	err   error
}

func (m *grpcMockServerStream) Recv() (*string, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.idx >= len(m.items) {
		return nil, io.EOF
	}
	item := m.items[m.idx]
	m.idx++
	return &item, nil
}

type grpcMockClientStream struct {
	sent []string
	err  error
}

func (m *grpcMockClientStream) Send(item *string) error {
	if m.err != nil {
		return m.err
	}
	m.sent = append(m.sent, *item)
	return nil
}

func (m *grpcMockClientStream) CloseAndRecv() error {
	return nil
}

func TestGRPCAdapters(t *testing.T) {
	type receiveTestCase struct {
		name              string
		setup             func() (context.Context, *grpcstream.StreamReceiver[string])
		wantErr           error
		assertSideEffects func(t *testing.T, received []string)
	}

	receiveCases := []receiveTestCase{
		// 1. Errors & Guards
		{
			name: "MUST propagate non-EOF transport error from gRPC server stream",
			setup: func() (context.Context, *grpcstream.StreamReceiver[string]) {
				mockServer := &grpcMockServerStream{err: errors.New("rpc transport connection reset")}
				return context.Background(), grpcstream.NewStreamReceiver[string](mockServer)
			},
			wantErr: errors.New("rpc transport connection reset"),
		},
		{
			name: "MUST return ctx.Err() when context is canceled during receive",
			setup: func() (context.Context, *grpcstream.StreamReceiver[string]) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				mockServer := &grpcMockServerStream{items: []string{"msg1"}}
				return ctx, grpcstream.NewStreamReceiver[string](mockServer)
			},
			wantErr: context.Canceled,
		},
		// 2. Core Domain Happy Path
		{
			name: "MUST receive all items until server stream returns io.EOF",
			setup: func() (context.Context, *grpcstream.StreamReceiver[string]) {
				mockServer := &grpcMockServerStream{items: []string{"g1", "g2", "g3"}}
				return context.Background(), grpcstream.NewStreamReceiver[string](mockServer)
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, received []string) {
				require.Equal(t, []string{"g1", "g2", "g3"}, received)
			},
		},
	}

	for _, tc := range receiveCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, rx := tc.setup()
			var received []string
			err := rx.Receive(ctx, func(ctx context.Context, item string) error {
				received = append(received, item)
				return nil
			})

			if tc.wantErr != nil {
				require.Error(t, err)
				require.Equal(t, tc.wantErr.Error(), err.Error())
				return
			}
			require.NoError(t, err)
			if tc.assertSideEffects != nil {
				tc.assertSideEffects(t, received)
			}
		})
	}

	type sendTestCase struct {
		name    string
		setup   func() (context.Context, *grpcstream.StreamSender[string], *grpcMockClientStream)
		items   []string
		wantErr error
	}

	sendCases := []sendTestCase{
		// 1. Errors & Guards
		{
			name: "MUST return error when client stream Send fails",
			setup: func() (context.Context, *grpcstream.StreamSender[string], *grpcMockClientStream) {
				mockClient := &grpcMockClientStream{err: errors.New("buffer full")}
				return context.Background(), grpcstream.NewStreamSender[string](mockClient), mockClient
			},
			items:   []string{"req1"},
			wantErr: errors.New("buffer full"),
		},
		{
			name: "MUST return ctx.Err() when context is canceled before send",
			setup: func() (context.Context, *grpcstream.StreamSender[string], *grpcMockClientStream) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				mockClient := &grpcMockClientStream{}
				return ctx, grpcstream.NewStreamSender[string](mockClient), mockClient
			},
			items:   []string{"req1"},
			wantErr: context.Canceled,
		},
		// 2. Core Domain Happy Path
		{
			name: "MUST forward all items to underlying gRPC client stream",
			setup: func() (context.Context, *grpcstream.StreamSender[string], *grpcMockClientStream) {
				mockClient := &grpcMockClientStream{}
				return context.Background(), grpcstream.NewStreamSender[string](mockClient), mockClient
			},
			items:   []string{"req1", "req2"},
			wantErr: nil,
		},
	}

	for _, tc := range sendCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, tx, client := tc.setup()
			var err error
			for _, item := range tc.items {
				if sendErr := tx.Send(ctx, item); sendErr != nil {
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
			require.Equal(t, tc.items, client.sent)
		})
	}
}
