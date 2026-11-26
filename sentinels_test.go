package streaming_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
)

func TestSentinels(t *testing.T) {
	ctx := context.Background()

	t.Run("NoopSender MUST succeed without error", func(t *testing.T) {
		sender := streaming.NewNoopSender[int]()
		err := sender.Send(ctx, 42)
		require.NoError(t, err)
	})

	t.Run("NoopReceiver MUST return nil without calling handler", func(t *testing.T) {
		receiver := streaming.NewNoopReceiver[int]()
		called := false
		err := receiver.Receive(ctx, func(ctx context.Context, item int) error {
			called = true
			return nil
		})
		require.NoError(t, err)
		require.False(t, called)
	})

	t.Run("EOFSender MUST return io.EOF", func(t *testing.T) {
		sender := streaming.NewEOFSender[string]()
		err := sender.Send(ctx, "test")
		require.Equal(t, io.EOF, err)
	})

	t.Run("EOFReceiver MUST return io.EOF without invoking handler", func(t *testing.T) {
		receiver := streaming.NewEOFReceiver[string]()
		called := false
		err := receiver.Receive(ctx, func(ctx context.Context, item string) error {
			called = true
			return nil
		})
		require.Equal(t, io.EOF, err)
		require.False(t, called)
	})
}

func TestChanStream_Receive(t *testing.T) {
	type testCase struct {
		name              string
		setup             func() (context.Context, <-chan int, func(ctx context.Context, item int) error)
		wantErr           error
		assertSideEffects func(t *testing.T, received []int)
	}

	testCases := []testCase{
		{
			name: "MUST return ctx.Err() when context is already canceled",
			setup: func() (context.Context, <-chan int, func(ctx context.Context, item int) error) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				ch := make(chan int, 1)
				ch <- 1
				return ctx, ch, func(ctx context.Context, item int) error { return nil }
			},
			wantErr: context.Canceled,
		},
		{
			name: "MUST return handler error and stop receiving",
			setup: func() (context.Context, <-chan int, func(ctx context.Context, item int) error) {
				ch := make(chan int, 3)
				ch <- 10
				ch <- 20
				ch <- 30
				return context.Background(), ch, func(ctx context.Context, item int) error {
					if item == 20 {
						return errors.New("handler error")
					}
					return nil
				}
			},
			wantErr: errors.New("handler error"),
		},
		{
			name: "MUST receive all items until channel is closed",
			setup: func() (context.Context, <-chan int, func(ctx context.Context, item int) error) {
				ch := make(chan int, 3)
				ch <- 1
				ch <- 2
				ch <- 3
				close(ch)
				return context.Background(), ch, nil
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, received []int) {
				require.Equal(t, []int{1, 2, 3}, received)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, ch, handler := tc.setup()
			var received []int
			if handler == nil {
				handler = func(ctx context.Context, item int) error {
					received = append(received, item)
					return nil
				}
			}

			stream := streaming.NewChanStream(ch, nil)
			err := stream.Receive(ctx, handler)

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
}

func TestChanStream_Send(t *testing.T) {
	type testCase struct {
		name    string
		setup   func() (context.Context, chan int)
		item    int
		wantErr error
	}

	testCases := []testCase{
		{
			name: "MUST return ctx.Err() when context canceled",
			setup: func() (context.Context, chan int) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				ch := make(chan int)
				return ctx, ch
			},
			item:    99,
			wantErr: context.Canceled,
		},
		{
			name: "MUST send item to buffered channel",
			setup: func() (context.Context, chan int) {
				return context.Background(), make(chan int, 1)
			},
			item:    42,
			wantErr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, ch := tc.setup()
			stream := streaming.NewChanStream(nil, ch)
			err := stream.Send(ctx, tc.item)

			if tc.wantErr != nil {
				require.Error(t, err)
				require.Equal(t, tc.wantErr.Error(), err.Error())
				return
			}
			require.NoError(t, err)
			got := <-ch
			require.Equal(t, tc.item, got)
		})
	}
}

func TestChanStreamCloser(t *testing.T) {
	ch := make(chan int, 2)
	stream := streaming.NewChanStreamCloser(nil, ch)

	err := stream.Close()
	require.NoError(t, err)

	// Channel must now be closed
	_, ok := <-ch
	require.False(t, ok)
}
