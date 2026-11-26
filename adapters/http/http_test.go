package httpstream_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	httpstream "github.com/stephen-mcelhose/go-streaming/adapters/http"
)

type notification struct {
	Msg  string `json:"msg"`
	Code int    `json:"code"`
}

func TestHTTPStreaming(t *testing.T) {
	ctx := context.Background()

	// 1. Create a test HTTP server serving SSE
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ch := make(chan notification, 2)
		ch <- notification{Msg: "started", Code: 100}
		ch <- notification{Msg: "completed", Code: 200}
		close(ch)

		source := streaming.NewChanStream(ch, nil)
		_ = httpstream.StreamToHTTP(r.Context(), w, source)
	}))
	defer server.Close()

	// 2. Consume from HTTP client via SSEReceiver
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	receiver := httpstream.NewSSEReceiver[notification](resp)
	var received []notification
	err = receiver.Receive(ctx, func(ctx context.Context, item notification) error {
		received = append(received, item)
		return nil
	})

	require.NoError(t, err)
	require.Len(t, received, 2)
	require.Equal(t, "started", received[0].Msg)
	require.Equal(t, "completed", received[1].Msg)
}
