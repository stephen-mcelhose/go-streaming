package httpstream

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/stephen-mcelhose/go-streaming"
)

// SSEStreamer writes events using the text/event-stream format over an HTTP response.
type SSEStreamer[T any] struct {
	w       http.ResponseWriter
	flusher http.Flusher
}

// NewSSEStreamer constructs an SSEStreamer.
func NewSSEStreamer[T any](w http.ResponseWriter) (*SSEStreamer[T], error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, fmt.Errorf("streaming unsupported: ResponseWriter does not implement http.Flusher")
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	return &SSEStreamer[T]{w: w, flusher: flusher}, nil
}

// Send encodes item as JSON and writes a data: payload followed by newline and flush.
func (s *SSEStreamer[T]) Send(ctx context.Context, item T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	data, err := json.Marshal(item)
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintf(s.w, "data: %s\n\n", data); err != nil {
		return err
	}
	s.flusher.Flush()
	return nil
}

// SSEReceiver parses an incoming Server-Sent Events stream from an HTTP response body.
type SSEReceiver[T any] struct {
	resp *http.Response
}

// NewSSEReceiver creates a receiver over an SSE HTTP response.
func NewSSEReceiver[T any](resp *http.Response) *SSEReceiver[T] {
	return &SSEReceiver[T]{resp: resp}
}

// Receive scans SSE chunks, extracting lines starting with "data: " and unmarshaling into T.
func (r *SSEReceiver[T]) Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error {
	defer r.resp.Body.Close()

	scanner := bufio.NewScanner(r.resp.Body)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			payload := strings.TrimPrefix(line, "data: ")
			var item T
			if err := json.Unmarshal([]byte(payload), &item); err != nil {
				return err
			}
			if err := handler(ctx, item); err != nil {
				return err
			}
		}
	}
	return scanner.Err()
}

// StreamToHTTP streams all items from receiver to the given SSE response writer.
func StreamToHTTP[T any](ctx context.Context, w http.ResponseWriter, source streaming.Receiver[T]) error {
	streamer, err := NewSSEStreamer[T](w)
	if err != nil {
		return err
	}
	return source.Receive(ctx, func(ctx context.Context, item T) error {
		return streamer.Send(ctx, item)
	})
}
