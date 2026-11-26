// Package jsonl implements Stream[T] over line-delimited JSON streams.
package jsonl

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
)

// JsonlStream implements Stream[T] over an io.Reader and io.Writer using JSON Lines framing.
type JsonlStream[T any] struct {
	reader io.Reader
	writer io.Writer
	mu     sync.Mutex
}

// NewJsonlStream constructs a JsonlStream wrapping the given reader and writer.
// Either reader or writer may be nil if only one direction is required.
func NewJsonlStream[T any](reader io.Reader, writer io.Writer) *JsonlStream[T] {
	return &JsonlStream[T]{
		reader: reader,
		writer: writer,
	}
}

// Receive reads lines from the underlying reader, deserializes JSON into T, and passes each item to handler.
func (s *JsonlStream[T]) Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error {
	if s.reader == nil {
		return nil
	}

	scanner := bufio.NewScanner(s.reader)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		var item T
		if err := json.Unmarshal(line, &item); err != nil {
			return err
		}

		if err := handler(ctx, item); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

// Send serializes item as JSON and writes it with a trailing newline to the underlying writer.
func (s *JsonlStream[T]) Send(ctx context.Context, item T) error {
	if s.writer == nil {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()

	_, err = s.writer.Write(data)
	return err
}
