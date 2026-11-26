package aggregation

import (
	"context"

	"github.com/stephen-mcelhose/go-streaming"
)

// Buffer groups incoming items into fixed-size slices of size bufferSize and emits them to out.
// Any trailing items are emitted as a final smaller slice when the input stream completes.
func Buffer[T any](
	ctx context.Context,
	bufferSize int,
	in streaming.Receiver[T],
	out streaming.Sender[[]T],
) error {
	if bufferSize <= 0 {
		bufferSize = 1
	}

	var buf []T
	err := in.Receive(ctx, func(ctx context.Context, item T) error {
		buf = append(buf, item)
		if len(buf) >= bufferSize {
			toSend := buf
			buf = nil
			return out.Send(ctx, toSend)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if len(buf) > 0 {
		return out.Send(ctx, buf)
	}
	return nil
}
