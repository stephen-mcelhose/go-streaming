package streaming

import (
	"errors"
	"io"
)

// ErrStreamStop is a sentinel error returned by receive handlers or stream consumers
// to request early, graceful termination of stream processing without marking the stream as failed.
//
// Unlike context.Canceled (which indicates external abort/timeout), ErrStreamStop indicates
// the consumer intentionally finished consuming early (e.g., iterator break, limit reached).
var ErrStreamStop = errors.New("stream processing stopped by consumer")

// IsCleanTermination returns true if err represents normal/clean stream termination:
// nil, io.EOF, or ErrStreamStop.
func IsCleanTermination(err error) bool {
	return err == nil || errors.Is(err, io.EOF) || errors.Is(err, ErrStreamStop)
}
