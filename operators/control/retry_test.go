package control_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/control"
)

type failSender struct {
	attempts  int
	succeedOn int
}

func (f *failSender) Send(ctx context.Context, item string) error {
	f.attempts++
	if f.attempts >= f.succeedOn {
		return nil
	}
	return errors.New("transient error")
}

func TestRetrySender(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST succeed when within retry attempts", func(t *testing.T) {
		fs := &failSender{succeedOn: 3}
		retryer := control.NewRetrySender[string](fs, control.RetryPolicy[string]{
			MaxAttempts:    3,
			InitialBackoff: 5 * time.Millisecond,
		})

		err := retryer.Send(ctx, "data")
		require.NoError(t, err)
		require.Equal(t, 3, fs.attempts)
	})

	t.Run("MUST route to FallbackSender when retries are exhausted", func(t *testing.T) {
		fs := &failSender{succeedOn: 99} // Never succeeds
		dlqCh := make(chan string, 1)

		retryer := control.NewRetrySender[string](fs, control.RetryPolicy[string]{
			MaxAttempts:    2,
			InitialBackoff: 5 * time.Millisecond,
			FallbackSender: streaming.NewChanStream(nil, dlqCh),
		})

		err := retryer.Send(ctx, "failed-payment")
		require.NoError(t, err)
		require.Equal(t, 2, fs.attempts)

		close(dlqCh)
		require.Equal(t, "failed-payment", <-dlqCh)
	})

	t.Run("MUST fail when exceeding max attempts without fallback", func(t *testing.T) {
		fs := &failSender{succeedOn: 5}
		retryer := control.NewRetrySender[string](fs, control.RetryPolicy[string]{
			MaxAttempts:    2,
			InitialBackoff: 5 * time.Millisecond,
		})

		err := retryer.Send(ctx, "data")
		require.Error(t, err)
		require.Equal(t, 2, fs.attempts)
	})
}
