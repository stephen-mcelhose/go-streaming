package gpubsub_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming/adapters/gpubsub"
)

type mockSub struct {
	messages []*gpubsub.Message
}

func (m *mockSub) Receive(ctx context.Context, f func(context.Context, *gpubsub.Message)) error {
	for _, msg := range m.messages {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		f(ctx, msg)
	}
	return nil
}

type mockTopic struct {
	mu        sync.Mutex
	published []*gpubsub.Message
}

func (m *mockTopic) Publish(ctx context.Context, msg *gpubsub.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = append(m.published, msg)
	return nil
}

type orderEvent struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

func TestPubsubReceiver_Lifecycle(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST Ack on successful processing", func(t *testing.T) {
		acked := false
		nacked := false

		data, _ := json.Marshal(orderEvent{OrderID: "101", Amount: 50.0})
		msg := gpubsub.NewMessage("msg-1", data, nil, func() { acked = true }, func() { nacked = true })

		sub := &mockSub{messages: []*gpubsub.Message{msg}}
		rx := gpubsub.NewPubsubReceiver[orderEvent](sub)

		var received []orderEvent
		err := rx.Receive(ctx, func(ctx context.Context, item orderEvent) error {
			received = append(received, item)
			return nil
		})

		require.NoError(t, err)
		require.Len(t, received, 1)
		require.True(t, acked)
		require.False(t, nacked)
	})

	t.Run("MUST Nack on error and continue receiving remaining messages", func(t *testing.T) {
		var acks []string
		var nacks []string

		d1, _ := json.Marshal(orderEvent{OrderID: "err-1", Amount: 10.0})
		m1 := gpubsub.NewMessage("1", d1, nil, func() { acks = append(acks, "1") }, func() { nacks = append(nacks, "1") })

		d2, _ := json.Marshal(orderEvent{OrderID: "ok-2", Amount: 20.0})
		m2 := gpubsub.NewMessage("2", d2, nil, func() { acks = append(acks, "2") }, func() { nacks = append(nacks, "2") })

		sub := &mockSub{messages: []*gpubsub.Message{m1, m2}}
		rx := gpubsub.NewPubsubReceiver[orderEvent](sub)

		err := rx.Receive(ctx, func(ctx context.Context, item orderEvent) error {
			if item.OrderID == "err-1" {
				return errors.New("temporary database connection failure")
			}
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, []string{"1"}, nacks)
		require.Equal(t, []string{"2"}, acks)
	})

	t.Run("MUST Ack on io.EOF to cleanly complete", func(t *testing.T) {
		acked := false
		data, _ := json.Marshal(orderEvent{OrderID: "eof-1", Amount: 10.0})
		msg := gpubsub.NewMessage("1", data, nil, func() { acked = true }, func() {})

		sub := &mockSub{messages: []*gpubsub.Message{msg}}
		rx := gpubsub.NewPubsubReceiver[orderEvent](sub)

		err := rx.Receive(ctx, func(ctx context.Context, item orderEvent) error {
			return io.EOF
		})

		require.NoError(t, err)
		require.True(t, acked)
	})
}

func TestPubsubSender(t *testing.T) {
	ctx := context.Background()
	topic := &mockTopic{}
	sender := gpubsub.NewPubsubSender[orderEvent](topic)

	err := sender.Send(ctx, orderEvent{OrderID: "pub-1", Amount: 99.9})
	require.NoError(t, err)

	require.Len(t, topic.published, 1)
	var decoded orderEvent
	err = json.Unmarshal(topic.published[0].Data, &decoded)
	require.NoError(t, err)
	require.Equal(t, "pub-1", decoded.OrderID)
}
