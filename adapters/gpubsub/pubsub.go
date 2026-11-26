// Package gpubsub provides an official Google Cloud Pub/Sub adapter for streaming.Receiver and streaming.Sender.
//
// Lifecycle & Error Handling Contract:
//   - If the receive handler returns nil, the adapter MUST call msg.Ack().
//   - If the receive handler returns a processing error (other than context cancellation or io.EOF),
//     the adapter MUST call msg.Nack() and MUST NOT terminate the receive loop (it continues processing messages).
//   - If the context is canceled (ctx.Err() != nil), the adapter MUST call msg.Nack() and return ctx.Err().
//   - If the handler returns io.EOF, the adapter MUST call msg.Ack() on the current message and return nil
//     to indicate clean stream termination.
package gpubsub

import (
	"context"
	"encoding/json"
	"errors"
	"io"
)

// Message represents a Pub/Sub message contract required by the adapter.
type Message struct {
	ID         string
	Data       []byte
	Attributes map[string]string
	ackFunc    func()
	nackFunc   func()
}

// Ack acknowledges the Pub/Sub message.
func (m *Message) Ack() {
	if m.ackFunc != nil {
		m.ackFunc()
	}
}

// Nack negatively acknowledges the Pub/Sub message for redelivery.
func (m *Message) Nack() {
	if m.nackFunc != nil {
		m.nackFunc()
	}
}

// NewMessage constructs a Message with custom ack and nack handlers (primarily for testing and custom wrappers).
func NewMessage(id string, data []byte, attrs map[string]string, ack func(), nack func()) *Message {
	return &Message{
		ID:         id,
		Data:       data,
		Attributes: attrs,
		ackFunc:    ack,
		nackFunc:   nack,
	}
}

// SubscriptionReceiver abstracts cloud.google.com/go/pubsub Subscription.Receive.
type SubscriptionReceiver interface {
	Receive(ctx context.Context, f func(ctx context.Context, msg *Message)) error
}

// TopicPublisher abstracts cloud.google.com/go/pubsub Topic.Publish.
type TopicPublisher interface {
	Publish(ctx context.Context, msg *Message) error
}

// PubsubReceiver adapts a Google Cloud Pub/Sub subscription to streaming.Receiver[T].
type PubsubReceiver[T any] struct {
	sub SubscriptionReceiver
}

// NewPubsubReceiver constructs a PubsubReceiver.
func NewPubsubReceiver[T any](sub SubscriptionReceiver) *PubsubReceiver[T] {
	return &PubsubReceiver[T]{sub: sub}
}

// Receive processes messages from the subscription. It manages ACK/NACK automatically per message:
// - Successful handling (nil error) triggers msg.Ack().
// - Transient errors trigger msg.Nack() without aborting the subscription loop.
// - Context cancellation triggers msg.Nack() and halts.
// - io.EOF triggers msg.Ack() and halts cleanly with nil.
func (r *PubsubReceiver[T]) Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error {
	if r.sub == nil {
		return nil
	}

	return r.sub.Receive(ctx, func(msgCtx context.Context, msg *Message) {
		select {
		case <-ctx.Done():
			msg.Nack()
			return
		default:
		}

		var item T
		if err := json.Unmarshal(msg.Data, &item); err != nil {
			// Unmarshal failure is non-retryable for this message format; Nack to allow DLQ routing
			msg.Nack()
			return
		}

		err := handler(msgCtx, item)
		if err == nil {
			msg.Ack()
			return
		}

		if errors.Is(err, io.EOF) {
			msg.Ack()
			return
		}

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			msg.Nack()
			return
		}

		// Standard domain processing error: Nack to retry, but keep receiving next messages
		msg.Nack()
	})
}

// PubsubSender adapts a Google Cloud Pub/Sub topic to streaming.Sender[T].
type PubsubSender[T any] struct {
	topic TopicPublisher
}

// NewPubsubSender constructs a PubsubSender.
func NewPubsubSender[T any](topic TopicPublisher) *PubsubSender[T] {
	return &PubsubSender[T]{topic: topic}
}

// Send serializes item to JSON and publishes to the topic.
func (s *PubsubSender[T]) Send(ctx context.Context, item T) error {
	if s.topic == nil {
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

	msg := &Message{Data: data}
	return s.topic.Publish(ctx, msg)
}

// PubsubStream combines PubsubReceiver and PubsubSender into a bidirectional Stream[T].
type PubsubStream[T any] struct {
	*PubsubReceiver[T]
	*PubsubSender[T]
}

// NewPubsubStream creates a unified PubSub Stream[T].
func NewPubsubStream[T any](sub SubscriptionReceiver, topic TopicPublisher) *PubsubStream[T] {
	return &PubsubStream[T]{
		PubsubReceiver: NewPubsubReceiver[T](sub),
		PubsubSender:   NewPubsubSender[T](topic),
	}
}
