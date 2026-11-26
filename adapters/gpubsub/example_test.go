package gpubsub_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/stephen-mcelhose/go-streaming/adapters/gpubsub"
)

type userSignup struct {
	Username string `json:"username"`
	Email    string `json:"email"`
}

func ExamplePubsubReceiver() {
	ctx := context.Background()

	data, _ := json.Marshal(userSignup{Username: "johndoe", Email: "john@example.com"})
	msg := gpubsub.NewMessage("msg-1", data, nil, func() {
		fmt.Println("ACK message msg-1")
	}, func() {
		fmt.Println("NACK message msg-1")
	})

	sub := &mockSub{messages: []*gpubsub.Message{msg}}
	receiver := gpubsub.NewPubsubReceiver[userSignup](sub)

	_ = receiver.Receive(ctx, func(ctx context.Context, signup userSignup) error {
		fmt.Printf("Processed signup for: %s\n", signup.Username)
		return nil
	})

	// Output:
	// Processed signup for: johndoe
	// ACK message msg-1
}
