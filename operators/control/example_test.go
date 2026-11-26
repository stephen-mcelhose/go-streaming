package control_test

import (
	"context"
	"fmt"
	"time"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/control"
)

// ExampleThrottle demonstrates rate-limiting external webhook deliveries to protect downstream APIs.
func ExampleThrottle() {
	ctx := context.Background()

	webhookQueue := make(chan string, 3)
	webhookQueue <- "https://api.partner.com/hook-1"
	webhookQueue <- "https://api.partner.com/hook-2"
	webhookQueue <- "https://api.partner.com/hook-3"
	close(webhookQueue)

	rateLimitedSink := make(chan string, 3)

	go func() {
		defer close(rateLimitedSink)
		// Limits throughput to at most 1 item per 10ms
		_ = control.Throttle(ctx, 10*time.Millisecond,
			streaming.NewChanStream(webhookQueue, nil),
			streaming.NewChanStream(nil, rateLimitedSink),
		)
	}()

	for hook := range rateLimitedSink {
		fmt.Println("Dispatched:", hook)
	}

	// Output:
	// Dispatched: https://api.partner.com/hook-1
	// Dispatched: https://api.partner.com/hook-2
	// Dispatched: https://api.partner.com/hook-3
}

// ExampleDebounce demonstrates squashing high-frequency search input keystrokes into settled queries.
func ExampleDebounce() {
	ctx := context.Background()

	keystrokes := make(chan string)
	settledQueries := make(chan string, 5)

	done := make(chan error, 1)
	go func() {
		defer close(settledQueries)
		done <- control.Debounce(ctx, 30*time.Millisecond,
			streaming.NewChanStream(keystrokes, nil),
			streaming.NewChanStream(nil, settledQueries),
		)
	}()

	// User types "g", "go", "gop" in rapid burst
	keystrokes <- "g"
	keystrokes <- "go"
	keystrokes <- "gopher"
	time.Sleep(60 * time.Millisecond) // Quiet period: "gopher" emits

	// User resumes typing after pause
	keystrokes <- "gophers"
	time.Sleep(60 * time.Millisecond) // Quiet period: "gophers" emits
	close(keystrokes)

	_ = <-done
	for query := range settledQueries {
		fmt.Println("Execute search query:", query)
	}

	// Output:
	// Execute search query: gopher
	// Execute search query: gophers
}
