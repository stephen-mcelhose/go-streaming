package routing_test

import (
	"context"
	"fmt"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/routing"
)

// PaymentTransaction represents an incoming multi-currency payment.
type PaymentTransaction struct {
	ID       string
	Currency string
	Amount   float64
}

// ExamplePartition demonstrates routing multi-currency transactions into currency-specific processors
// with an unclassified/DLQ fallback stream for unexpected currencies.
func ExamplePartition() {
	ctx := context.Background()

	txStream := make(chan PaymentTransaction, 3)
	txStream <- PaymentTransaction{ID: "tx-1", Currency: "USD", Amount: 150.00}
	txStream <- PaymentTransaction{ID: "tx-2", Currency: "EUR", Amount: 80.00}
	txStream <- PaymentTransaction{ID: "tx-3", Currency: "BTC", Amount: 0.05}
	close(txStream)

	usdSink := make(chan PaymentTransaction, 1)
	eurSink := make(chan PaymentTransaction, 1)
	dlqSink := make(chan PaymentTransaction, 1)

	routes := map[string]streaming.Sender[PaymentTransaction]{
		"USD": streaming.NewChanStream(nil, usdSink),
		"EUR": streaming.NewChanStream(nil, eurSink),
	}

	go func() {
		defer close(usdSink)
		defer close(eurSink)
		defer close(dlqSink)

		_ = routing.Partition(ctx,
			func(tx PaymentTransaction) (string, error) { return tx.Currency, nil },
			routes,
			streaming.NewChanStream(nil, dlqSink), // Default fallback route for unsupported currencies
			streaming.NewChanStream(txStream, nil),
		)
	}()

	dlqTx := <-dlqSink
	fmt.Printf("USD Route: %s\n", (<-usdSink).ID)
	fmt.Printf("EUR Route: %s\n", (<-eurSink).ID)
	fmt.Printf("DLQ Route: %s (unsupported currency: %s)\n", dlqTx.ID, dlqTx.Currency)

	// Output:
	// USD Route: tx-1
	// EUR Route: tx-2
	// DLQ Route: tx-3 (unsupported currency: BTC)
}

// ExampleBroadcast demonstrates replicating security telemetry to both an audit database logger
// and a live incident alerting webhook.
func ExampleBroadcast() {
	ctx := context.Background()

	alertEvents := make(chan string, 1)
	alertEvents <- "CRITICAL_AUTH_FAILURE"
	close(alertEvents)

	dbLogger := make(chan string, 1)
	incidentWebhook := make(chan string, 1)

	go func() {
		defer close(dbLogger)
		defer close(incidentWebhook)

		_ = routing.Broadcast(ctx,
			streaming.NewChanStream(alertEvents, nil),
			streaming.NewChanStream(nil, dbLogger),
			streaming.NewChanStream(nil, incidentWebhook),
		)
	}()

	fmt.Printf("DB Log: %s\n", <-dbLogger)
	fmt.Printf("Incident Alert: %s\n", <-incidentWebhook)

	// Output:
	// DB Log: CRITICAL_AUTH_FAILURE
	// Incident Alert: CRITICAL_AUTH_FAILURE
}
