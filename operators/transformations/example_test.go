package transformations_test

import (
	"context"
	"fmt"
	"strings"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

// AuditEvent represents a raw incoming security event.
type AuditEvent struct {
	TenantID string
	UserID   string
	Action   string
	Severity string
}

// EnrichedEvent represents an event mapped with normalized formatting and alerting priority.
type EnrichedEvent struct {
	TenantPrefix string
	UserTag      string
	Description  string
	IsCritical   bool
}

// ExampleMap demonstrates enriching raw security audit logs into normalized domain events.
func ExampleMap() {
	ctx := context.Background()

	rawLogs := make(chan AuditEvent, 2)
	rawLogs <- AuditEvent{TenantID: "acme", UserID: "usr_101", Action: "LOGIN_FAILED", Severity: "WARN"}
	rawLogs <- AuditEvent{TenantID: "globex", UserID: "usr_202", Action: "DB_DROP_TABLE", Severity: "CRITICAL"}
	close(rawLogs)

	enrichedEvents := make(chan EnrichedEvent, 2)
	inStream := streaming.NewChanStream(rawLogs, nil)
	outStream := streaming.NewChanStream(nil, enrichedEvents)

	go func() {
		defer close(enrichedEvents)
		_ = transformations.Map(ctx, func(ctx context.Context, raw AuditEvent) (EnrichedEvent, error) {
			return EnrichedEvent{
				TenantPrefix: strings.ToUpper(raw.TenantID),
				UserTag:      "@" + raw.UserID,
				Description:  fmt.Sprintf("[%s] performed %s", raw.TenantID, raw.Action),
				IsCritical:   raw.Severity == "CRITICAL",
			}, nil
		}, inStream, outStream)
	}()

	for event := range enrichedEvents {
		fmt.Printf("%s | %s | Critical=%t\n", event.TenantPrefix, event.Description, event.IsCritical)
	}

	// Output:
	// ACME | [acme] performed LOGIN_FAILED | Critical=false
	// GLOBEX | [globex] performed DB_DROP_TABLE | Critical=true
}

// ExampleFilter demonstrates filtering high-priority security alerts from a continuous stream.
func ExampleFilter() {
	ctx := context.Background()

	events := make(chan AuditEvent, 3)
	events <- AuditEvent{TenantID: "acme", Action: "LOGIN", Severity: "INFO"}
	events <- AuditEvent{TenantID: "acme", Action: "PASSWORD_RESET", Severity: "WARN"}
	events <- AuditEvent{TenantID: "acme", Action: "UNAUTHORIZED_ACCESS", Severity: "CRITICAL"}
	close(events)

	criticalAlerts := make(chan AuditEvent, 3)
	inStream := streaming.NewChanStream(events, nil)
	outStream := streaming.NewChanStream(nil, criticalAlerts)

	go func() {
		defer close(criticalAlerts)
		_ = transformations.Filter(ctx, func(ctx context.Context, ev AuditEvent) (bool, error) {
			return ev.Severity == "CRITICAL", nil
		}, inStream, outStream)
	}()

	for alert := range criticalAlerts {
		fmt.Printf("ALERT: %s on tenant %s\n", alert.Action, alert.TenantID)
	}

	// Output:
	// ALERT: UNAUTHORIZED_ACCESS on tenant acme
}

// OrderMetrics tracks financial summary across a batch of purchase transactions.
type OrderMetrics struct {
	TransactionCount int
	TotalGrossUSD    float64
}

// ExampleReduce demonstrates accumulating a bounded batch of transactions into financial summary metrics.
func ExampleReduce() {
	ctx := context.Background()

	transactions := make(chan float64, 3)
	transactions <- 120.50
	transactions <- 49.99
	transactions <- 250.00
	close(transactions)

	metricsOut := make(chan OrderMetrics, 1)
	inStream := streaming.NewChanStream(transactions, nil)
	outStream := streaming.NewChanStream(nil, metricsOut)

	go func() {
		defer close(metricsOut)
		_ = transformations.Reduce(ctx, OrderMetrics{}, func(ctx context.Context, acc OrderMetrics, amount float64) (OrderMetrics, error) {
			acc.TransactionCount++
			acc.TotalGrossUSD += amount
			return acc, nil
		}, inStream, outStream)
	}()

	metrics := <-metricsOut
	fmt.Printf("Processed %d transactions totaling $%.2f USD\n", metrics.TransactionCount, metrics.TotalGrossUSD)

	// Output:
	// Processed 3 transactions totaling $420.49 USD
}
