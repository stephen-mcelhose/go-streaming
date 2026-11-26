package examples_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/adapters/jsonl"
	"github.com/stephen-mcelhose/go-streaming/operators/aggregation"
	"github.com/stephen-mcelhose/go-streaming/operators/merging"
	"github.com/stephen-mcelhose/go-streaming/operators/routing"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

// RawClickEvent represents raw, uncleaned web events read directly from a log file or stream.
type RawClickEvent struct {
	TenantID  string `json:"tenant_id"`
	SessionID string `json:"session_id"`
	URL       string `json:"url"`
	Timestamp int64  `json:"ts"`
}

// CleanClickEvent represents validated records formatted for long-term database storage.
type CleanClickEvent struct {
	TenantID  string `json:"tenant_id"`
	SessionID string `json:"session_id"`
	Path      string `json:"path"`
	Timestamp int64  `json:"ts"`
}

// Example_dataEngineer demonstrates how to build an ETL data pipeline:
// 1. Reads line-delimited JSON (JSONL) records using adapters/jsonl.
// 2. Extracts and standardizes URL paths using transformations.Map.
// 3. Separates records by customer account (TenantID) using routing.Partition.
// 4. Groups single records into small batches (by count or timer) using aggregation.Batch before writing.
// 5. Combines pre-sorted historical log files in order using merging.MergeSort.
func Example_dataEngineer() {
	ctx := context.Background()

	// Ingress stream of JSON Lines log data
	jsonlSource := rawClickEventSource()

	// Step 1: Clean raw records by removing full domain names and keeping only URL paths
	cleanedCh := make(chan CleanClickEvent, 5)
	cleanedStream := streaming.NewChanStream(nil, cleanedCh)

	go func() {
		defer close(cleanedCh)
		_ = transformations.Map(ctx, func(ctx context.Context, raw RawClickEvent) (CleanClickEvent, error) {
			path := strings.TrimPrefix(raw.URL, "https://app.io")
			return CleanClickEvent{
				TenantID:  raw.TenantID,
				SessionID: raw.SessionID,
				Path:      path,
				Timestamp: raw.Timestamp,
			}, nil
		}, jsonlSource, cleanedStream)
	}()

	// Step 2: Route records to specific queues based on TenantID
	tenantA_Ch := make(chan CleanClickEvent, 5)
	tenantB_Ch := make(chan CleanClickEvent, 5)
	dlqCh := make(chan CleanClickEvent, 5)

	routes := map[string]streaming.Sender[CleanClickEvent]{
		"corp_a": streaming.NewChanStream(nil, tenantA_Ch),
		"corp_b": streaming.NewChanStream(nil, tenantB_Ch),
	}

	_ = routing.Partition(ctx,
		func(ev CleanClickEvent) (string, error) { return ev.TenantID, nil },
		routes,
		streaming.NewChanStream(nil, dlqCh), // Fallback destination if an unknown tenant ID arrives
		streaming.NewChanStream(cleanedCh, nil),
	)

	close(tenantA_Ch)
	close(tenantB_Ch)
	close(dlqCh)

	// Step 3: Group individual records into small batches (max 2 items or after 50ms)
	// This reduces network calls when writing to cloud storage or relational databases.
	batchedSink := make(chan []CleanClickEvent, 5)
	go func() {
		defer close(batchedSink)
		_ = aggregation.Batch(ctx, aggregation.BatchConfig{
			MaxSize: 2,
			MaxWait: 50 * time.Millisecond,
		}, streaming.NewChanStream(tenantA_Ch, nil), streaming.NewChanStream(nil, batchedSink))
	}()

	var stagingSink bytes.Buffer
	stageWriter := jsonl.NewJsonlStream[CleanClickEvent](nil, &stagingSink)

	for batch := range batchedSink {
		for _, item := range batch {
			_ = stageWriter.Send(ctx, item)
		}
	}

	fmt.Printf("Tenant A Staged Records:\n%s", stagingSink.String())

	// Step 4: Merge two pre-sorted streams in timestamp order (lowest timestamp first)
	p1, p2 := partitionedBackfillStreams()

	sortedSink := make(chan CleanClickEvent, 4)
	go func() {
		defer close(sortedSink)
		_ = merging.MergeSort(ctx,
			func(a, b CleanClickEvent) int { return int(a.Timestamp - b.Timestamp) },
			streaming.NewChanStream(nil, sortedSink),
			streaming.NewChanStream(p1, nil),
			streaming.NewChanStream(p2, nil),
		)
	}()

	fmt.Println("Globally Sorted Backfill:")
	for item := range sortedSink {
		fmt.Printf("ts=%d [%s] %s\n", item.Timestamp, item.TenantID, item.SessionID)
	}

	// Output:
	// Tenant A Staged Records:
	// {"tenant_id":"corp_a","session_id":"s_101","path":"/home","ts":1001}
	// {"tenant_id":"corp_a","session_id":"s_102","path":"/dashboard","ts":1003}
	// Globally Sorted Backfill:
	// ts=10 [corp_a] s_1
	// ts=20 [corp_b] s_2
	// ts=30 [corp_a] s_3
	// ts=40 [corp_b] s_4
}

// rawClickEventSource simulates reading log files from cloud object storage (such as AWS S3 or Google Cloud Storage).
// Each line contains a JSON object representing a user interaction on a website.
func rawClickEventSource() streaming.Receiver[RawClickEvent] {
	rawJSONL := `{"tenant_id":"corp_a","session_id":"s_101","url":"https://app.io/home","ts":1001}
{"tenant_id":"corp_b","session_id":"s_201","url":"https://app.io/checkout","ts":1002}
{"tenant_id":"corp_a","session_id":"s_102","url":"https://app.io/dashboard","ts":1003}
`
	reader := strings.NewReader(rawJSONL)
	return jsonl.NewJsonlStream[RawClickEvent](reader, nil)
}

// partitionedBackfillStreams provides two independent streams of log data that are already sorted by time.
// We use these to demonstrate how MergeSort combines separate log files into a single ordered timeline.
func partitionedBackfillStreams() (<-chan CleanClickEvent, <-chan CleanClickEvent) {
	p1 := make(chan CleanClickEvent, 2)
	p1 <- CleanClickEvent{TenantID: "corp_a", SessionID: "s_1", Timestamp: 10}
	p1 <- CleanClickEvent{TenantID: "corp_a", SessionID: "s_3", Timestamp: 30}
	close(p1)

	p2 := make(chan CleanClickEvent, 2)
	p2 <- CleanClickEvent{TenantID: "corp_b", SessionID: "s_2", Timestamp: 20}
	p2 <- CleanClickEvent{TenantID: "corp_b", SessionID: "s_4", Timestamp: 40}
	close(p2)

	return p1, p2
}
