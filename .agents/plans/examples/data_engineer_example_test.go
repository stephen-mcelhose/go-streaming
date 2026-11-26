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

// RawClickEvent represents raw unstructured clickstream events.
type RawClickEvent struct {
	TenantID  string `json:"tenant_id"`
	SessionID string `json:"session_id"`
	URL       string `json:"url"`
	Timestamp int64  `json:"ts"`
}

// CleanClickEvent represents validated, schema-conforming analytics events.
type CleanClickEvent struct {
	TenantID  string `json:"tenant_id"`
	SessionID string `json:"session_id"`
	Path      string `json:"path"`
	Timestamp int64  `json:"ts"`
}

// Example_dataEngineerWorkflow demonstrates how a Data Engineer implements a real-world pipeline:
// 1. Ingests raw JSONL events from an object store or stream source.
// 2. Cleans URLs into standardized paths.
// 3. Shards events by Tenant into tenant-specific processing queues.
// 4. Micro-batches events (by count or timeout) for high-throughput batch writes.
// 5. Merges multi-partition pre-sorted historical partition streams using MergeSort for consistent analytics.
func Example_dataEngineerWorkflow() {
	ctx := context.Background()

	// Ingress raw JSONL data stream (e.g. from GCS/S3 or Kafka connector)
	rawJSONL := `{"tenant_id":"corp_a","session_id":"s_101","url":"https://app.io/home","ts":1001}
{"tenant_id":"corp_b","session_id":"s_201","url":"https://app.io/checkout","ts":1002}
{"tenant_id":"corp_a","session_id":"s_102","url":"https://app.io/dashboard","ts":1003}
`
	reader := strings.NewReader(rawJSONL)
	jsonlSource := jsonl.NewJsonlStream[RawClickEvent](reader, nil)

	// Step 1: Clean and extract normalized path
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

	// Step 2: Route by Tenant using Partition
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
		streaming.NewChanStream(nil, dlqCh),
		streaming.NewChanStream(cleanedCh, nil),
	)

	close(tenantA_Ch)
	close(tenantB_Ch)
	close(dlqCh)

	// Step 3: Micro-batch Tenant A records for Lakehouse staging
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

	// Step 4: Multi-partition sorted merge (MergeSort)
	p1 := make(chan CleanClickEvent, 2)
	p1 <- CleanClickEvent{TenantID: "corp_a", SessionID: "s_1", Timestamp: 10}
	p1 <- CleanClickEvent{TenantID: "corp_a", SessionID: "s_3", Timestamp: 30}
	close(p1)

	p2 := make(chan CleanClickEvent, 2)
	p2 <- CleanClickEvent{TenantID: "corp_b", SessionID: "s_2", Timestamp: 20}
	p2 <- CleanClickEvent{TenantID: "corp_b", SessionID: "s_4", Timestamp: 40}
	close(p2)

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
