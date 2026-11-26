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

// RawClickEvent represents raw unstructured clickstream events from Kafka/S3.
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

// Example_dataEngineer demonstrates how a Data Engineer implements a real-world Lakehouse ingestion pipeline:
// 1. Ingests raw JSONL events from an object store or stream source.
// 2. Normalizes URLs into clean analytical paths via transformations.Map.
// 3. Shards events by Tenant into dedicated partition queues via routing.Partition.
// 4. Micro-batches events (by count or timeout) via aggregation.Batch for high-throughput batch writes.
// 5. Merges multi-partition pre-sorted historical partition streams using merging.MergeSort for consistent backfill analytics.
func Example_dataEngineer() {
	ctx := context.Background()

	// Ingress raw JSONL data stream
	jsonlSource := rawClickEventSource()

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

	// Step 4: Multi-partition sorted merge (MergeSort) for backfill ordering
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

// rawClickEventSource provides a streaming JSONL source emulating continuous log files
// delivered from Cloud Object Storage (S3/GCS) or CDC message connectors.
// For Data Engineers and Lakehouse Architects, each line contains unstructured, multi-tenant click events
// with full URLs requiring parsing, validation, and schema conforming before analytical table ingestion.
func rawClickEventSource() streaming.Receiver[RawClickEvent] {
	rawJSONL := `{"tenant_id":"corp_a","session_id":"s_101","url":"https://app.io/home","ts":1001}
{"tenant_id":"corp_b","session_id":"s_201","url":"https://app.io/checkout","ts":1002}
{"tenant_id":"corp_a","session_id":"s_102","url":"https://app.io/dashboard","ts":1003}
`
	reader := strings.NewReader(rawJSONL)
	return jsonl.NewJsonlStream[RawClickEvent](reader, nil)
}

// partitionedBackfillStreams creates two independently ordered historical partition streams
// representing distinct data shards (e.g., Kafka topic partitions or historical hourly S3 prefixes).
// For Data Engineers executing backfills or compaction runs, MergeSort merges these pre-sorted partitions
// into a globally ordered chronological time-series stream in O(N log K) time without loading entire datasets into memory.
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
