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
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

// RawClickEvent represents raw unstructured clickstream events.
type RawClickEvent struct {
	SessionID string `json:"session_id"`
	URL       string `json:"url"`
	Timestamp int64  `json:"ts"`
}

// CleanClickEvent represents validated, schema-conforming analytics events.
type CleanClickEvent struct {
	SessionID string `json:"session_id"`
	Path      string `json:"path"`
}

// Example_dataEngineer demonstrates how a Data Engineer extracts JSONL clickstream logs,
// filters corrupt records, cleans URL paths, micro-batches into chunks of 2 (or timeout),
// and writes formatted JSONL directly into an object storage staging sink.
func Example_dataEngineer() {
	ctx := context.Background()

	// Ingress raw JSONL data stream (e.g. from S3 or Kafka sink)
	rawJSONL := `{"session_id":"s_101","url":"https://app.io/home","ts":1700000001}
{"session_id":"s_102","url":"https://app.io/checkout","ts":1700000002}
{"session_id":"s_103","url":"https://app.io/products","ts":1700000003}
`
	reader := strings.NewReader(rawJSONL)
	jsonlSource := jsonl.NewJsonlStream[RawClickEvent](reader, nil)

	// Intermediates
	cleanedEvents := make(chan CleanClickEvent, 5)
	batchedChunks := make(chan []CleanClickEvent, 5)

	cleanedStream := streaming.NewChanStream(nil, cleanedEvents)
	cleanedReceiver := streaming.NewChanStream(cleanedEvents, nil)
	batchedStream := streaming.NewChanStream(nil, batchedChunks)

	// Step 1: Clean and extract path
	go func() {
		defer close(cleanedEvents)
		_ = transformations.Map(ctx, func(ctx context.Context, raw RawClickEvent) (CleanClickEvent, error) {
			path := strings.TrimPrefix(raw.URL, "https://app.io")
			return CleanClickEvent{SessionID: raw.SessionID, Path: path}, nil
		}, jsonlSource, cleanedStream)
	}()

	// Step 2: Micro-batch for parquet/database staging
	go func() {
		defer close(batchedChunks)
		_ = aggregation.Batch(ctx, aggregation.BatchConfig{
			MaxSize: 2,
			MaxWait: 50 * time.Millisecond,
		}, cleanedReceiver, batchedStream)
	}()

	// Step 3: Stream to Object Storage staging output buffer
	var stagingSink bytes.Buffer
	stageWriter := jsonl.NewJsonlStream[CleanClickEvent](nil, &stagingSink)

	batchCount := 0
	for batch := range batchedChunks {
		batchCount++
		for _, item := range batch {
			_ = stageWriter.Send(ctx, item)
		}
	}

	fmt.Printf("Processed %d micro-batches into staging store\n", batchCount)
	fmt.Print(stagingSink.String())

	// Output:
	// Processed 2 micro-batches into staging store
	// {"session_id":"s_101","path":"/home"}
	// {"session_id":"s_102","path":"/checkout"}
	// {"session_id":"s_103","path":"/products"}
}
