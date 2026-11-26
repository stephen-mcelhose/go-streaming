package merging_test

import (
	"context"
	"fmt"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/merging"
)

// ExampleConcat demonstrates sequentially processing a historical archive file followed by backfilled logs.
func ExampleConcat() {
	ctx := context.Background()

	archiveLogs := make(chan string, 2)
	archiveLogs <- "ARCHIVE: 2023-Q4-01"
	archiveLogs <- "ARCHIVE: 2023-Q4-02"
	close(archiveLogs)

	backfillLogs := make(chan string, 2)
	backfillLogs <- "BACKFILL: 2024-Q1-01"
	backfillLogs <- "BACKFILL: 2024-Q1-02"
	close(backfillLogs)

	combinedPipeline := make(chan string, 4)
	outStream := streaming.NewChanStream(nil, combinedPipeline)

	go func() {
		defer close(combinedPipeline)
		// Concat guarantees archive logs finish completely before backfill logs begin
		_ = merging.Concat(ctx, outStream,
			streaming.NewChanStream(archiveLogs, nil),
			streaming.NewChanStream(backfillLogs, nil),
		)
	}()

	for logEntry := range combinedPipeline {
		fmt.Println(logEntry)
	}

	// Output:
	// ARCHIVE: 2023-Q4-01
	// ARCHIVE: 2023-Q4-02
	// BACKFILL: 2024-Q1-01
	// BACKFILL: 2024-Q1-02
}

// ExampleInterleave demonstrates fair round-robin scheduling across high-priority and normal task queues.
func ExampleInterleave() {
	ctx := context.Background()

	priorityQ := make(chan string, 2)
	priorityQ <- "P-Task 1"
	priorityQ <- "P-Task 2"
	close(priorityQ)

	normalQ := make(chan string, 2)
	normalQ <- "N-Task 1"
	normalQ <- "N-Task 2"
	close(normalQ)

	workerQueue := make(chan string, 4)
	outStream := streaming.NewChanStream(nil, workerQueue)

	go func() {
		defer close(workerQueue)
		// Interleave guarantees fair turn-taking between priority and normal queues
		_ = merging.Interleave(ctx, outStream,
			streaming.NewChanStream(priorityQ, nil),
			streaming.NewChanStream(normalQ, nil),
		)
	}()

	for task := range workerQueue {
		fmt.Println(task)
	}

	// Output:
	// P-Task 1
	// N-Task 1
	// P-Task 2
	// N-Task 2
}
