# go-streaming

[![Go Reference](https://pkg.go.dev/badge/github.com/stephen-mcelhose/go-streaming.svg)](https://pkg.go.dev/github.com/stephen-mcelhose/go-streaming)
[![Go Report Card](https://goreportcard.com/badge/github.com/stephen-mcelhose/go-streaming)](https://goreportcard.com/report/github.com/stephen-mcelhose/go-streaming)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A modular, composable, typed stream-processing library for Go built on simple, context-aware `Receiver[T]` and `Sender[T]` primitives.

---

## Highlights

- **Typed Primitives**: Generic `Receiver[T]` and `Sender[T]` contracts decoupled from underlying transport mechanisms.
- **Context & Error Propagation**: All primitives enforce `context.Context` cancellation and explicit error handling out of the box.
- **Standard Go 1.23 Iterators**: Seamless bidirectional conversion between push `Receiver[T]` streams and pull `iter.Seq2[T, error]` iterators.
- **Rich Operator Suite**: Transformations (`Map`, `FlatMap`, `Filter`, `Reduce`, `Batch`), Merging (`Merge`, `Zip`, `MergeSort`), Routing (`Broadcast`, `RoundRobin`, `Partition`), Control (`Throttle`, `Debounce`, `RetrySender`), and infallible mathematical wrappers (`math`).
- **Production-Ready Adapters**: Pre-built adapters for Go channels, JSON Lines (`io.Reader`/`io.Writer`), Cloud Object Storage (S3/GCS), HTTP Server-Sent Events (SSE), gRPC streams, and message brokers.

---

## Installation

```bash
go get github.com/stephen-mcelhose/go-streaming
```

Requires Go 1.23 or higher.

---

## Core Primitives

```go
// Receiver consumes items from a stream and passes them to handler.
type Receiver[T any] interface {
    Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error
}

// Sender publishes items into a stream or downstream sink.
type Sender[T any] interface {
    Send(ctx context.Context, item T) error
}

// Stream combines both Receiver and Sender contracts.
type Stream[T any] interface {
    Receiver[T]
    Sender[T]
}
```

Optional lifecycle closers (`ReceiverCloser[T]`, `SenderCloser[T]`, `StreamCloser[T]`) are provided for transports that require resource cleanup via `io.Closer`.

---

## Quick Start

### 1. Channel Streaming & Transformation Pipeline

```go
package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

func main() {
	ctx := context.Background()

	inCh := make(chan string, 3)
	inCh <- "alice"
	inCh <- "bob"
	inCh <- "charlie"
	close(inCh)

	outCh := make(chan string, 3)

	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	// Transform stream elements: map to uppercase
	go func() {
		defer close(outCh)
		_ = transformations.Map(ctx, func(ctx context.Context, s string) (string, error) {
			return strings.ToUpper(s), nil
		}, inStream, outStream)
	}()

	for res := range outCh {
		fmt.Println(res) // ALICE, BOB, CHARLIE
	}
}
```

### 2. Go 1.23 Iterator (`iter.Seq2`) Interoperability

Convert between push-based `Receiver[T]` streams and pull-based Go 1.23 iterators:

```go
// Push Receiver[T] -> Pull iter.Seq2[T, error]
seq := func(yield func(T, error) bool) {
    _ = receiver.Receive(ctx, func(ctx context.Context, item T) error {
        if !yield(item, nil) {
            return context.Canceled
        }
        return nil
    })
}

for item, err := range seq {
    if err != nil {
        break
    }
    fmt.Println(item)
}
```

---

## Architecture & Operators

```
go-streaming/
├── stream.go                     # Receiver[T], Sender[T], Stream[T], Closers
├── sentinels.go                  # ChanStream, Noop, and EOF sentinels
├── operators/
│   ├── transformations/          # Map, FlatMap, Filter, Reduce, Batch
│   ├── merging/                  # Merge (concurrent), Zip, MergeSort (heap-based)
│   ├── routing/                  # Broadcast, RoundRobin, Partition
│   ├── control/                  # Throttle, Debounce, RetrySender
│   └── math/                     # Panic-on-error wrappers for pure math funcs
└── adapters/
    ├── jsonl/                    # JsonlStream (io.Reader / io.Writer)
    ├── objectstorage/            # Cloud Object Storage JSONL connector
    ├── http/                     # Server-Sent Events (SSE) Streamer & Receiver
    ├── grpc/                     # gRPC Client & Server Stream adapters
    └── messaging/                # Kafka / Cloud PubSub BrokerStream
```

### Operator Overview

| Category | Operator | Description |
|---|---|---|
| **Transform** | `Map[I, O]` | Transforms elements using `func(ctx, I) (O, error)`. |
| | `FlatMap[I, O]` | Maps element to slice and emits individually. |
| | `Filter[T]` | Filters stream based on predicate. |
| | `Reduce[T]` | Aggregates elements into accumulator. |
| | `Batch[T]` | Emits slices based on max batch size or flush timeout. |
| **Merge** | `Merge[T]` | Concurrently merges multiple receivers into one sender. |
| | `Zip[A, B, O]` | Pairs items 1:1 from two streams. |
| | `MergeSort[T]` | Merges pre-sorted partitions via priority queue heap. |
| **Routing** | `Broadcast[T]` | Concurrently replicates elements to multiple senders. |
| | `RoundRobin[T]` | Cyclically distributes elements across senders. |
| | `Partition[T, K]`| Key-based partitioning to route-specific senders. |
| **Control** | `Throttle[T]` | Limits stream throughput to 1 item per duration. |
| | `Debounce[T]` | Emits item only after quiet duration passes. |
| | `RetrySender[T]` | Retries failed sends with backoff. |
| **Math** | `Map`, `Filter`, `Reduce` | Ergonomic pure math wrappers that panic on underlying stream errors. |

---

## Built-In Adapters

### JSON Lines (`adapters/jsonl`)
Stream line-delimited JSON objects over arbitrary `io.Reader` and `io.Writer`:
```go
stream := jsonl.NewJsonlStream[MyEvent](reader, writer)
_ = stream.Receive(ctx, func(ctx context.Context, ev MyEvent) error {
    return nil
})
```

### HTTP Server-Sent Events (`adapters/http`)
Serve real-time events over HTTP:
```go
http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
    _ = httpstream.StreamToHTTP(r.Context(), w, sourceReceiver)
})
```

---

## Running Tests

```bash
go test -v ./...
```

---

## License

This project is licensed under the [MIT License](LICENSE).
