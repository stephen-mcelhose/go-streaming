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
- **Rich Operator Suite**: Transformations (`Map`, `FlatMap`, `Filter`, `Reduce`, `Scan`), Aggregations (`Buffer`, `Batch`, `TumblingWindow`), Merging (`Merge`, `Concat`, `Interleave`, `Zip`, `MergeSort`), Routing (`Broadcast`, `BroadcastBestEffort`, `RoundRobin`, `Partition`), Control (`Throttle`, `Debounce`, `RetrySender`), and pure functional wrappers (`pure`).
- **Production-Ready Adapters**: Pre-built adapters for Go channels, Google Cloud Pub/Sub, JSON Lines (`io.Reader`/`io.Writer`), Cloud Object Storage (S3/GCS), HTTP Server-Sent Events (SSE), gRPC streams, and message brokers.

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

Optional lifecycle closers (`ReceiverCloser[T]`, `SenderCloser[T]`, `StreamCloser[T]`, `ChanStreamCloser[T]`) are provided for transports that require resource cleanup via `io.Closer`.

---

## Architecture & Operators

```
go-streaming/
├── stream.go                     # Receiver[T], Sender[T], Stream[T], Closers
├── sentinels.go                  # ChanStream, Noop, and EOF sentinels
├── errors.go                     # ErrStreamStop, IsCleanTermination
├── operators/
│   ├── transformations/          # Map, FlatMap, Filter, Reduce, Scan
│   ├── aggregation/              # Buffer (count), Batch (hybrid), TumblingWindow (time)
│   ├── merging/                  # Merge (concurrent), Concat (serial), Interleave, Zip, MergeSort
│   ├── routing/                  # Broadcast, BroadcastBestEffort, RoundRobin, Partition
│   ├── control/                  # Throttle, Debounce, RetrySender (with fallback)
│   └── pure/                     # Zero-boilerplate pure functional wrappers (func(I) O)
└── adapters/
    ├── gpubsub/                  # Google Cloud Pub/Sub with automatic ACK/NACK management
    ├── jsonl/                    # JsonlStream (io.Reader / io.Writer)
    ├── objectstorage/            # Cloud Object Storage JSONL connector
    ├── http/                     # Server-Sent Events (SSE) Streamer & Receiver
    ├── grpc/                     # gRPC Client & Server Stream adapters
    └── messaging/                # Kafka / Generic BrokerStream
```

### Operator Overview

| Category | Operator | Description |
|---|---|---|
| **Transform** | `Map[I, O]` | Transforms elements 1:1 using `func(ctx, I) (O, error)`. |
| | `FlatMap[I, O]` | Expands 1 input element into a sub-stream `Receiver[O]`. |
| | `Filter[T]` | Filters stream based on predicate `func(ctx, T) (bool, error)`. |
| | `Reduce[I, O]` | Accumulates bounded stream into terminal state object `O`. |
| | `Scan[I, O]` | Continuously emits running accumulated state `O` after every item. |
| **Aggregation** | `Buffer[T]` | Gathers items into count-based chunks. |
| | `Batch[T]` | Hybrid micro-batching triggered by item count or max duration. |
| | `TumblingWindow[T]`| Time-windowed grouping of stream elements. |
| **Merge** | `Merge[T]` | Concurrently merges multiple receivers into one sender. |
| | `Concat[T]` | Serially exhausts streams one after another ($R_1 \to R_2 \to \dots$). |
| | `Interleave[T]` | Fair round-robin turn-taking across streams ($A_1, B_1, A_2, \dots$). |
| | `Zip[A, B, O]` | Pairs items 1:1 from two streams. |
| | `MergeSort[T]` | Merges pre-sorted partitions via priority queue heap. |
| **Routing** | `Broadcast[T]` | Concurrently replicates elements to multiple senders (fail-fast). |
| | `BroadcastBestEffort[T]` | Fire-and-forget fan-out ignoring target delivery failures. |
| | `RoundRobin[T]` | Cyclically distributes elements across senders. |
| | `Partition[T, K]`| Key-based partitioning with optional default/DLQ fallback route. |
| **Control** | `Throttle[T]` | Limits stream throughput to 1 item per duration. |
| | `Debounce[T]` | Emits item only after quiet duration passes. |
| | `RetrySender[T]` | Retries failed sends with backoff and optional DLQ fallback. |
| **Pure** | `Map`, `Filter`, `Reduce`, `Scan` | Pure functional wrappers accepting `func(I) O` without error clutter. |

---

## Running Tests

```bash
go test -v ./...
```

---

## License

This project is licensed under the [MIT License](LICENSE).
