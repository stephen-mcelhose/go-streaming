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
- **Rich Operator Suite**: 
  - **Transformations**: `Map`, `FlatMap`, `Filter`, `Reduce`, `Scan`
  - **Aggregations**: `Buffer` (count), `Batch` (hybrid count+time), `TumblingWindow` (time)
  - **Merging**: `Merge` (concurrent fan-in), `Concat` (sequential), `Interleave` (fair round-robin), `Zip`, `MergeSort` (heap)
  - **Routing**: `Broadcast` (fail-fast), `BroadcastBestEffort`, `RoundRobin`, `Partition` (with default/DLQ fallback)
  - **Control**: `Throttle`, `Debounce`, `RetrySender` (with backoff & DLQ fallback), `CUSUM` (change-point detection)
  - **Pure Functional**: `pure` (Map, Filter, Reduce, Scan without error tuples)
  - **Math & Statistics**: `math` (`Welford`, `BivariateWelford`, `Covariance`, `PearsonCorrelation`, `Moments` with Skewness & Kurtosis, `Sum`, `RunningSum`, `Stats`, `EMA` supporting Real & Complex numbers)
- **Production-Ready Adapters**: Google Cloud Pub/Sub, JSON Lines (`io.Reader`/`io.Writer`), Cloud Object Storage (S3/GCS), HTTP Server-Sent Events (SSE), gRPC streams, Go channels, and message brokers.

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

## Operator Matrix

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
| | `CUSUM[T]` | Page's Cumulative Sum control chart for early detection of mean shifts. |
| **Pure** | `Map`, `Filter`, `Reduce`, `Scan` | Pure functional wrappers accepting `func(I) O` without error clutter. |
| **Math & Moments** | `Sum`, `RunningSum` | Total and cumulative summations (Real & Complex). |
| | `Stats`, `RunningStats` | Single-pass Welford summary moments (Mean, Variance, StdDev, Min, Max). |
| | `Covariance`, `PearsonCorrelation` | Online bivariate Welford covariance and correlation ($r \in [-1, 1]$). |
| | `MomentsStats`, `RunningMoments` | Higher-order shape statistics (Skewness and Kurtosis via Pébay recurrence). |
| | `EMA` | Exponential Moving Average with configurable smoothing weight $\alpha$. |

---

## Running Tests

```bash
go test -v ./...
```

---

## License

This project is licensed under the [MIT License](LICENSE).
