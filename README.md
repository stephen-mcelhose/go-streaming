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
  - **Control**: `Throttle`, `Debounce`, `RetrySender` (with backoff & DLQ fallback)
  - **Pure Functional**: `pure` (Map, Filter, Reduce, Scan without error tuples)
  - **Math & Statistics**: `math` (Sum, RunningSum, Welford Stats, EMA supporting Real & Complex numbers)
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

## Persona Cookbook: How to Use `go-streaming`

### 1. For Software Engineers: Resilient Async Task Dispatcher
```go
// Ingest signup commands, map to email templates, and dispatch with exponential retry + DLQ
_ = transformations.Map(ctx, func(ctx context.Context, cmd SignupCmd) (EmailJob, error) {
    return EmailJob{Recipient: cmd.Email, Subject: "Welcome!"}, nil
}, cmdReceiver, emailJobSender)

retrySender := control.NewRetrySender(emailClientStream, control.RetryPolicy[EmailJob]{
    MaxAttempts:    3,
    InitialBackoff: 10 * time.Millisecond,
    Multiplier:     2.0,
    FallbackSender: dlqSender, // Auto-route persistent failures to Dead Letter Queue
})
```

### 2. For Data Engineers: ETL Micro-Batching to Object Storage
```go
// Ingest JSONL stream, clean/filter records, micro-batch by 500 items or 100ms, write to cloud store
jsonlSource := jsonl.NewJsonlStream[RawEvent](reader, nil)

_ = aggregation.Batch(ctx, aggregation.BatchConfig{
    MaxSize: 500,
    MaxWait: 100 * time.Millisecond,
}, cleanedEventsReceiver, batchSender)

stageWriter := jsonl.NewJsonlStream[CleanEvent](nil, objectStorageWriter)
```

### 3. For Network Engineers: Traffic Partitioning & Flood Throttling
```go
// Partition network packets by protocol (TCP vs UDP) and throttle security pager alerts
routes := map[string]streaming.Sender[Packet]{
    "TCP": tcpMonitoringSender,
    "UDP": udpMonitoringSender,
}
_ = routing.Partition(ctx, func(p Packet) (string, error) { return p.Protocol, nil }, routes, defaultSink, packetReceiver)

// Throttle critical alerts to at most 1 incident page per 100ms during DDoS
_ = control.Throttle(ctx, 100*time.Millisecond, alertReceiver, pagerSender)
```

### 4. For Mathematicians & Signal Processing Engineers: Pure Vector Calculations
```go
// Process complex signal phase angles without callback error-wrapping boilerplate
_ = pure.Map(ctx, func(z complex128) float64 {
    return cmplx.Phase(z)
}, complexSignalReceiver, phaseSender)

// Exact vector summation over complex stream
_ = mathops.Sum(ctx, complexVectorReceiver, sumSender)
```

### 5. For Data Scientists: Online Stream Statistics & Anomaly Detection
```go
// Real-time Exponential Moving Average (EMA) and online Welford summary statistics
_ = mathops.EMA(ctx, 0.2, latencyReceiver, emaSender)
_ = mathops.Stats(ctx, sampleReceiver, statsSender) // Emits Count, Mean, Variance, StdDev, Min, Max
```

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
| **Pure** | `Map`, `Filter`, `Reduce`, `Scan` | Pure functional wrappers accepting `func(I) O` without error clutter. |
| **Math** | `Sum`, `RunningSum`, `Stats`, `EMA` | Dedicated numeric/statistical stream processing (Real & Complex). |

---

## Running Tests

```bash
go test -v ./...
```

---

## License

This project is licensed under the [MIT License](LICENSE).
