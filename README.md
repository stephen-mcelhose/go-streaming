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

### 1. For Software Engineers: Resilient Async Task Dispatcher with DLQ
```go
// Ingest signup commands, map to work orders, and dispatch with exponential retry + Dead Letter Queue
_ = transformations.Map(ctx, func(ctx context.Context, cmd UserSignupCommand) (ProvisionJob, error) {
    return ProvisionJob{JobID: "job_" + cmd.UserID, Recipient: cmd.Email, Tier: cmd.Tier}, nil
}, inStream, outStream)

retryPolicy := control.RetryPolicy[ProvisionJob]{
    MaxAttempts:    3,
    InitialBackoff: 5 * time.Millisecond,
    Multiplier:     2.0,
    FallbackSender: streaming.NewChanStream(nil, dlqSink), // Automatic fallback to Dead Letter Queue
}
resilientSender := control.NewRetrySender[ProvisionJob](serviceBackend, retryPolicy)
```

### 2. For Data Engineers: Lakehouse ETL Micro-Batching & Multi-Partition Sorted Backfills
```go
// Ingest JSONL stream, clean URLs into normalized paths, and shard by tenant
_ = routing.Partition(ctx,
    func(ev CleanClickEvent) (string, error) { return ev.TenantID, nil },
    routes, dlqSink, cleanedStream,
)

// Micro-batch tenant records (by count or timeout) for high-throughput cloud storage writes
_ = aggregation.Batch(ctx, aggregation.BatchConfig{
    MaxSize: 500,
    MaxWait: 100 * time.Millisecond,
}, tenantStream, batchedSink)

// Globally merge pre-sorted historical partition streams via min-heap
_ = merging.MergeSort(ctx,
    func(a, b CleanClickEvent) int { return int(a.Timestamp - b.Timestamp) },
    sortedSink, partition1, partition2,
)
```

### 3. For Network Engineers: Traffic Demuxing, Telemetry Fan-Out, and Alert Flood Throttling
```go
// Partition captured packets by transport protocol
_ = routing.Partition(ctx,
    func(p NetworkPacket) (string, error) { return p.Protocol, nil },
    map[string]streaming.Sender[NetworkPacket]{"TCP": tcpSink, "UDP": udpSink},
    nil, rawPacketStream,
)

// Non-blocking fire-and-forget replication to SIEM analytics and real-time IDS
_ = routing.BroadcastBestEffort(ctx, alertStream, siemSink, idsSink)

// Throttle burst alerts to prevent incident responder pager fatigue during DDoS storms
_ = control.Throttle(ctx, 100*time.Millisecond, burstAlertsStream, pagerSink)
```

### 4. For Mathematicians & DSP Engineers: Complex Signal Demodulation & Energy Integration
```go
// Demodulate complex IQ baseband samples (complex128) into polar amplitude & phase without error boilerplate
_ = pure.Map(ctx, func(z complex128) RawPolar {
    return RawPolar{Magnitude: cmplx.Abs(z), Phase: cmplx.Phase(z), Power: cmplx.Abs(z) * cmplx.Abs(z)}
}, iqStream, polarStream)

// Instantaneous FM/PM frequency discrimination (Delta Phase) via stateful pure.Scan
_ = pure.Scan(ctx, phaseState{}, func(st phaseState, raw RawPolar) phaseState {
    delta := raw.Phase - st.prevPhase // with phase unwrap correction
    st.prevPhase = raw.Phase
    st.deltaPhase = delta
    return st
}, polarStream, discriminatedStream)

// Total integrated pulse energy accumulation
_ = mathops.Sum(ctx, powerStream, totalEnergySink)
```

### 5. For Data Scientists: Real-Time Single-Pass Feature Engineering & Anomaly Detection
```go
// Single-pass running moments (Welford's Algorithm) + Exponential Moving Average (EMA)
_ = pure.Scan(ctx, onlineStatsState{alpha: 0.3}, func(acc onlineStatsState, tick MetricTick) onlineStatsState {
    // Online EMA update: EMA_t = alpha * X_t + (1 - alpha) * EMA_{t-1}
    acc.ema = acc.alpha*tick.Value + (1.0-acc.alpha)*acc.ema
    
    // Online Welford update: Mean and M2 (Variance)
    delta := tick.Value - acc.mean
    acc.mean += delta / float64(acc.count)
    acc.m2 += delta * (tick.Value - acc.mean)
    
    // Dynamic real-time Z-score anomaly scoring
    zScore := (tick.Value - priorMean) / priorStdDev
    acc.isAnomaly = zScore > 2.5
    return acc
}, rawTicksStream, scoredFeatureStream)
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
