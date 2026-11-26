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
  - **Control**: `Throttle`, `Debounce`, `RetrySender` (with backoff & DLQ fallback), `CUSUM` (Page's change-point detection)
  - **Pure Functional**: `pure` (Map, Filter, Reduce, Scan without error tuples)
  - **Math & Statistics**: `math` (`Welford`, `BivariateWelford`, `Covariance`, `PearsonCorrelation`, `Moments` with Skewness & Kurtosis, `StreamingFFT` with Cooley-Tukey Radix-2, `Sum`, `RunningSum`, `Stats`, `EMA` supporting Real & Complex numbers)
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

| Category | Operator | Description | Citations / References |
|---|---|---|---|
| **Transform** | `Map[I, O]` | Transforms elements 1:1 using `func(ctx, I) (O, error)`. | |
| | `FlatMap[I, O]` | Expands 1 input element into a sub-stream `Receiver[O]`. | |
| | `Filter[T]` | Filters stream based on predicate `func(ctx, T) (bool, error)`. | |
| | `Reduce[I, O]` | Accumulates bounded stream into terminal state object `O`. | |
| | `Scan[I, O]` | Continuously emits running accumulated state `O` after every item. | |
| **Aggregation** | `Buffer[T]` | Gathers items into count-based chunks. | |
| | `Batch[T]` | Hybrid micro-batching triggered by item count or max duration. | |
| | `TumblingWindow[T]`| Time-windowed grouping of stream elements. | |
| **Merge** | `Merge[T]` | Concurrently merges multiple receivers into one sender. | |
| | `Concat[T]` | Serially exhausts streams one after another ($R_1 \to R_2 \to \dots$). | |
| | `Interleave[T]` | Fair round-robin turn-taking across streams ($A_1, B_1, A_2, \dots$). | |
| | `Zip[A, B, O]` | Pairs items 1:1 from two streams. | |
| | `MergeSort[T]` | Merges pre-sorted partitions via priority queue heap. | |
| **Routing** | `Broadcast[T]` | Concurrently replicates elements to multiple senders (fail-fast). | |
| | `BroadcastBestEffort[T]` | Fire-and-forget fan-out ignoring target delivery failures. | |
| | `RoundRobin[T]` | Cyclically distributes elements across senders. | |
| | `Partition[T, K]`| Key-based partitioning with optional default/DLQ fallback route. | |
| **Control** | `Throttle[T]` | Limits stream throughput to 1 item per duration. | |
| | `Debounce[T]` | Emits item only after quiet duration passes. | |
| | `RetrySender[T]` | Retries failed sends with backoff and optional DLQ fallback. | |
| | `CUSUM[T]` | Page's Cumulative Sum control chart for early detection of mean shifts. | [Page 1954](https://doi.org/10.1093/biomet/41.1-2.100), [Page 1961](https://www.jstor.org/stable/1266470) |
| **Pure** | `Map`, `Filter`, `Reduce`, `Scan` | Pure functional wrappers accepting `func(I) O` without error clutter. | |
| **Math & Frequency** | `Sum`, `RunningSum` | Total and cumulative summations (Real & Complex). | |
| | `Stats`, `RunningStats` | Single-pass Welford summary moments (Mean, Variance, StdDev, Min, Max). | [Welford 1962](https://doi.org/10.1080/00401706.1962.10490022), [Knuth TAOCP Vol 2](https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_algorithm) |
| | `Covariance`, `PearsonCorrelation` | Online bivariate Welford covariance and correlation ($r \in [-1, 1]$). | [Knuth 1997](https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Covariance) |
| | `MomentsStats`, `RunningMoments` | Higher-order shape statistics (Skewness and Kurtosis via Pébay recurrence). | [Pébay 2008](https://prod-ng.sandia.gov/techlib-noauth/access-control.cgi/2008/086212.pdf) |
| | `StreamingFFT` | Radix-2 Decimation-in-Time Fast Fourier Transform over window chunks. | [Cooley & Tukey 1965](https://doi.org/10.1090/S0025-5718-1965-0178586-1) |
| | `EMA` | Exponential Moving Average with configurable smoothing weight $\alpha$. | |

---

## Advanced Domain Recipes (`examples/`)

- **[Mean First Passage Time (MFPT / Barrier Renewal)](examples/mfpt_example_test.go)**: Measures average hitting times and burst renewal cycles ([Redner 2001](https://doi.org/10.1017/CBO9780511606014), [Condamin et al. Nature 2007](https://doi.org/10.1038/nature06201)).
- **[Goertzel Single-Tone Frequency Detector](examples/dsp_goertzel_example_test.go)**: 2nd-order IIR filter for $\mathcal{O}(1)$ single-frequency detection ([Goertzel 1958](https://doi.org/10.2307/2310304)).
- **[Declarative LaTeX Formula Execution (`goexeclatex`)](examples/latex_eval_example_test.go)**: Evaluates dynamic recurrence relations ($S_t = a x + (1-a)s$) and window summations ($\sum_{i=1}^N x_i$) over streams.

---

## Running Tests

```bash
go test -v ./...
```

---

## License

This project is licensed under the [MIT License](LICENSE).
