# go-streaming

A modular, composable streaming library in Go built on top of typed `Receiver` and `Sender` primitives.

---

## 1. Core Interfaces & Types

### 1.1 Base Contracts

```go
type Receiver[T any] interface {
	Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error
}

type Sender[T any] interface {
	Send(ctx context.Context, item T) error
}

type Stream[T any] interface {
	Receiver[T]
	Sender[T]
}

type ReceiverCloser[T any] interface {
	Receiver[T]
	io.Closer
}

type SenderCloser[T any] interface {
	Sender[T]
	io.Closer
}

type StreamCloser[T any] interface {
	Stream[T]
	io.Closer
}
```

> **Note on Push vs. Pull**: Core primitives use push semantics (`Receive(ctx, handler)`). Push-to-pull adapters (e.g. converting `Receiver[T]` to Go 1.23 `iter.Seq2[T, error]` or channels) will be provided as standard examples/utilities.
> **Note on Termination**: Explicit stream termination semantics (`io.EOF`, sentinels, drain hooks) are deferred to observe real caller requirements across adapter implementations.

---

## 2. In-Memory & Sentinel Adapters

```go
// Channel Adapter
type ChanStream[T any] struct {
	receiver <-chan T
	sender   chan<- T
}

func NewChanStream[T any](receiver <-chan T, sender chan<- T) *ChanStream[T]
func (s *ChanStream[T]) Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error
func (s *ChanStream[T]) Send(ctx context.Context, item T) error

// Sentinels & Stubs
type NoopSender[T any] struct{}
func NewNoopSender[T any]() *NoopSender[T]
func (s *NoopSender[T]) Send(ctx context.Context, item T) error { return nil }

type NoopReceiver[T any] struct{}
func NewNoopReceiver[T any]() *NoopReceiver[T]
func (s *NoopReceiver[T]) Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error { return nil }

type EOFSender[T any] struct{}
func NewEOFSender[T any]() *EOFSender[T]
func (s *EOFSender[T]) Send(ctx context.Context, item T) error { return io.EOF }

type EOFReceiver[T any] struct{}
func NewEOFReceiver[T any]() *EOFReceiver[T]
func (s *EOFReceiver[T]) Receive(ctx context.Context, handler func(ctx context.Context, item T) error) error { return io.EOF }
```

---

## 3. Operators Architecture

### 3.1 Operator Package Layout

```
operators/
├── transformations/ # Map, FlatMap, Filter, Reduce, Batch
├── routing/         # Broadcast, RoundRobin, Partition
├── merging/         # Merge, MergeSort, Zip
├── control/         # Throttle, Debounce, Retry, DeadLetterQueue
└── math/            # Pure/mather wrappers (panic-on-error, context-free & context-aware)
```

### 3.2 Dual Signature Convention

1. **Primitive Layer (`operators/*`)**: MUST be `context.Context`-aware and error-aware (`func(context.Context, I) (O, error)`).
2. **Mather Layer (`operators/math/*`)**: Provides ergonomic wrappers accepting pure functions `func(I) O`. Panics on error under the assumption that the caller's mathematical pipeline is infallible.

### 3.3 Operator Specifications

#### `operators/transformations/`
- **`Map[I, O]`**: `func(ctx context.Context, mapper func(context.Context, I) (O, error), in Receiver[I], out Sender[O]) error`
- **`FlatMap[I, O]`**: `func(ctx context.Context, mapper func(context.Context, I) ([]O, error), in Receiver[I], out Sender[O]) error`
- **`Filter[T]`**: `func(ctx context.Context, predicate func(context.Context, T) (bool, error), in Receiver[T], out Sender[T]) error`
- **`Reduce[T]`**: `func(ctx context.Context, reducer func(context.Context, acc T, item T) (T, error), in Receiver[T], out Sender[T]) error`
- **`Batch[T]`**: `func(ctx context.Context, size int, timeout time.Duration, in Receiver[T], out Sender[[]T]) error`

#### `operators/routing/`
- **`Broadcast[T]`**: Fans out each received item to all destination senders.
- **`RoundRobin[T]`**: Distributes items cyclically across multiple senders.
- **`Partition[T, K comparable]`**: Routes items to distinct senders based on a key extraction function.

#### `operators/merging/`
- **`Merge[T]`**: Combines multiple `Receiver[T]` inputs concurrently into a single `Sender[T]`.
- **`MergeSort[T]`**: Priority-queue/heap over buffered heads of sorted `Receiver[T]` partitions using a comparator `func(T, T) int`.
- **`Zip[A, B, O]`**: Pairs items from two streams 1:1 and emits via combinator function.

#### `operators/control/`
- **`Throttle[T]`**: Enforces rate limiting per time interval.
- **`Debounce[T]`**: Emits only after a quiet duration has elapsed without new items.
- **`Retry[T]`**: Wraps sender/receiver with backoff policy on transient failures.

#### `operators/math/`
- **`Map[I, O]`**: Wraps transformation with `func(I) O` (no context, no error).
- **`Filter[T]`**: Wraps filter with `func(T) bool`.
- **`Reduce[T]`**: Wraps reduce with `func(T, T) T`.

---

## 4. Adapters & Connectors

1. **`adapters/jsonl`**: `JsonlStream[T]` over `io.Reader` / `io.Writer`.
2. **`adapters/objectstorage`**: `JsonlObjectStorageStream[T]` wrapping storage streaming readers & writers.
3. **`adapters/grpc`**: gRPC streaming client/server connector.
4. **`adapters/http`**: HTTP SSE and NDJSON streaming connectors.
5. **`adapters/kafka`**: Kafka consumer/producer connector (cutline/interface).
6. **`adapters/pubsub`**: PubSub subscriber/publisher connector (cutline/interface).

---

## 5. Examples & Recipes

- **Push-to-Pull**: Converting `Receiver[T]` into Go 1.23 `iter.Seq2[T, error]` and Go channels.
- **Pull-to-Push**: Feeding channel/iterator sources into `Sender[T]`.
- **Emitters**: JSONL, HTTP SSE, batch processing.

---

## 6. Implementation Phasing

- **Phase 1: Core Domain & In-Memory**:
  - `Receiver`, `Sender`, `Stream`, `ReceiverCloser`, `SenderCloser`, `StreamCloser`.
  - `NoopSender`, `NoopReceiver`, `EOFSender`, `EOFReceiver`, `ChanStream`.
  - Push-to-pull / pull-to-push conversion examples.
- **Phase 2: Core Operators & Math Wrappers**:
  - `transformations/` (`Map`, `FlatMap`, `Filter`, `Reduce`, `Batch`).
  - `merging/` (`Merge`, `MergeSort`, `Zip`).
  - `math/` panic-on-error math wrappers.
- **Phase 3: File & Storage Adapters**:
  - `adapters/jsonl` (`JsonlStream`).
  - `adapters/objectstorage` (`JsonlObjectStorageStream`).
- **Phase 4: Network & Message Brokers**:
  - `adapters/http` (SSE / NDJSON).
  - `adapters/grpc`.
  - `adapters/kafka` & `adapters/pubsub` (Cutline / adapter interfaces).
- **Phase 5: Extended Operators**:
  - `routing/` (`Broadcast`, `RoundRobin`, `Partition`).
  - `control/` (`Throttle`, `Debounce`, `Retry`).
