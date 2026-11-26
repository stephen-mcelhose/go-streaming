# ADR 002: Stream Termination, Sentinel Semantics, and Bounded vs Unbounded Streams

## Context and Problem Statement
In streaming systems, streams can be either **bounded** (finite files, batch slices, fixed-size database query results) or **unbounded** (continuous Pub/Sub subscriptions, Kafka consumer groups, live ticker feeds).

When consumers or producers stop processing, we must distinguish between:
1. **Fatal failures**: network drops, deserialization faults, handler panics.
2. **Context cancellations**: timeouts, deadline exceeded, process SIGTERM (`ctx.Done()`).
3. **Normal stream exhaustion (Bounded)**: reaching the end of a finite stream (`io.EOF` / clean return).
4. **Consumer-initiated graceful early stop**: e.g., `Take(10)`, iterator `break`, early target match.

## Reference Implementations & Prior Art

### 1. Standard Go `io.Reader` and `database/sql.Rows`
- `io.Reader` returns `(n int, err error)` where `err == io.EOF` signals clean termination.
- `sql.Rows` terminates iteration when `Next() == false`, followed by `Err()` to distinguish clean exhaustion from query failure.

### 2. Reactive Streams / Project Reactor / RxJava
- Tri-signal protocol: `onNext(item)`, `onError(throwable)`, `onComplete()`.
- An unbounded stream simply never calls `onComplete()`.
- A downstream subscriber calls `Subscription.cancel()` to stop early without signaling an error.

### 3. Apache Beam / Apache Flink
- Explicit separation of `PCollection.isBounded()` into `BOUNDED` vs `UNBOUNDED`.
- Bounded streams trigger watermark finalization and emit a terminal checkpoint; unbounded streams run continuously until external cancellation.

### 4. Alternative Considered: `(error, isClean)` Signature
- Changing handler or receive return signatures to `(err error, isClean bool)` was considered.
- Rejected for now: adds cognitive overhead and allocation/signature churn compared to idiomatic Go sentinel errors (`errors.Is`).

## Decision Outcome (Y-Statement)
In the context of designing stream lifecycle and termination in `go-streaming`,
facing the need to handle both bounded and unbounded streams gracefully,
we decided to **adopt sentinel-based clean termination (`io.EOF` and `streaming.ErrStreamStop`) paired with `streaming.IsCleanTermination(err)` helper**
and neglected **custom boolean status tuples `(err, isClean)` and explicit runtime stream type tags**,
to achieve **seamless integration with Go standard library `io.EOF` patterns, minimal interface pollution, and idiomatic error bubbling**,
accepting that **callers and operators must use `streaming.IsCleanTermination(err)` rather than raw `err == nil` checks when evaluating stream exit states**.

## Key Invariants
- `io.EOF`: Producer/source reached the end of a bounded stream.
- `ErrStreamStop`: Consumer/downstream intentionally requests an early graceful stop (e.g., iterator break, limit reached).
- `IsCleanTermination(err)`: Returns `true` for `nil`, `io.EOF`, and `ErrStreamStop`.
- Unbounded streams remain active indefinitely until `ctx` cancellation or a fatal error occurs.
