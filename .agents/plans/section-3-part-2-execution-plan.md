# Section 3 (Part 2) Refactor & Enhancement Plan

## 1. Design & Architectural Invariants

### 1.1 `operators/routing/`
- **File Organization**:
  - `doc.go`
  - `broadcast.go` & `broadcast_test.go`
  - `round_robin.go` & `round_robin_test.go`
  - `partition.go` & `partition_test.go`
  - `example_test.go`
- **Partition Refinement**:
  - `Partition[T any, K comparable](ctx, keyFn, routes, defaultRoute, in)`:
    - If `defaultRoute == nil` and key is missing, returns an explicit unrouted item error (fail-fast).
    - If `defaultRoute != nil`, routes unmapped keys to `defaultRoute` (e.g. DLQ, unclassified sink).
- **Broadcast Strategies**:
  - `Broadcast[T](ctx, in, targets...) error`: FailOnError (returns first error encountered after all targets finish).
  - `BroadcastBestEffort[T](ctx, in, targets...) error`: Fire-and-forget; ignores individual target delivery errors and guarantees every target gets an attempt.

### 1.2 `operators/control/`
- **File Organization**:
  - `doc.go`
  - `throttle.go` & `throttle_test.go`
  - `debounce.go` & `debounce_test.go`
  - `retry.go` & `retry_test.go`
  - `example_test.go`
- **Throttle**: Document fixed-interval ticker behavior vs token-bucket alternatives.
- **RetrySender Options**:
  - `RetryPolicy`: MaxAttempts, InitialBackoff, MaxBackoff, BackoffMultiplier (exponential), Optional fallback `FallbackSender Sender[T]` (e.g. Dead Letter Queue stream).
  - Clean error isolation and context-aware sleep.

### 1.3 `operators/math/`
- **File Organization & Philosophy**:
  - Pure, zero-error ergonomics for mathematical pipelines ($f(x)$, stats, signal processing).
  - Split into `doc.go`, `map.go`, `filter.go`, `reduce.go`, `scan.go`, and `example_test.go`.
  - Update `Reduce[I, O]` and `Scan[I, O]` to match generic state accumulator signatures.
  - Document the value proposition clearly in `doc.go` (why mathers use this package).

### 1.4 Story-Driven Examples
- **`routing`**: Payment routing by currency/region with fallback to unclassified/DLQ (`Partition`), multi-destination telemetry replication (`Broadcast`).
- **`control`**: Rate-limiting external webhook deliveries (`Throttle`), UI search box keystroke burst consolidation (`Debounce`), payment gateway retry with DLQ stream fallback (`RetrySender`).
- **`math`**: Numerical vector calculation, Euclidean distance calculation, moving statistical average (`Scan`).
