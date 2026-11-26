# Refactor and Feature Expansion Plan

## Overview
This plan implements the architectural refinements from Human Review 1:
1. Formal Google Cloud Pub/Sub adapter with explicit ACK/NACK, error, and EOF lifecycle behavior.
2. Operator reorganization (`transformations/` split and new `aggregation/` package for `buffer`, `window`, and `batch`).
3. FlatMap refactor to Option A (`mapper func(ctx, item I) (Receiver[O], error)`).
4. Merge strategies expansion (`Merge`, `Concat`, `Interleave`, `MergeSort`) and clarification of `io.EOF` handling.
5. Standard Go runnable `Example*` functions with `// Output:` comments.

---

## Task Breakdown

### Task 1: Google Cloud Pub/Sub Adapter (`adapters/gpubsub`)
- **Specification (RFC 2119)**:
  - **MUST** map Pub/Sub `*pubsub.Message` payloads to typed generic items `T`.
  - **MUST** manage message lifecycle:
    - If the handler returns `nil`, the adapter **MUST** call `msg.Ack()`.
    - If the handler returns an error (other than context cancellation or EOF), the adapter **MUST** call `msg.Nack()` and **MUST NOT** terminate the overall receive loop.
    - If context is canceled (`ctx.Err() != nil`), the adapter **MUST** call `msg.Nack()` and return `ctx.Err()`.
    - If handler returns `io.EOF`, the adapter **MUST** call `msg.Ack()` on the current message and return `nil` to signal clean stream termination.
  - **MUST** provide `PubsubStream[T]` implementing `Receiver[T]` and `Sender[T]`.
  - **MUST** provide testable runnable `Example` showing setup, publish, receive, and ack/nack handling.

### Task 2: Operator Reorganization & FlatMap Update
- **`operators/transformations/`**:
  - `map.go` & `map_test.go`
  - `flat_map.go` & `flat_map_test.go` (Option A: sub-stream `streaming.Receiver[O]`)
  - `filter.go` & `filter_test.go`
  - `reduce.go` & `reduce_test.go`
- **`operators/aggregation/`**:
  - `buffer.go` & `buffer_test.go` (Count-based buffering)
  - `window.go` & `window_test.go` (Sliding and tumbling time windows)
  - `batch.go` & `batch_test.go` (Count-based, time-based, and hybrid count+time batching)

### Task 3: Merge Strategies Clarification & Expansion (`operators/merging/`)
- Clarify `io.EOF`: individual input stream exhaustion with `io.EOF` or clean return **MUST NOT** abort concurrent merges; only unexpected errors cancel.
- Implement:
  - `Merge`: Concurrent fan-in (non-deterministic arrival).
  - `Concat`: Serial exhaust-and-advance merge.
  - `Interleave`: Round-robin fair fan-in across receivers.
  - `MergeSort`: Sorted fan-in using comparator heap.

### Task 4: Standard Go `Example*` Functions
- Add runnable, documented `Example*` functions with `// Output:` comments in:
  - `examples/push_pull_example_test.go`
  - `adapters/gpubsub/example_test.go`
  - `operators/transformations/example_test.go`
  - `operators/merging/example_test.go`
  - `operators/aggregation/example_test.go`
