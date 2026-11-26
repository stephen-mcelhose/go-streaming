# Section 3 Review & Refinement Plan (Part 1: Transformations, Aggregation, Merging)

## 1. Design Discussions & Decisions

### 1.1 Map vs. FlatMap vs. Filter Mental Model
- **`Map[I, O]`**: 1 item in -> exactly 1 item out (`I -> O`).
- **`FlatMap[I, O]`**: 1 item in -> $0..N$ items out (or a sub-stream). 
  - Why current `FlatMap` takes `(streaming.Receiver[O], error)`: In streaming, mapping an incoming item (e.g. a batch payload or a directory path) produces an independent sub-stream of items.
  - Why Map feels like FlatMap in Go: because both push to an `out streaming.Sender[O]`. The distinction is cardinality: `Map` enforces strict 1:1 mapping per input, whereas `FlatMap` expands/flattens 1 input into an arbitrary stream of outputs.
- **`Filter[T]`**: 1 item in -> 0 or 1 item out based on predicate.

### 1.2 Reduce vs. Scan (Bounded vs. Unbounded Accumulation)
- **`Reduce[I any, O any]`**: For **bounded** streams. Consumes entire stream to completion and emits a **single terminal accumulated state `O`**.
- **`Scan[I any, O any]`**: For **unbounded** streams. Emits running accumulated state `O` after **every** incoming item `I` (e.g., running total, current session state).
- **Generic Expansion**: Update signatures to `Reduce[I any, O any]` and `Scan[I any, O any]` to support reducing items of type `I` into state objects of type `O`.

### 1.3 Aggregation & Windowing Naming Standards (Flink / Rx / Kafka Streams)
- **Buffer vs. Batch vs. Window**:
  - `Buffer[T](ctx, count, in, out)`: Count-based chunking.
  - `Window[T](ctx, duration, in, out)` (or `TumblingWindow`): Fixed non-overlapping time intervals.
  - `Batch[T](ctx, BatchConfig{MaxSize, MaxWait}, in, out)`: Hybrid count + timeout batching (standard broker micro-batching pattern).

### 1.4 Merge, Concat, and Interleave Mechanics
- **`Merge`**: Concurrent non-deterministic fan-in. Multiple goroutines push into single sender as items arrive.
  - *"Cancels remaining receivers"*: If Receiver 1 hits a fatal unrecoverable DB error, `Merge` cancels the child `context.Context` so Receivers 2 & 3 don't leak background worker goroutines.
- **`Concat` on Unbounded Streams**: Concat is strictly sequential ($R_1 \to R_2 \to \dots$). If $R_1$ is unbounded (e.g. Pub/Sub), $R_2$ will never start. We will document this requirement and verify clean termination on bounded streams.
- **`Interleave` vs. `Merge`**:
  - `Merge`: Non-deterministic race. Fast producer can starve slow producer.
  - `Interleave`: Deterministic round-robin fair turn-taking ($A_1, B_1, A_2, B_2, \dots$). If $A$ produces 1,000 items and $B$ produces 1 item, `Interleave` takes 1 from $A$, 1 from $B$, then 1 from $A$, etc.

### 1.5 Story-Driven Examples Strategy
Update examples across packages to tell real-world end-to-end domain stories:
1. `transformations`: Ingesting raw JSON audit logs, mapping/enriching with tenant metadata, filtering unauthorized events, and reducing/scanning into session metrics.
2. `aggregation`: Micro-batching high-throughput telemetry sensor events for database bulk-insert.
3. `merging`: Merging live order updates from webhooks and batch backfills into a unified order pipeline.

---

## 2. Execution Tasks

- [ ] **Task 3.1: Transformations Update**:
  - Expand `Reduce[I any, O any]` with initial state `O` and reducer `func(ctx, O, I) (O, error)`.
  - Add `Scan[I any, O any]` for running unbounded state emission.
  - Rewrite `operators/transformations/doc.go` with comprehensive package docs and architectural overview.
  - Rewrite `operators/transformations/example_test.go` with narrative, story-driven examples.
- [ ] **Task 3.2: Aggregation Update**:
  - Clarify and standardize `Buffer` (count), `TumblingWindow` (time), and `Batch` (hybrid count+time).
  - Rewrite `operators/aggregation/doc.go` and add story-driven micro-batch database ingestion example in `example_test.go`.
- [ ] **Task 3.3: Merging & Concat Refactor**:
  - Add domain-specific internal type names in `interleave.go` and `mergesort.go` for readability.
  - Clarify documentation on `Concat` regarding bounded streams and `io.EOF` requirement.
  - Rewrite `operators/merging/doc.go` and add story-driven examples.
- [ ] **Task 3.4 (Deferred / Next Subtask)**:
  - Plan and review `operators/routing/`, `operators/control/`, and `operators/math/`.
