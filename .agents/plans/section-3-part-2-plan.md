# Section 3 Review & Refinement Plan (Part 2: Routing, Control, Math)

## 1. Scope & Topics

### 1.1 `operators/routing/` (`Broadcast`, `RoundRobin`, `Partition`)
- **Key Extractors & Error Safety**:
  - `Partition[T, K]`: Key extractor function `func(item T) (K, error)`.
  - Missing key behavior: Currently errors if key not in map. Review fallback default sender or dead-letter routing.
- **Concurrent Fan-Out**:
  - `Broadcast[T]`: Concurrently pushes to all targets. Error isolation vs fail-fast cancellation across targets.
- **Story-Driven Examples**:
  - `Partition`: Sharding payment events to region-specific processing queues by country code.
  - `Broadcast`: Replicating critical sensor alerts simultaneously to database logger and live incident notification webhook.
  - `RoundRobin`: Distributing load across worker pool senders.

### 1.2 `operators/control/` (`Throttle`, `Debounce`, `RetrySender`)
- **`Throttle[T]`**:
  - Ticker-based rate limiting per interval. Context cancellation verification.
- **`Debounce[T]`**:
  - Quiet window timer. Clean final flush on stream completion.
- **`RetrySender[T]`**:
  - Exponential vs constant backoff policies. Error classification (retryable vs fatal).
- **Dead-Letter Queue (DLQ)**:
  - Resilient sender wrapping with fallback to DLQ sender after retry exhaustion.
- **Story-Driven Examples**:
  - `Throttle`: Rate limiting external API webhook delivery to 5 requests per second.
  - `Debounce`: Search autocomplete keystroke stream debouncer.
  - `RetrySender` + DLQ: Retrying transient HTTP calls and routing persistent failures to DLQ.

### 1.3 `operators/math/`
- **Pure Math Invariants**:
  - Infallible wrappers `func(I) O` that panic on underlying stream transport failures.
  - Update `Reduce` in `math/` to support `[I any, O any]` consistent with `transformations.Reduce`.
  - Package documentation and story-driven numerical calculation examples (e.g. geometric distance, running statistical variance).

---

## 2. Review Checklist

- [ ] Inspect and review `operators/routing/`.
- [ ] Inspect and review `operators/control/`.
- [ ] Inspect and review `operators/math/`.
- [ ] Implement refinements, story-driven examples, and doc updates.
