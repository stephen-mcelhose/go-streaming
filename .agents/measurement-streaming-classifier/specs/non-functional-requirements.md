# Non-Functional Requirements (NFRs): Inverted Prefix-Trie & Repartitioned Streaming Graph

## 1. Latency & Throughput Budgets

| Operation | Scale Tier | Latency Target ($p_{99}$) | Complexity | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Ingest (`Put`)** | Up to $10^7$ nodes | $< 5.0\ \mu\text{s}$ | $O(\text{depth})$ | Sub-microsecond hot path in L1; avoids memory allocations after trie paths warm. |
| **Lookup (`Get`)** | Up to $10^7$ nodes | $< 2.0\ \mu\text{s}$ | $O(\text{depth})$ | Direct segment traversal down $6\text{--}8$ tree pointers. |
| **Subtree Invalidation (`InvalidateEntity`)** | 1 to $10^5$ nodes / entity | $< 1.0\ \mu\text{s}$ | $O(1)$ | Single pointer detachment at the entity node. Leaf GC deferred to Go runtime. |
| **Metric Invalidation (`InvalidatePrimary` / `Class` / `Instance`)** | 1 to $10^6$ nodes / metric | $< 1.5\ \mu\text{s}$ | $O(1)$ on Projected Trie | Immediate root detachment on the repartitioned metric index. |
| **Calculation Trigger Cascade** | Depth $\le 10$ | $< 25.0\ \mu\text{s}$ | $O(D \cdot \text{slots})$ | Synchronous cascade through reactive formula chain without I/O blocking. |

---

## 2. Concurrency & Contention Bounds

1. **Disjoint Parallelism ($99.6\%$ Contention-Free Writes):**
   - With $N = 256$ lock shards, two randomly selected active entities have a collision probability of $\frac{1}{256} \approx 0.39\%$.
   - Goroutines streaming to distinct entities MUST run in parallel with near-linear core scaling.
2. **Lock Hold Time:**
   - Critical section duration under entity/shard write lock MUST NOT exceed $5\ \mu\text{s}$.
   - No I/O, network calls, serialization, or unbounded loops inside shard locks.
3. **Deadlock Freedom (Strict Lock Hierarchy):**
   - `metaMu` (Root) MUST always be acquired *before* any entity shard lock.
   - Dual-index operations MUST acquire entity locks before primary index locks in deterministic order.

---

## 3. Memory Footprint & Allocation Invariants

1. **Zero-Allocation Hot Reads:**
   - Point lookups (`Get`) on existing coordinates MUST generate 0 heap allocations (`0 B/op`).
2. **Deterministic Mutex Overhead:**
   - Sharded lock ring uses fixed array of 256 `sync.RWMutex` ($\approx 6\ \text{KB}$ total memory overhead), independent of whether $10^2$ or $10^8$ entities are actively tracked.
3. **Memory Density & Compaction:**
   - Stored node pointer overhead $\le 64\ \text{bytes}$ per trie branch node.
   - Empty pruned branches MUST be eligible for GC immediately upon entity detachment without memory leaks.

---

## 4. Failure Modes, Isolation & Graceful Degradation

1. **Entity-Level Blast Radius Isolation:**
   - A malformed observation, calculation panic, or single entity retraction MUST NEVER corrupt or block sibling entity subtrees.
2. **Two-Tier Cache Resilience (`TrieCache`):**
   - If L2 persistent store (e.g. RelateDB / external DB) fails or times out:
     - Error MUST propagate cleanly to the caller without corrupting L1 hot memory.
     - L1 cache MUST continue serving active reads.
3. **Context Cancellation & Timeout Propagation:**
   - All public operations taking `context.Context` MUST immediately abort and return `ctx.Err()` when context is canceled.

---

## 5. Observability & Operability

1. **Lock-Free Global Metrics:**
   - Total placed measurement count MUST be queryable in $O(1)$ via atomic counter without acquiring shard locks.
2. **Deterministic Coordinate Reproducibility:**
   - Segment serialization (`NodeCoordinate.String()` and `NodeCoordinate.Segments()`) MUST be deterministic, with map characteristics sorted alphabetically to ensure identical trie paths regardless of runtime map ordering.
