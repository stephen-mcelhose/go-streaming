---
title: "Concept: Inverted Prefix Trie Hierarchy & O(1) Subtree Pruning"
type: "Concept"
tags:
  - "concept"
  - "data-structure"
  - "prefix-trie"
  - "invalidation"
  - "coordinate-hierarchy"
version: "1.1.0"
status: "Active"
---

# Inverted Prefix Trie Hierarchy & O(1) Subtree Pruning

## 1. Domain Problem & Why Traditional Metric Tries Fail

In measurement systems, intuitive trie hierarchies index data by the metric question at the root:

```
[ Traditional Metric-First Trie ]
GRAIN_YIELD
 └── CLASS_GRAIN_YIELD
      └── COMBINE_1
           ├── plot_101 ──► 185.4
           ├── plot_102 ──► 192.1
           └── plot_103 ──► 178.0
```

### The Streaming Bottlenecks of Metric-First Indexing:
1. **Scattered Entity Working Memory**: To evaluate a calculation for `plot_101` (e.g. `StandardizedYield = f(GRAIN_YIELD, MOISTURE, SPEED)`), the engine must traverse multiple disconnected metric branches across the entire graph.
2. **$O(N)$ Eviction Penalty on Window Close**: In streaming, entities (e.g. field plots, machinery shifts, patient telemetry sessions) have sliding time windows. When `plot_101`'s window closes, clearing its working memory requires **scanning every single metric and class branch in the trie** ($O(N)$ total facts), introducing latency spikes and pipeline stalls.

---

## 2. The Inverted Solution: Entity-First Prefix Indexing

The **Inverted Prefix Trie** ([Trie Reference](https://en.wikipedia.org/wiki/Trie)) flips the hierarchy so the subject entity is at the top:

```
[ Inverted Entity-First Trie Hierarchy ]
BusinessUnit (e.g. Agronomy)
 └── EntityType (e.g. PLOT)
      └── EntityID (e.g. plot_101)                <── ALL working memory localized here
           ├── GRAIN_YIELD
           │    └── CLASS_GRAIN_YIELD
           │         └── COMBINE_1 ──► 185.4
           └── GRAIN_MOISTURE
                └── CLASS_GRAIN_MOISTURE
                     └── PROBE_1   ──► 14.2
```

---

## 3. Concrete Tree Topology & Memory Isolation

By placing `[BU -> EntityType -> EntityID]` as the root prefix path:
- Every observation for `plot_101` lives strictly inside `plot_101`'s subtree pointer.
- `plot_102`'s streaming observations live in a separate, disjoint subtree pointer.
- Lock contention across workers streaming to different plots is eliminated via [Striped Locking](../architecture/two-tier-storage.md).

```
                                  Root
                                 /    \
                       [Agronomy]      [Cardiology]
                           /                |
                     [PLOT]             [PATIENT]
                     /    \                 |
            [plot_101]    [plot_102]    [pat_9901]
               /              \             |
       [GRAIN_YIELD]     [MOISTURE]    [HEART_RATE]
           /                  |             |
   [CLASS_YIELD]        [CLASS_MOIST]   [CLASS_ECG]
         |                    |             |
   [COMBINE_ROTARY]     [PROBE_PIN]     [LEAD_II]
         |                    |             |
   (Leaf: 188.5)         (Leaf: 14.2)   (Leaf: 72.0)
```

---

## 4. O(1) Subtree Pruning Mechanics

When an entity's sliding window closes or its facts expire, `InvalidateEntity` detaches the entire entity branch in a single pointer operation:

```
  typeNode.children["PLOT"]
        ├── "plot_101" ──► (Pointer detached in O(1)) ──► Memory reclaimed / GC'd
        └── "plot_102" ──► (Active working memory completely untouched)
```

### Algorithmic Invariants:
- **Zero Subtree Walking**: Detaching the entity pointer takes $< 1.0\ \mu\text{s}$ whether the entity holds 2 observations or 100,000 observations.
- **Blast-Radius Isolation**: Pruning `plot_101` has zero impact on `plot_102` or any adjacent branch.
- **Atomic Counter Update**: Global measurement counts decrement atomically without locking unaffected shard buckets.

---

## 5. Asymptotic Complexity Comparison Matrix

| Operation | Traditional Metric-First Trie | Inverted Entity-First Trie | Operational Benefit in `go-streaming` |
| :--- | :--- | :--- | :--- |
| **Point Placement (`Put`)** | $O(\text{depth})$ | $O(\text{depth})$ | Sub-microsecond hot path ingestion ($< 2.0\ \mu\text{s}$). |
| **Point Lookup (`Get`)** | $O(\text{depth})$ | $O(\text{depth})$ | Direct path traversal down segment pointers. |
| **Entity Working Memory Scan (`GetByEntity`)** | $O(N)$ (Full graph scan) | **$O(k)$** (Subtree collection) | Instant local access to all entity facts for calculation triggering. |
| **Entity Window Invalidation (`InvalidateEntity`)** | $O(N)$ (Scan every metric branch) | **$O(1)$** (Single pointer detachment) | Deterministic $< 1.0\ \mu\text{s}$ window cleanup on window close. |
| **Metric Bulk Invalidation (`InvalidatePrefix`)** | $O(1)$ | $O(E)$ (Addressed via Rekey) | Solved by [Detached Subtree Rekeying](cross-trie-invalidation.md) or [Rekeying Projections](rekey-and-partitioning.md). |

---

## 6. Related Documents & Specifications
- **Glossary**: [Inverted Prefix Trie in Glossary](../glossary.md#inverted-prefix-trie)
- **Concept**: [Rete Pattern Matching & Slot Resolution](rete-slot-resolution.md)
- **CodeArtifact**: [artifacts/trie-graph-store.md](../artifacts/trie-graph-store.md)
- **Generic Structure**: [artifacts/generic-prefix-trie.md](../artifacts/generic-prefix-trie.md)
