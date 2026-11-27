---
title: "Concept: Inverted Prefix Trie Hierarchy & O(1) Subtree Pruning"
type: "Concept"
tags:
  - "concept"
  - "data-structure"
  - "prefix-trie"
  - "invalidation"
version: "1.0.0"
status: "Active"
---

# Inverted Prefix Trie Hierarchy & O(1) Subtree Pruning

## 1. Domain Problem & Theory

In continuous streaming measurement systems, traditional trie layouts index metrics at the root (Metric-First):
$$\text{Metric} \longrightarrow \text{Class} \longrightarrow \text{Instance} \longrightarrow \text{Entity}$$

While Metric-First indexing makes cross-fleet sensor monitoring intuitive, it creates a fatal bottleneck for streaming calculations:
- Calculating a derived metric for an entity (e.g. plot `plot_101`) requires traversing across disjoint metric subtrees.
- When an entity's observation window closes or expires, invalidating all working memory for that entity requires an $O(N)$ full-tree sweep across all metric branches.

### The Inverted Solution: Entity-First Prefix Indexing
The **Inverted Prefix Trie** ([Trie Reference](https://en.wikipedia.org/wiki/Trie)) structures the coordinate keyspace entity-first:
$$\text{BusinessUnit} \longrightarrow \text{EntityType} \longrightarrow \text{EntityID} \longrightarrow \text{PrimaryCode} \longrightarrow \text{ClassID} \longrightarrow \text{InstanceID} \longrightarrow [\text{Characteristics}\dots]$$

---

## 2. Structural Topology

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
   (Leaf Value: 188.5)   (Value: 14.2)  (Value: 72.0)
```

---

## 3. Asymptotic Complexity Matrix

| Operation | Metric-First Trie | Inverted Entity-First Trie | Notes |
| :--- | :--- | :--- | :--- |
| **Point Placement (`Put`)** | $O(\text{depth})$ | $O(\text{depth})$ | Identical descent cost ($6\text{--}8$ pointer hops, $< 2.0\ \mu\text{s}$). |
| **Point Lookup (`Get`)** | $O(\text{depth})$ | $O(\text{depth})$ | Direct path traversal down segment pointers. |
| **Entity Working Memory Scan (`GetByEntity`)** | $O(N)$ (Full graph scan) | **$O(k)$** (Subtree collection) | Gathers all observations for 1 entity in sub-microsecond time. |
| **Entity Window Invalidation (`InvalidateEntity`)** | $O(N)$ (Scan every metric branch) | **$O(1)$** (Single pointer detachment) | Prunes all entity working memory by severing `typeNode.children[EntityID]`. |
| **Metric Invalidation (`InvalidatePrefix`)** | $O(1)$ | $O(\text{Entities})$ | Solved via [Rekey Projection](rekey-and-partitioning.md) or [Detached Subtrees](cross-trie-invalidation.md). |

---

## 4. Subtree Detachment Mechanics

When an entity window closes or an eviction event occurs, `InvalidateEntity` executes a pointer slice:

```
  typeNode.children["PLOT"]
        ├── "plot_101" ──► (Detached in O(1)) ──► Eligible for GC / Batch RelateDB Flush
        └── "plot_102" ──► (Untouched active working memory)
```

The operation does not walk leaf nodes or rebalance trees, providing deterministic $< 1.0\ \mu\text{s}$ invalidation latency regardless of how many thousands of metrics reside under the entity.

---

## 5. Related Artifacts & Specifications
- **CodeArtifact**: [artifacts/trie-graph-store.md](../artifacts/trie-graph-store.md)
- **Generic Data Structure**: [artifacts/generic-prefix-trie.md](../artifacts/generic-prefix-trie.md)
- **Specification**: [.agents/measurement-streaming-classifier/specs/rekey-and-partitioner-spec.md](../../../../.agents/measurement-streaming-classifier/specs/rekey-and-partitioner-spec.md)
