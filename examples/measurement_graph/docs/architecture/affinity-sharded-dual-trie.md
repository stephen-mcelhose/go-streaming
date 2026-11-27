---
title: "Architectural: Single-Owner Affinity Sharding & Dual-Trie Coordinator"
type: "Architectural"
tags:
  - "architecture"
  - "adr-0004"
  - "dual-trie"
  - "affinity-sharding"
  - "gc-pressure"
  - "relatedb"
version: "1.0.0"
status: "Proposed"
---

# Architectural Design: Single-Owner Affinity Sharding & Dual-Trie Coordinator (ADR-0004)

## 1. Executive Summary & Vision

This document details the architecture for scaling our in-memory streaming measurement graph engine across multi-axis queries and fleet-wide invalidations without lock contention, AB-BA deadlocks, or Go garbage collector (GC) thrashing.

To overcome the structural hazards of locking two independent global tries and the hotspotting of metric-first roots, this architecture introduces **Single-Owner Affinity Sharding** paired with an **Internal Dual-Trie Shard Wrapper**. Long-term memory retention and massive historical invalidations are offloaded to our L2 native Graph Database Tier ([RelateDB](https://github.com/stephen-mcelhose/relate) via C-FFI).

---

## 2. ADR-0004: Single-Owner Affinity Sharding with Dual-Trie Wrapper

### 2.1 Context & Problem Statement

Our core streaming engine uses an [Inverted Prefix Trie](../concepts/inverted-prefix-trie.md) (`EntityFirst`) to store observations under an entity boundary, providing $O(\text{depth})$ routing and $O(1)$ entity window pruning. However, maintaining a continuous secondary index (`PrimaryFirst`) for fleet-wide sensor invalidations introduces three critical challenges if implemented as two separate sharded tries:

1. **Metric Root Hotspotting**: In a standalone `PrimaryFirst` trie sharded by prefix, all 10,000 plots reporting `GRAIN_YIELD` hash to the exact same top-level metric prefix (`GRAIN_YIELD -> CLASS_YIELD`), serializing all concurrent ingestion workers onto a single mutex.
2. **Cross-Trie Lock Ordering & Deadlocks**: Acquiring locks across two differently keyed tries during simultaneous `Put` and `Invalidate` operations creates AB-BA deadlock hazards.
3. **Unbounded $O(k)$ Rekey Latency**: Detaching a sensor fleet of $k = 50,000$ observations from a single global `PrimaryFirst` trie holds a lock while rekeying all 50,000 items.

### 2.2 Architectural Decision

We implement a **Single-Owner Affinity Sharding** architecture where the top-level `DualIndexStore` acts as a stateless lock-free router. Incoming observations and entity queries are routed to $M$ independent shards (default $M = 256$) using FNV-1a hashing on `EntityIdentity`:

$$\text{ShardIdx} = \text{fnv32a}(\text{BU}, \text{EntityType}, \text{EntityID}) \ \& \ (M - 1)$$

```
                         [ Top-Level Stateless Router ]
                    ShardIdx = fnv32a(EntityIdentity) & 255
                                       │
         ┌─────────────────────────────┼─────────────────────────────┐
         ▼                             ▼                             ▼
┌─────────────────┐           ┌─────────────────┐           ┌─────────────────┐
│   Shard [0]     │           │   Shard [i]     │           │  Shard [255]    │
│  sync.RWMutex   │           │  sync.RWMutex   │           │  sync.RWMutex   │
│ ┌─────────────┐ │           │ ┌─────────────┐ │           │ ┌─────────────┐ │
│ │TrieByEntity │ │           │ │TrieByEntity │ │           │ │TrieByEntity │ │
│ └─────────────┘ │           │ └─────────────┘ │           │ └─────────────┘ │
│ ┌─────────────┐ │           │ ┌─────────────┐ │           │ ┌─────────────┐ │
│ │TrieByPrimary│ │           │ │TrieByPrimary│ │           │ │TrieByPrimary│ │
│ └─────────────┘ │           │ └─────────────┘ │           │ └─────────────┘ │
└─────────────────┘           └─────────────────┘           └─────────────────┘
```

Each `DualIndexShard` is a self-contained unit encapsulating **one** `sync.RWMutex` and **two** unsharded internal tries:
- **`TrieByEntity`**: Structured as `[BU, EntityType, EntityID, PrimaryCode, ClassID, InstanceID, ...Characteristics]`. Optimized for hot ingestion, calculation slot resolution, and $O(1)$ entity window expiration.
- **`TrieByPrimary`**: Structured as `[PrimaryCode, ClassID, InstanceID, BU, EntityType, EntityID, ...Characteristics]`. Optimized for localized $O(1)$ cross-cutting metric and sensor fleet invalidations.

### 2.3 Invariants & Guarantees

1. **Single-Owner Mutex Isolation (Zero Deadlocks)**: A worker targeting `Shard[i]` acquires only `Shard[i].mu`. It updates both `TrieByEntity` and `TrieByPrimary` atomically under that single lock and releases it. No operation ever holds locks across two shards simultaneously.
2. **Elimination of Metric Hotspots**: Because `TrieByPrimary` is partitioned across 256 shards by `EntityIdentity`, concurrent writes to `GRAIN_YIELD` across different plots hit disjoint shards with $99.61\%$ pairwise collision-free concurrency.
3. **Entity Locality Invariant**: All observations, slot candidates, and historical states for a given `EntityIdentity` reside strictly within a single shard.

---

## 3. Stateless Coordinator & Fanout Modes

The top-level `DualIndexStore` coordinator contains **no global locks** or cross-shard transactional state.

### 3.1 Point & Entity Operations ($O(1)$ Shard Routing)
- `Put(ctx, m)`, `Get(ctx, node)`, `GetByEntity(ctx, entity)`, `GetByClass(ctx, entity, classID)`, and `InvalidateEntity(ctx, entity)` compute `ShardIdx` directly from `EntityIdentity` and execute strictly within that single shard.

### 3.2 Cross-Cutting Fleet Operations (Synchronous vs. Async Broadcast)
For operations without an `EntityIdentity`—such as `InvalidatePrefix("GRAIN_YIELD", "CLASS_YIELD", "COMBINE_1")`—the coordinator fans out across all $M$ shards:
1. **Deterministic Synchronous Fanout (Default)**: Iterates or parallel-fans out across all 256 shards, acquiring each shard's write lock, performing the localized micro-snip, and returning the total pruned count upon completion.
2. **Asynchronous Queue Signaling (Optional Edge Mode)**: Pushes an invalidation command into each shard's local ring buffer and immediately ACKs the edge caller. Each shard drains its command buffer sequentially under its write lock to prevent out-of-order race conditions against newer telemetry.

---

## 4. Localized Micro-Rekeying Mechanics

When an operator revokes a faulty sensor fleet (`InvalidatePrefix("GRAIN_YIELD", "CLASS_YIELD", "COMBINE_1")`), shard isolation transforms a massive global rekey into 256 tiny micro-operations:

```
Shard[i].mu.Lock()
  │
  ├──► 1. Micro-Snip O(1):
  │       detached := shard.TrieByPrimary.DetachPrefix("GRAIN_YIELD", "CLASS_YIELD", "COMBINE_1")
  │       (Detaches only the k_i ≈ k / 256 leaves owned by Shard[i])
  │
  ├──► 2. Targeted Point Retraction O(k_i · depth):
  │       shard.TrieByEntity.PruneMatching(ctx, detached)
  │       (Retracts those exact k_i coordinates from Shard[i]'s entity trie)
  │
Shard[i].mu.Unlock()
```

### Performance Impact
Because global fleet size $k$ is divided across $M = 256$ shards ($k_i \approx \frac{k}{256}$), revoking a sensor across $k = 10,000$ plots means each shard only snips and prunes $\approx 39$ leaves. Per-shard lock hold time remains in the low microseconds, preventing pipeline stalls.

---

## 5. Granular Memory Estimates & Go GC Pointer Tax

Storing pointer-heavy tries in managed Go heap memory incurs structural overhead that dictates where L1 hot memory must hand off to L2 native storage.

### 5.1 Per-Measurement Memory Overhead Factors
- **Go String Headers**: 16 bytes per segment header + backing UTF-8 byte array.
- **Trie Node & Map Overhead**: Each interior `trieNode` with a Go `map` header and bucket pointers costs ~110–130 bytes.
- **Payload Size**: `StoredMeasurement` struct consumes ~64–96 bytes.
- **The Dual-Trie Tax**: Indexing every active observation in both `TrieByEntity` and `TrieByPrimary` doubles the interior trie node and map pointer count (~300–310 bytes total per stored measurement).

### 5.2 Footprint & Garbage Collection Projections (10 Hot Primaries Baseline)

| Scale (Measurements) | Raw Payload Value | Trie Nodes & Map Pointers | Total Go Heap Footprint | Go Garbage Collector (GC) Behavior |
| :--- | :--- | :--- | :--- | :--- |
| **10 Million** | ~550 MB | ~2.55 GB | **~3.1 GB** | **Healthy Zone**: Concurrent GC scans pointers with negligible latency impact. |
| **100 Million** | ~5.5 GB | ~25.5 GB | **~31.0 GB** | **Pointer Tax Threshold**: Tens of millions of heap pointers trigger GC mark-assist thrashing and CPU spikes during mass invalidations. |
| **1 Billion** | ~55.0 GB | ~255.0 GB | **~310.0 GB** | **System Failure Zone (Single Node)**: Unviable in a single Go process heap; pointer scanning dominates CPU cycles and risks OOM. |

### 5.3 1 Billion Measurements Across Shard Topologies ($M = 16, 64, 128, 256$)

Evaluating $N = 1,000,000,000$ measurements (~310 GB total footprint, 10 hot primaries, $k = 100\text{M}$ measurements per primary fleet) across power-of-two shard counts reveals two distinct physical boundaries:
1. **In-Process Sharding** (1 Go runtime with $M$ mutex shards): Sharding reduces **per-shard lock hold times** during `InvalidatePrefix`, but **does not reduce single-process Go GC pointer pressure** (~310 GB heap).
2. **Distributed / Multi-Process Sharding** ($M$ isolated worker nodes/processes): Partitioning shards across separate OS processes or containers divides **both** lock hold time and per-process Go heap footprint.

| Shard Count ($M$) | Measurements / Shard ($N / M$) | Memory / Shard (`~310 GB / M`) | Fleet Invalidation Size / Shard ($k_i = 100\text{M} / M$) | Per-Shard Micro-Snip & Rekey Hold Time ($\approx 0.5\ \mu\text{s} / \text{item}$) | Pairwise Collision Prob ($k=8$ workers) | Distributed Node Viability (1 Process / Shard) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **$M = 16$** | **62.50M** | **~19.38 GB** | 6,250,000 | **~3.12 sec** (Blocks shard ingestion) | $87.9\%$ (High contention) | **Borderline**: ~19.4 GB/node approaches GC mark-assist pressure; 3.1s lock stall on fleet revoke. |
| **$M = 64$** | **15.63M** | **~4.84 GB** | 1,562,500 | **~781 ms** (Sub-second pause) | $36.6\%$ (Moderate) | **Viable**: ~4.8 GB/node is comfortable for Go GC; sub-second fleet invalidation. |
| **$M = 128$** | **7.81M** | **~2.42 GB** | 781,250 | **~390 ms** (Fast spike) | $20.0\%$ (Low contention) | **Healthy**: ~2.4 GB/node runs with minimal GC overhead; sub-400ms fleet invalidation. |
| **$M = 256$** | **3.91M** | **~1.21 GB** | 390,625 | **~195 ms** (Fast recovery) | $10.5\%$ (Near-linear) | **Optimal**: ~1.2 GB/node is firmly in Go's sweet spot; minimal contention. |

#### Key Architectural Takeaways at the 1B Scale:
- **Why In-Process Sharding Alone Fails at 1B**: Even at $M = 256$ shards, a single Go process holding 1B dual-indexed measurements still has a **310 GB shared heap** with billions of pointers. Go's garbage collector is global to the process and will thrash regardless of mutex count.
- **How to Serve 1B Measurements**:
  1. **Single-Node**: Use L1 `DualIndexStore` ($M = 256$) as a bounded hot sliding window ($\le 10\text{M}$ active measurements $\approx 3.1\text{ GB}$ RAM) and offload the remaining 990M historical measurements to L2 **RelateDB** (off-heap Rust/C-FFI).
  2. **Clustered**: Partition the $M = 128$ or $M = 256$ affinity shards across 16–32 physical worker processes (giving ~10–20 GB RAM per process) or 128 lightweight sidecars (~2.4 GB RAM each).

---

## 6. Optimization Tier: L2 Native Graph Offload (RelateDB C-FFI)

To scale beyond the 100M Go pointer threshold without massive RAM or GC pauses, our [Two-Tier Storage Architecture](two-tier-storage.md) pairs the L1 `DualIndexStore` with L2 [RelateDB](../glossary.md#relatedb) over C-FFI (`NativeRelateDBGraphStore`).

### 6.1 Sliding Window L1 + Off-Heap L2 Cleanup
1. **L1 as Bounded Working Memory**: The Go `DualIndexStore` retains only active sliding-window entities required for live Rete slot resolution. Completed entities are pruned in $O(1)$ from Go memory, keeping the Go heap flat and well below the 10M pointer threshold.
2. **Off-Heap Native Fleet Deletion**: During fleet invalidations, each Go shard performs its sub-millisecond micro-snip in L1, while historical cold records are purged off-heap in Rust/C++ space via Cypher:
   ```cypher
   MATCH (e:Entity)-[r:HAS_OBSERVATION]->(obs:Observation)
   WHERE obs.primaryCode = "GRAIN_YIELD"
     AND obs.classId = "CLASS_YIELD"
     AND obs.instanceId = "COMBINE_1"
   DETACH DELETE obs
   ```
3. **Bi-Directional Graph Indexing**: Because RelateDB indexes edges bi-directionally outside Go's GC heap, historical cold queries can traverse from either the `Entity` axis or the `Observation` (`PrimaryCode`) axis without maintaining dual in-memory tries for historical data.

---

## 7. Related Documents
- **Glossary**: [Striped Locking](../glossary.md#striped-locking), [RelateDB](../glossary.md#relatedb), [CoordinatePartitioner](../glossary.md#coordinatepartitioner)
- **Architecture**: [Two-Tier Storage Architecture](two-tier-storage.md), [Striped Concurrency & Shard Scaling](striped-concurrency.md)
- **CodeArtifact**: [artifacts/trie-graph-store.md](../artifacts/trie-graph-store.md), [artifacts/trie-cache.md](../artifacts/trie-cache.md)
