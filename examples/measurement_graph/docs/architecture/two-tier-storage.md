---
title: "Architectural: Two-Tier Storage Architecture"
type: "Architectural"
tags:
  - "architecture"
  - "storage"
  - "caching"
  - "relatedb"
  - "trie-cache"
version: "1.0.0"
status: "Active"
---

# Two-Tier Storage Architecture

## 1. System Overview

The measurement graph storage subsystem employs a two-tier storage model that separates low-latency stream processing from persistent audit and query capabilities:

```
                  [ Incoming Stream Ingestion ]
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ Tier 1: Hot L1 Working Memory (PrefixTrieGraphStore)         │
│  - In-Memory Inverted Prefix Trie                           │
│  - Striped Lock Ring Concurrency (M = 256 shards)           │
│  - Point Lookup: < 2.0 μs | Point Ingest: < 5.0 μs         │
│  - Subtree Entity Invalidation: O(1) (< 1.0 μs)             │
└──────────────────────────────┬──────────────────────────────┘
                               │
               (Write-Through / Read-Through)
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ Tier 2: Persistent L2 Backing Store (GraphStore)            │
│  - Native RelateDB via C-FFI / Pure Go Memory Fallback      │
│  - Cypher Graph Schema: (Entity)-[:HAS_OBSERVATION]->(Obs)  │
│  - Persistent Audit, Cold Queries & Bulk Graph Analytics    │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. Component Roles & Contracts

| Tier | Component | Technology | Primary Role |
| :--- | :--- | :--- | :--- |
| **Tier 1 (L1)** | `PrefixTrieGraphStore` | Pure Go Generic `PrefixTrie` | Ultra-fast working memory for real-time slot binding, entity calculation triggers, and instantaneous window invalidation. |
| **Tier 2 (L2)** | `NativeRelateDBGraphStore` / `MemoryGraphStore` | Rust [RelateDB](https://github.com/stephen-mcelhose/relate) via [C-FFI](https://en.wikipedia.org/wiki/Foreign_function_interface) / Go Maps | Durable graph persistence, cross-entity graph traversals, Cypher queries, and cold-data reconstruction. |
| **Decorator** | `TrieCache` | Go Composition | Cohesive facade coordinating read-through caching, synchronous write-through, and dual-layer invalidation. |

---

## 3. Read & Write Life Cycles

### 3.1 Read-Through Sequence (`Get`, `GetByEntity`, `GetByClass`)
```
Caller ──► TrieCache.Get(node)
              │
              ├── L1 Hit? ────► Return Value (0 ns L2 overhead)
              │
              └── L1 Miss
                    │
                    ├── L2 Backing Present?
                    │     ├── No  ──► Return (Zero, False, nil)
                    │     └── Yes ──► L2.Get(node)
                    │                   │
                    │                   ├── Found? ──► Populate L1 ──► Return Value
                    │                   └── Miss   ──► Return (Zero, False, nil)
```

### 3.2 Write-Through Sequence (`Put`)
```
Caller ──► TrieCache.Put(meas)
              │
              ├── 1. Write to Hot L1 Trie (Fast-fail on L1 error)
              │
              └── 2. Propagate to Persistent L2 Store (e.g. Cypher MERGE in RelateDB)
```

### 3.3 Dual Invalidation Sequence (`InvalidateEntity`)
```
Caller ──► TrieCache.InvalidateEntity(entity)
              │
              ├── 1. L1.InvalidatePrefix(BU, Type, ID) ──► O(1) Pointer Detachment (< 1 μs)
              │
              └── 2. L2.InvalidateEntity(entity)        ──► Cypher: DETACH DELETE e, obs
```

---

## 4. Invariants & Fault Isolation

1. **L2 Failure Resilience**: If the L2 backing store fails or encounters a disk I/O timeout, the error propagates to the caller, but L1 working memory remains valid and continues serving low-latency reads.
2. **Deterministic Pruning**: Invalidating an entity in `TrieCache` clears both the hot in-memory trie branch and the persistent L2 graph nodes.

---

## 5. Related Documents & Specifications
- **Glossary**: [TrieCache in Glossary](../glossary.md#triecache), [RelateDB](../glossary.md#relatedb)
- **CodeArtifact**: [artifacts/trie-cache.md](../artifacts/trie-cache.md)
- **Specification**: [.agents/measurement-streaming-classifier/specs/trie-cache-spec.md](../../../../.agents/measurement-streaming-classifier/specs/trie-cache-spec.md)
