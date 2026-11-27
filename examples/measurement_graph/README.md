---
title: "Measurement Streaming Classifier & Calculation Routing Engine"
type: "Architectural"
tags:
  - "overview"
  - "entrypoint"
  - "measurement-graph"
  - "architecture"
version: "1.1.0"
status: "Active"
---

# Measurement Streaming Classifier & Calculation Routing Engine

Welcome to the `examples/measurement_graph` subsystem. This package implements a high-throughput, reactive streaming measurement classification and stateful calculation routing engine.

---

## 1. Quick Start: Documentation Reading Order

An agent or engineer should explore this documentation in the following order:

```
┌────────────────────────────────────────────────────────┐
│ 1. Ubiquitous Language & Terminology                   │
│    └── docs/glossary.md                                │
└──────────────────────────┬─────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────┐
│ 2. Core Concepts & Theory                              │
│    ├── docs/concepts/inverted-prefix-trie.md           │
│    ├── docs/concepts/rete-slot-resolution.md           │
│    └── docs/concepts/forward-chaining.md               │
└──────────────────────────┬─────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────┐
│ 3. System Architecture & Lifecycle                     │
│    ├── docs/architecture/system-overview.md            │
│    ├── docs/architecture/two-tier-storage.md           │
│    └── docs/architecture/striped-concurrency.md        │
└──────────────────────────┬─────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────┐
│ 4. Code Artifacts & Specification Sheets               │
│    ├── docs/artifacts/generic-prefix-trie.md (Layer 1) │
│    ├── docs/artifacts/trie-graph-store.md    (Layer 2) │
│    ├── docs/artifacts/trie-cache.md          (Layer 3) │
│    └── docs/artifacts/classifier-engine.md   (Layer 4) │
└──────────────────────────┬─────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────┐
│ 5. Formal Definitions & Edge Invariants                │
│    ├── docs/definitions/node-coordinate.md             │
│    └── docs/definitions/stateful-calculation.md        │
└────────────────────────────────────────────────────────┘
```

---

## 2. Executive Architectural Summary

### The Core Problem
In multi-variable streaming computations (e.g. computing standardized metrics across agricultural plots, medical patients, or factory machines), systems face three bottlenecks:
1. **Query & Polling Latency**: Database roundtrips to look up dependent inputs stall streams.
2. **Brittle Sensor Coupling**: Hardcoding calculations to concrete hardware sensor IDs breaks when hardware changes.
3. **$O(N)$ Invalidation Spikes**: Scanning millions of records across metric tables when a sliding entity window closes causes GC and CPU stalls.

### Our Solution
1. **Inverted Prefix Trie Working Memory** ([Concept](docs/concepts/inverted-prefix-trie.md)): Coordinates are entity-first (`BU:EntityType:EntityID:...`), localizing working memory and enabling **$O(1)$ sub-microsecond window pruning**.
2. **Rete Pattern Matching** ([Concept](docs/concepts/rete-slot-resolution.md)): Alpha network evaluates intra-fact labels; Beta network evaluates entity-scoped joins across domain `ClassSelector`s rather than brittle hardware IDs.
3. **Forward-Chaining Feedback Loop** ([Concept](docs/concepts/forward-chaining.md)): Derived calculation outputs are standard facts re-injected into working memory, driving multi-tier formula pipelines synchronously.
4. **Two-Tier Storage Caching** ([Architecture](docs/architecture/two-tier-storage.md)): Ultra-fast L1 hot memory backed by persistent L2 graph stores (including native embedded [RelateDB](https://github.com/stephen-mcelhose/relate) via C-FFI).
5. **Lossless Rekeying & Detached Subtree Invalidation** ([Artifact](docs/artifacts/trie-graph-store.md)): Alternate coordinate projections (e.g. Primary-First for sensor fleets) snip $k$-node subtrees in $O(1)$ and propagate retractions in $O(k \cdot \text{depth})$ without full-table sweeps.

---

## 3. Directory Layout

```
examples/measurement_graph/
├── AGENTS.md                  # Documentation standards, OKF frontmatter rules, and guidelines
├── README.md                  # This entry point
├── types.go                   # High-level classification & routing types
├── classifier.go              # Alpha network rule evaluation
├── calculations.go            # Calculation contracts & forward-chaining types
├── engine.go                  # Top-level Engine orchestrator & router
├── measurement_graph_test.go  # Engine & classification integration test suite
├── relatedb_test.go           # C-FFI RelateDB integration test suite
├── docs/                      # OKF documentation tree
│   ├── glossary.md            # Lexicographically sorted domain & technical glossary
│   ├── concepts/              # Theoretical foundations (Tries, Rete, Forward Chaining)
│   ├── architecture/          # Multi-tier storage, dataflow & concurrency design
│   ├── artifacts/             # Layered API contract sheets & test mapping
│   └── definitions/           # Formal domain definitions
└── storage/                   # Storage engine & data structures
    ├── generic_trie.go        # Pure generic PrefixTrie[K, V] with striped concurrency
    ├── trie_store.go          # Domain adapter PrefixTrieGraphStore
    ├── rekey.go               # Pluggable partitioners & lossless Rekey
    ├── trie_cache.go          # Two-tier L1/L2 read-through/write-through cache
    ├── storage.go             # GraphStore interface & in-memory store
    └── relatedb_native.go     # Embedded RelateDB C-FFI driver
```
