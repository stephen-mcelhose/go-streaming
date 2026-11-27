---
title: "Measurement Streaming Classifier — Domain & Technical Glossary"
type: "Glossary"
tags:
  - "glossary"
  - "domain-model"
  - "ubiquitous-language"
  - "measurement-graph"
version: "1.1.0"
status: "Active"
---

# Domain & Technical Glossary

This glossary defines the ubiquitous language, internal domain entities, storage mechanics, and external paradigms powering the `examples/measurement_graph` package.

Terms are grouped by category and ordered **lexicographically (A–Z)** within each section per [Documentation Management Guidelines](../AGENTS.md).

---

## 1. External Systems & Core Paradigms

### Birthday Problem & Hash Collisions
- **Definition**: A probability theory concept describing the likelihood that two elements share a bucket in a finite hash space.
- **Reference**: [Wikipedia: Birthday Problem](https://en.wikipedia.org/wiki/Birthday_problem)
- **Role**: Models concurrent worker lock contention across striped lock shards ($M = 256$), demonstrating that collision probability reaches $\approx 50\%$ at $k = 19$ saturated workers.
- **Related Terms**: [Striped Locking](#striped-locking), [PrefixTrie](#prefixtrie).

### C-FFI (C Foreign Function Interface)
- **Definition**: The interface mechanism allowing higher-level languages (like Go, Python, or Rust) to directly call compiled C-compatible functions in-process via CGo, eliminating network hops and serialization overhead.
- **Reference**: [Wikipedia: Foreign Function Interface](https://en.wikipedia.org/wiki/Foreign_function_interface)
- **Role**: Bridges pure Go streaming routines to the embedded Rust-based [RelateDB](#relatedb) storage engine.
- **Related Terms**: [RelateDB](#relatedb), [NativeRelateDBGraphStore](#nativerelatedbgraphstore).

### Cypher Query Language
- **Definition**: A declarative graph query language for expressive and efficient pattern matching and structural mutation over property graphs.
- **Reference**: [openCypher Standard](https://opencypher.org/)
- **Role**: Used by [NativeRelateDBGraphStore](#nativerelatedbgraphstore) to execute `MERGE`, `MATCH`, and `DETACH DELETE` operations on entity/observation graphs.
- **Related Terms**: [RelateDB](#relatedb), [GraphStore](#graphstore).

### FNV-1a Hash Algorithm
- **Definition**: A non-cryptographic, high-dispersion, low-latency hash function designed for fast table lookups and lock sharding.
- **Reference**: [IETF: The FNV Non-Cryptographic Hash Algorithm](https://datatracker.ietf.org/doc/html/draft-eastlake-fnv-17)
- **Role**: Hashes coordinate path segments to determine lock shard assignment in [PrefixTrie](#prefixtrie).
- **Related Terms**: [Striped Locking](#striped-locking), [PrefixTrie](#prefixtrie).

### Forward Chaining
- **Definition**: An inference method in rule-based expert systems where available facts are placed into working memory to deduce new facts iteratively until a goal is reached or no further rules match.
- **Reference**: [Wikipedia: Forward Chaining](https://en.wikipedia.org/wiki/Forward_chaining)
- **Role**: Drives multi-tier calculation cascading where derived calculation outputs are placed into working memory to trigger downstream formulas.
- **Related Terms**: [Engine](#engine), [PlacementResult](#placementresult), [StatefulCalculation](#statefulcalculation).

### Inverted Prefix Trie
- **Definition**: An ordered tree data structure (radix/prefix trie) where coordinate paths are structured entity-first (`[BU -> EntityType -> EntityID -> Metric...]`) rather than metric-first.
- **Reference**: [Wikipedia: Trie](https://en.wikipedia.org/wiki/Trie)
- **Advantage**: Localizes working memory per entity under a single sub-tree pointer, enabling instant $O(1)$ subtree pruning on window close.
- **Related Terms**: [PrefixTrie](#prefixtrie), [NodeCoordinate](#nodecoordinate), [CoordinatePartitioner](#coordinatepartitioner).

### RelateDB
- **Definition**: An embedded, in-process graph and relational storage engine written in Rust with Cypher query support.
- **Reference**: [RelateDB Repository](https://github.com/stephen-mcelhose/relate)
- **Role**: Serves as the cold/persistent L2 storage tier backing hot in-memory trie caches.
- **Related Terms**: [C-FFI](#c-ffi-c-foreign-function-interface), [TrieCache](#triecache), [NativeRelateDBGraphStore](#nativerelatedbgraphstore).

### Rete Algorithm & Pattern Network
- **Definition**: A pattern-matching algorithm for production rule systems that constructs a directed acyclic graph (Alpha and Beta memory networks) to evaluate declarative rules against working memory facts.
- **Reference**: [Wikipedia: Rete Algorithm](https://en.wikipedia.org/wiki/Rete_algorithm) / [Forgy (1982) Paper](https://doi.org/10.1016/0004-3702(82)90020-0)
- **Role**: Drives dynamic multi-slot binding and best-fit constraint resolution for streaming calculations without database roundtrips.
- **Related Terms**: [ClassSelector](#classselector), [StatefulCalculation](#statefulcalculation), [Router](#router).

---

## 2. Core Domain Entities & Classifier Models

### ClassSelector
- **Definition**: An ontology contract declaring the required metric and categorical label keys needed to satisfy a calculation slot (e.g. `Metric: GRAIN_YIELD`, required labels: `["TAXA", "METHOD"]`).
- **Type**: `examples/measurement_graph.ClassSelector`
- **Artifact**: [artifacts/classifier-engine.md](artifacts/classifier-engine.md)
- **Role**: Decouples calculation formulas from brittle physical hardware IDs.
- **Related Terms**: [InstanceSelector](#instanceselector), [StatefulCalculation](#statefulcalculation).

### Engine
- **Definition**: The top-level orchestrator that ingests continuous measurement streams, executes classification rules, places facts into graph storage, and triggers calculation pipelines.
- **Type**: `examples/measurement_graph.Engine`
- **Artifact**: [artifacts/classifier-engine.md](artifacts/classifier-engine.md)
- **Related Terms**: [PlacementResult](#placementresult), [Router](#router), [GraphStore](#graphstore).

### EntityIdentity
- **Definition**: A domain triplet `[BusinessUnit, EntityType, EntityID]` uniquely identifying the physical or logical subject being observed (e.g. `Agronomy:PLOT:plot_37711542`).
- **Type**: `examples/measurement_graph/storage.EntityIdentity`
- **Related Terms**: [NodeCoordinate](#nodecoordinate), [StreamingMeasurement](#streamingmeasurement).

### InstanceSelector
- **Definition**: A concrete classification rule that maps incoming observation label values (or wildcards) to a physical or logical `InstanceID` (e.g. `COMBINE_ROTARY_01`).
- **Type**: `examples/measurement_graph.InstanceSelector`
- **Artifact**: [artifacts/classifier-engine.md](artifacts/classifier-engine.md)
- **Related Terms**: [ClassSelector](#classselector), [NodeCoordinate](#nodecoordinate).

### NodeCoordinate
- **Definition**: The full hierarchical ontology address of a placed observation: `BU:EntityType:EntityID:PrimaryCode:ClassID:InstanceID[:Key=Value...]`.
- **Type**: `examples/measurement_graph/storage.NodeCoordinate`
- **Methods**: `Segments() []string`, `PrimaryFirstSegments() []string`, `String() string`.
- **Related Terms**: [EntityIdentity](#entityidentity), [CoordinatePartitioner](#coordinatepartitioner).

### PlacementResult
- **Definition**: The output resulting from ingesting a measurement, containing the placed `NodeCoordinate` and any derived calculations triggered during the cascade.
- **Type**: `examples/measurement_graph.PlacementResult`
- **Artifact**: [artifacts/classifier-engine.md](artifacts/classifier-engine.md)
- **Related Terms**: [Engine](#engine), [StreamingMeasurement](#streamingmeasurement).

### Router
- **Definition**: The calculation dependency evaluation component that checks working memory when an observation is placed and triggers stateful formulas whose required slots are satisfied.
- **Type**: `examples/measurement_graph.Router`
- **Artifact**: [artifacts/classifier-engine.md](artifacts/classifier-engine.md)
- **Related Terms**: [ClassSelector](#classselector), [StatefulCalculation](#statefulcalculation).

### StatefulCalculation
- **Definition**: A stateful model or mathematical formula that binds to one or more [ClassSelector](#classselector) slots and evaluates when all inputs are present for an entity.
- **Type**: `examples/measurement_graph.StatefulCalculation`
- **Artifact**: [artifacts/classifier-engine.md](artifacts/classifier-engine.md)
- **Related Terms**: [ClassSelector](#classselector), [Router](#router), [PlacementResult](#placementresult).

### StreamingMeasurement
- **Definition**: An incoming observation containing subject identity, a primary numeric metric reading, categorical key-value labels, numeric secondary readings, and a timestamp.
- **Type**: `examples/measurement_graph/storage.StreamingMeasurement[N OrderedNumber]`
- **Related Terms**: [EntityIdentity](#entityidentity), [StoredMeasurement](#storedmeasurement).

### StoredMeasurement
- **Definition**: A materialized measurement residing in graph storage indexed at an exact [NodeCoordinate](#nodecoordinate).
- **Type**: `examples/measurement_graph/storage.StoredMeasurement`
- **Related Terms**: [NodeCoordinate](#nodecoordinate), [GraphStore](#graphstore).

---

## 3. Storage Architecture, Caching & Partitioning

### CoordinatePartitioner
- **Definition**: A pluggable function signature `func(coord NodeCoordinate) []string` mapping a coordinate to an ordered sequence of trie path segments.
- **Built-in Implementations**:
  - `EntityFirstPartitioner`: `[BU -> Type -> ID -> Primary -> Class -> Instance -> ...Chars]`
  - `PrimaryFirstPartitioner`: `[Primary -> Class -> Instance -> BU -> Type -> ID -> ...Chars]`
  - `ClassFirstPartitioner`: `[Class -> Primary -> Instance -> BU -> Type -> ID -> ...Chars]`
- **Artifact**: [artifacts/trie-graph-store.md](artifacts/trie-graph-store.md)
- **Related Terms**: [PrefixTrie](#prefixtrie), [Rekey](#rekey).

### DetachPrefix
- **Definition**: An $O(1)$ subtree clipping operation on a trie that snips a target prefix branch and returns it as an independent, fully-functional mini-trie without copying unaffected data.
- **Method**: `PrefixTrie.DetachPrefix(prefix ...K)`
- **Artifact**: [artifacts/generic-prefix-trie.md](artifacts/generic-prefix-trie.md)
- **Related Terms**: [PrefixTrie](#prefixtrie), [PruneMatching](#prunematching).

### GraphStore
- **Definition**: The core storage interface for putting, getting, querying by entity/class, counting, and invalidating measurement nodes across hot and cold tiers.
- **Type**: `examples/measurement_graph/storage.GraphStore`
- **Implementations**: [PrefixTrieGraphStore](#prefixtriegraphstore), [MemoryGraphStore](#memorygraphstore), [RelateDBGraphStore](#relatedbgraphstore), [NativeRelateDBGraphStore](#nativerelatedbgraphstore), [TrieCache](#triecache).
- **Artifact**: [artifacts/trie-graph-store.md](artifacts/trie-graph-store.md)
- **Related Terms**: [TrieCache](#triecache), [StoredMeasurement](#storedmeasurement).

### MemoryGraphStore
- **Definition**: A thread-safe, pure in-memory `GraphStore` implementation using standard Go maps, used for baseline testing and fallback persistence.
- **Type**: `examples/measurement_graph/storage.MemoryGraphStore`
- **Related Terms**: [GraphStore](#graphstore), [PrefixTrieGraphStore](#prefixtriegraphstore).

### NativeRelateDBGraphStore
- **Definition**: A `GraphStore` implementation backed by embedded RelateDB via CGo/C-FFI under the `relate` build tag.
- **Type**: `examples/measurement_graph/storage.NativeRelateDBGraphStore`
- **Artifact**: [artifacts/trie-cache.md](artifacts/trie-cache.md)
- **Related Terms**: [C-FFI](#c-ffi-c-foreign-function-interface), [RelateDB](#relatedb), [GraphStore](#graphstore).

### PrefixTrie
- **Definition**: A pure generic, thread-safe prefix-trie `PrefixTrie[K comparable, V any]` with striped lock-ring concurrency and lock-free atomic counting.
- **Type**: `examples/measurement_graph/storage.PrefixTrie[K, V]`
- **Methods**: `Put`, `Get`, `CollectPrefix`, `CollectAll`, `InvalidatePrefix`, `DetachPrefix`, `Delete`, `Count`, `Close`.
- **Artifact**: [artifacts/generic-prefix-trie.md](artifacts/generic-prefix-trie.md)
- **Related Terms**: [PrefixTrieGraphStore](#prefixtriegraphstore), [Inverted Prefix Trie](#inverted-prefix-trie).

### PrefixTrieGraphStore
- **Definition**: A domain adapter implementing `GraphStore` on top of `PrefixTrie[string, StoredMeasurement]` with a configured [CoordinatePartitioner](#coordinatepartitioner).
- **Type**: `examples/measurement_graph/storage.PrefixTrieGraphStore`
- **Artifact**: [artifacts/trie-graph-store.md](artifacts/trie-graph-store.md)
- **Related Terms**: [PrefixTrie](#prefixtrie), [CoordinatePartitioner](#coordinatepartitioner), [GraphStore](#graphstore).

### PruneMatching
- **Definition**: An operation that removes all coordinates present in a source mini-trie from the receiving store, executing point retractions in $O(k \cdot \text{depth})$ without full-store scans.
- **Method**: `PrefixTrieGraphStore.PruneMatching(ctx, otherStore)`
- **Artifact**: [artifacts/trie-graph-store.md](artifacts/trie-graph-store.md)
- **Related Terms**: [DetachPrefix](#detachprefix), [Rekey](#rekey).

### Rekey
- **Definition**: A projection function that transforms any `GraphStore` into a newly partitioned `TrieCache` under a specified [CoordinatePartitioner](#coordinatepartitioner).
- **Function**: `Rekey(ctx, partitioner, srcGraphStore) (*TrieCache, error)`
- **Invariant**: Provably lossless and reversible across repartitioning cycles (lossless isomorphism).
- **Artifact**: [artifacts/trie-graph-store.md](artifacts/trie-graph-store.md)
- **Related Terms**: [CoordinatePartitioner](#coordinatepartitioner), [TrieCache](#triecache).

### RelateDBGraphStore
- **Definition**: A pure Go wrapper implementing `GraphStore` with fallback in-memory structures when compiled without native C-FFI tags.
- **Type**: `examples/measurement_graph/storage.RelateDBGraphStore`
- **Related Terms**: [NativeRelateDBGraphStore](#nativerelatedbgraphstore), [GraphStore](#graphstore).

### Striped Locking
- **Definition**: A concurrency architecture using a fixed array of $M$ (e.g. 256) `sync.RWMutex` instances indexed by a hash of the root path segment (`fnv32`), providing constant $\sim 6\ \text{KB}$ mutex memory overhead regardless of active fact volume.
- **Related Terms**: [PrefixTrie](#prefixtrie), [Inverted Prefix Trie](#inverted-prefix-trie), [Birthday Problem](#birthday-problem--hash-collisions).

### TrieCache
- **Definition**: A two-tier layered `GraphStore` decorator embedding an L1 hot `PrefixTrieGraphStore` and delegating to an optional L2 persistent `GraphStore` with read-through, write-through, and dual invalidation semantics.
- **Type**: `examples/measurement_graph/storage.TrieCache`
- **Artifact**: [artifacts/trie-cache.md](artifacts/trie-cache.md)
- **Related Terms**: [PrefixTrieGraphStore](#prefixtriegraphstore), [GraphStore](#graphstore).
