---
title: "Measurement Streaming Classifier — Domain & Technical Glossary"
type: "Glossary"
tags:
  - "glossary"
  - "domain-model"
  - "ubiquitous-language"
  - "measurement-graph"
version: "1.0.0"
status: "Active"
---

# Domain & Technical Glossary

This glossary defines the ubiquitous language, internal domain entities, storage mechanics, and external paradigms powering the `examples/measurement_graph` package.

Terms are grouped by category and ordered **lexicographically (A–Z)** within each section per [Documentation Management Guidelines](../AGENTS.md).

---

## 1. External Systems & Core Paradigms

### C-FFI (C Foreign Function Interface)
- **Definition**: The low-level interface mechanism allowing Go code to directly call C-compatible binary libraries (e.g. `.dylib`, `.so`) in-process via CGo, eliminating network hops and RPC serialization overhead.
- **Context in Repo**: Used to bridge pure Go streaming routines to the embedded Rust-based [RelateDB](#relatedb) storage engine.
- **Related Terms**: [RelateDB](#relatedb), [NativeRelateDBGraphStore](#nativerelatedbgraphstore).

### Cypher
- **Definition**: A declarative graph query language for pattern matching and structural mutation over property graphs.
- **Context in Repo**: Used by [NativeRelateDBGraphStore](#nativerelatedbgraphstore) to execute `MERGE`, `MATCH`, and `DETACH DELETE` operations on entity/observation graphs.
- **Related Terms**: [RelateDB](#relatedb), [GraphStore](#graphstore).

### Inverted Prefix Trie
- **Definition**: An ordered tree data structure where path keys are structured entity-first (`[BU -> EntityType -> EntityID -> Metric...]`) rather than metric-first.
- **Advantage**: Localizes all working memory for an entity under a single sub-tree pointer, enabling instant $O(1)$ entity window invalidation upon window close.
- **Related Terms**: [PrefixTrie](#prefixtrie), [NodeCoordinate](#nodecoordinate), [CoordinatePartitioner](#coordinatepartitioner).

### RelateDB
- **Definition**: An embedded, in-process graph and relational storage engine written in Rust with Cypher query support.
- **Role**: Serves as the cold/persistent L2 storage tier backing hot in-memory trie caches.
- **Related Terms**: [C-FFI](#c-ffi-c-foreign-function-interface), [TrieCache](#triecache), [NativeRelateDBGraphStore](#nativerelatedbgraphstore).

### Rete Algorithm & Pattern Network
- **Definition**: A pattern-matching algorithm for production rule systems that constructs a directed acyclic graph (Alpha and Beta memory networks) to evaluate declarative rules against working memory facts.
- **Role**: Drives the dynamic multi-slot binding and best-fit constraint resolution for streaming calculations without database roundtrips.
- **Related Terms**: [ClassSelector](#classselector), [StatefulCalculation](#statefulcalculation), [Router](#router).

---

## 2. Core Domain Entities & Classifier Models

### ClassSelector
- **Definition**: An ontology contract declaring the required metric and categorical label keys needed to satisfy a calculation slot (e.g. `Metric: GRAIN_YIELD`, required labels: `["TAXA", "METHOD"]`).
- **Type**: `examples/measurement_graph.ClassSelector`
- **Role**: Decouples calculation formulas from brittle physical hardware IDs.
- **Related Terms**: [InstanceSelector](#instanceselector), [StatefulCalculation](#statefulcalculation).

### Engine
- **Definition**: The top-level orchestrator that ingests continuous measurement streams, executes classification rules, places facts into graph storage, and triggers calculation pipelines.
- **Type**: `examples/measurement_graph.Engine`
- **Related Terms**: [PlacementResult](#placementresult), [Router](#router), [GraphStore](#graphstore).

### EntityIdentity
- **Definition**: A domain triplet `[BusinessUnit, EntityType, EntityID]` uniquely identifying the physical or logical subject being observed (e.g. `Agronomy:PLOT:plot_37711542`).
- **Type**: `examples/measurement_graph/storage.EntityIdentity`
- **Related Terms**: [NodeCoordinate](#nodecoordinate), [StreamingMeasurement](#streamingmeasurement).

### InstanceSelector
- **Definition**: A concrete classification rule that maps incoming observation label values (or wildcards) to a physical or logical `InstanceID` (e.g. `COMBINE_ROTARY_01`).
- **Type**: `examples/measurement_graph.InstanceSelector`
- **Related Terms**: [ClassSelector](#classselector), [NodeCoordinate](#nodecoordinate).

### NodeCoordinate
- **Definition**: The full hierarchical ontology address of a placed observation: `BU:EntityType:EntityID:PrimaryCode:ClassID:InstanceID[:Key=Value...]`.
- **Type**: `examples/measurement_graph/storage.NodeCoordinate`
- **Methods**: `Segments() []string`, `PrimaryFirstSegments() []string`, `String() string`.
- **Related Terms**: [EntityIdentity](#entityidentity), [CoordinatePartitioner](#coordinatepartitioner).

### PlacementResult
- **Definition**: The output resulting from ingesting a measurement, containing the placed `NodeCoordinate` and any derived calculations triggered during the cascade.
- **Type**: `examples/measurement_graph.PlacementResult`
- **Related Terms**: [Engine](#engine), [StreamingMeasurement](#streamingmeasurement).

### Router
- **Definition**: The calculation dependency evaluation component that checks working memory when an observation is placed and triggers stateful formulas whose required slots are satisfied.
- **Type**: `examples/measurement_graph.Router`
- **Related Terms**: [ClassSelector](#classselector), [StatefulCalculation](#statefulcalculation).

### StatefulCalculation
- **Definition**: A stateful model or mathematical formula that binds to one or more [ClassSelector](#classselector) slots and evaluates when all inputs are present for an entity.
- **Type**: `examples/measurement_graph.StatefulCalculation`
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
- **Related Terms**: [PrefixTrie](#prefixtrie), [Rekey](#rekey).

### DetachPrefix
- **Definition**: An $O(1)$ subtree clipping operation on a trie that snips a target prefix branch and returns it as an independent, fully-functional mini-trie without copying unaffected data.
- **Method**: `PrefixTrie.DetachPrefix(prefix ...K)`
- **Related Terms**: [PrefixTrie](#prefixtrie), [PruneMatching](#prunematching).

### GraphStore
- **Definition**: The core storage interface for putting, getting, querying by entity/class, counting, and invalidating measurement nodes across hot and cold tiers.
- **Type**: `examples/measurement_graph/storage.GraphStore`
- **Implementations**: [PrefixTrieGraphStore](#prefixtriegraphstore), [MemoryGraphStore](#memorygraphstore), [RelateDBGraphStore](#relatedbgraphstore), [NativeRelateDBGraphStore](#nativerelatedbgraphstore), [TrieCache](#triecache).
- **Related Terms**: [TrieCache](#triecache), [StoredMeasurement](#storedmeasurement).

### MemoryGraphStore
- **Definition**: A thread-safe, pure in-memory `GraphStore` implementation using standard Go maps, used for baseline testing and fallback persistence.
- **Type**: `examples/measurement_graph/storage.MemoryGraphStore`
- **Related Terms**: [GraphStore](#graphstore), [PrefixTrieGraphStore](#prefixtriegraphstore).

### NativeRelateDBGraphStore
- **Definition**: A `GraphStore` implementation backed by embedded RelateDB via CGo/C-FFI under the `relate` build tag.
- **Type**: `examples/measurement_graph/storage.NativeRelateDBGraphStore`
- **Related Terms**: [C-FFI](#c-ffi-c-foreign-function-interface), [RelateDB](#relatedb), [GraphStore](#graphstore).

### PrefixTrie
- **Definition**: A pure generic, thread-safe prefix-trie `PrefixTrie[K comparable, V any]` with striped lock-ring concurrency and lock-free atomic counting.
- **Type**: `examples/measurement_graph/storage.PrefixTrie[K, V]`
- **Methods**: `Put`, `Get`, `CollectPrefix`, `CollectAll`, `InvalidatePrefix`, `DetachPrefix`, `Delete`, `Count`, `Close`.
- **Related Terms**: [PrefixTrieGraphStore](#prefixtriegraphstore), [Inverted Prefix Trie](#inverted-prefix-trie).

### PrefixTrieGraphStore
- **Definition**: A domain adapter implementing `GraphStore` on top of `PrefixTrie[string, StoredMeasurement]` with a configured [CoordinatePartitioner](#coordinatepartitioner).
- **Type**: `examples/measurement_graph/storage.PrefixTrieGraphStore`
- **Related Terms**: [PrefixTrie](#prefixtrie), [CoordinatePartitioner](#coordinatepartitioner), [GraphStore](#graphstore).

### PruneMatching
- **Definition**: An operation that removes all coordinates present in a source mini-trie from the receiving store, executing point retractions in $O(k \cdot \text{depth})$ without full-store scans.
- **Method**: `PrefixTrieGraphStore.PruneMatching(ctx, otherStore)`
- **Related Terms**: [DetachPrefix](#detachprefix), [Rekey](#rekey).

### Rekey
- **Definition**: A projection function that transforms any `GraphStore` into a newly partitioned `TrieCache` under a specified [CoordinatePartitioner](#coordinatepartitioner).
- **Function**: `Rekey(ctx, partitioner, srcGraphStore) (*TrieCache, error)`
- **Invariant**: Provably lossless and reversible across repartitioning cycles (lossless isomorphism).
- **Related Terms**: [CoordinatePartitioner](#coordinatepartitioner), [TrieCache](#triecache).

### RelateDBGraphStore
- **Definition**: A pure Go wrapper implementing `GraphStore` with fallback in-memory structures when compiled without native C-FFI tags.
- **Type**: `examples/measurement_graph/storage.RelateDBGraphStore`
- **Related Terms**: [NativeRelateDBGraphStore](#nativerelatedbgraphstore), [GraphStore](#graphstore).

### Striped Locking
- **Definition**: A concurrency architecture using a fixed array of $M$ (e.g. 256) `sync.RWMutex` instances indexed by a hash of the root path segment (`fnv32`), providing constant $\sim 6\ \text{KB}$ mutex memory overhead regardless of active fact volume.
- **Related Terms**: [PrefixTrie](#prefixtrie), [Inverted Prefix Trie](#inverted-prefix-trie).

### TrieCache
- **Definition**: A two-tier layered `GraphStore` decorator embedding an L1 hot `PrefixTrieGraphStore` and delegating to an optional L2 persistent `GraphStore` with read-through, write-through, and dual invalidation semantics.
- **Type**: `examples/measurement_graph/storage.TrieCache`
- **Related Terms**: [PrefixTrieGraphStore](#prefixtriegraphstore), [GraphStore](#graphstore).
