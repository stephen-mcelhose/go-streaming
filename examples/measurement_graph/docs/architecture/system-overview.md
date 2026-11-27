---
title: "Architectural: System Overview & Data Flow"
type: "Architectural"
tags:
  - "architecture"
  - "system-overview"
  - "data-flow"
  - "pipeline"
version: "1.0.0"
status: "Active"
---

# System Overview & Data Flow

## 1. High-Level Architecture

The `measurement_graph` package provides an end-to-end streaming data pipeline that ingests continuous measurement streams, classifies observations across dynamic ontology schemas, places facts into an inverted prefix trie, and executes reactive calculation cascades.

```
[ Ingest Stream ] ──► [ Engine / Classifier ] ──► [ TrieCache (L1/L2) ] ──► [ Router / Calculations ]
                              │                            │                           │
                              │ (Ontology Rules)           │ (O(1) Entity Pruning)     │ (Forward Chaining)
                              ▼                            ▼                           ▼
                     [ Class / Instance ]          [ PrefixTrie / RelateDB ]    [ Derived Fact Stream ]
```

---

## 2. End-to-End Data Flow Sequence

```
Producer                     Engine                   Classifier                   TrieCache                     Router                  Calculation
   │                            │                         │                            │                            │                         │
   │ 1. ClassifyAndPlace(meas)  │                         │                            │                            │                         │
   ├───────────────────────────►│                         │                            │                            │                         │
   │                            │ 2. MatchRules(meas)     │                            │                            │                         │
   │                            ├────────────────────────►│                            │                            │                         │
   │                            │    Returns NodeCoord    │                            │                            │                         │
   │                            │◄────────────────────────┤                            │                            │                         │
   │                            │                                                      │                            │                         │
   │                            │ 3. Put(storedMeas)                                   │                            │                         │
   │                            ├─────────────────────────────────────────────────────►│                            │                         │
   │                            │    (Writes L1 Trie + Propagates to L2 RelateDB)      │                            │                         │
   │                            │                                                      │                            │                         │
   │                            │ 4. CheckAndTrigger(entity, coord)                    │                            │                         │
   │                            ├──────────────────────────────────────────────────────────────────────────────────►│                         │
   │                            │                                                      │ 5. GetByClass(slots)       │                         │
   │                            │                                                      │◄───────────────────────────┤                         │
   │                            │                                                      │    Returns slot facts      │                         │
   │                            │                                                      ├───────────────────────────►│                         │
   │                            │                                                      │                            │ 6. Execute(inputs)      │
   │                            │                                                      │                            ├────────────────────────►│
   │                            │                                                      │                            │    Returns derivedMeas  │
   │                            │                                                      │                            │◄────────────────────────┤
   │                            │    Returns triggered slice                           │                            │                         │
   │                            │◄──────────────────────────────────────────────────────────────────────────────────┤                         │
   │                            │                                                                                                             │
   │                            │ 7. (Optional Feedback) ClassifyAndPlace(derivedMeas) [Recurses for Tier-2+ formulas]                        │
   │                            ├──────┐                                                                                                      │
   │                            │◄─────┘                                                                                                      │
   │                            │                                                                                                             │
   │ Returns PlacementResult    │                                                                                                             │
   │◄───────────────────────────┤                                                                                                             │
```

---

## 3. Subsystem Boundaries & Responsibilities

| Subsystem | Key Types | Primary Responsibility | Performance Target |
| :--- | :--- | :--- | :--- |
| **Ingest & Orchestration** | `Engine`, `PlacementResult` | Coordinates end-to-end flow, recursion depth limiting, and feedback loops. | $< 25.0\ \mu\text{s}$ total pipeline |
| **Ontology Classification** | `ClassSelector`, `InstanceSelector` | Evaluates categorical label schemas (Alpha network) to resolve canonical coordinates. | $< 1.0\ \mu\text{s}$ evaluation |
| **Hot Working Memory (L1)** | `PrefixTrieGraphStore`, `PrefixTrie[K, V]` | Entity-first in-memory index; provides $O(1)$ window invalidation and striped concurrency. | $< 2.0\ \mu\text{s}$ read / $< 5.0\ \mu\text{s}$ write |
| **Persistent Persistence (L2)** | `NativeRelateDBGraphStore`, `RelateDBGraphStore` | Durable embedded graph store via C-FFI; handles Cypher queries, cold lookups, and audit trails. | $< 5.0\ \text{ms}$ in-memory CGo |
| **Reactive Calculation Router** | `Router`, `StatefulCalculation` | Evaluates multi-variable class dependencies (Beta network) and executes domain formulas. | $< 5.0\ \mu\text{s}$ slot join |

---

## 4. Related Documents
- **Glossary**: [Engine](../glossary.md#engine), [Router](../glossary.md#router), [GraphStore](../glossary.md#graphstore)
- **Architecture**: [Two-Tier Storage Architecture](two-tier-storage.md)
- **Architecture**: [Striped Concurrency & Shard Scaling](striped-concurrency.md)
- **CodeArtifact**: [artifacts/classifier-engine.md](../artifacts/classifier-engine.md)
