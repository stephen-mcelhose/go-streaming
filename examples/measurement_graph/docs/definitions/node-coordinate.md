---
title: "Definition: NodeCoordinate"
type: "Definition"
tags:
  - "definition"
  - "domain-model"
  - "coordinates"
  - "ontology"
version: "1.0.0"
status: "Active"
---

# Definition: NodeCoordinate

## 1. Formal Definition

A `NodeCoordinate` is the immutable, multi-dimensional address identifying an observation's exact position within the inverted graph ontology. It uniquely locates a fact across subject identity, metric question, ontology classification, concrete hardware/model instance, and optional discriminators.

---

## 2. Structural Schema & Segments

A `NodeCoordinate` is represented in memory as:

```go
type NodeCoordinate struct {
    Entity          EntityIdentity    // [BusinessUnit, EntityType, EntityID]
    PrimaryCode     string            // Primary metric / question code (e.g. "GRAIN_YIELD")
    ClassID         string            // Parent domain class (e.g. "CLASS_GRAIN_YIELD")
    InstanceID      string            // Concrete classifier instance (e.g. "COMBINE_ROTARY")
    Characteristics map[string]string // Expanding key-space of identifying discriminators
}
```

### Path Hierarchy Segments:
1. **`BusinessUnit`**: Top-level administrative or domain namespace (e.g. `"Agronomy"`).
2. **`EntityType`**: Physical or logical category of the observed subject (e.g. `"PLOT"`).
3. **`EntityID`**: Unique subject identifier within the business unit and entity type (e.g. `"plot_37711542"`).
4. **`PrimaryCode`**: The core question or metric being observed (e.g. `"GRAIN_YIELD"`).
5. **`ClassID`**: The ontology class grouping matching categorical label schemas (e.g. `"CLASS_GRAIN_YIELD"`).
6. **`InstanceID`**: The concrete sensor, instrument, or calculation instance (e.g. `"COMBINE_ROTARY"`).
7. **`Characteristics`** *(Optional)*: Alphabetically sorted key-value pairs (e.g. `PASS=1`, `SECTION=North`).

---

## 3. String Serialization & Invariants

### 3.1 String Format
```
BU:EntityType:EntityID:PrimaryCode:ClassID:InstanceID[:Key1=Val1,Key2=Val2...]
```

### 3.2 Invariants
1. **Determinism**: Characteristic keys **MUST** be sorted alphabetically prior to serialization so that identical coordinates produce identical string keys and path segments regardless of runtime map ordering.
2. **Uniqueness**: A unique `NodeCoordinate` **MUST** map to exactly one value leaf in a `PrefixTrie`.
3. **Partition Invariance**: The data fields of a `NodeCoordinate` remain invariant under different `CoordinatePartitioner` functions; only the path segment sequence changes.

---

## 4. Related Documents
- **Glossary**: [NodeCoordinate in Glossary](../glossary.md#nodecoordinate)
- **Concept**: [Inverted Prefix Trie](../concepts/inverted-prefix-trie.md)
- **CodeArtifact**: [artifacts/trie-graph-store.md](../artifacts/trie-graph-store.md)
