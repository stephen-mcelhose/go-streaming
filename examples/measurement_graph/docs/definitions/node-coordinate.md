---
title: "Definition: NodeCoordinate"
type: "Definition"
tags:
  - "definition"
  - "domain-model"
  - "coordinates"
  - "ontology"
  - "characteristics"
version: "1.1.0"
status: "Active"
---

# Definition: NodeCoordinate

## 1. Formal Definition

A `NodeCoordinate` is the immutable, multi-dimensional address identifying an observation's exact position within the inverted graph ontology. It uniquely locates a fact across subject identity, metric question, ontology classification, concrete hardware/model instance, and optional identifying characteristics.

---

## 2. Structural Schema & Segments

A `NodeCoordinate` is defined in Go as:

```go
type NodeCoordinate struct {
    Entity          EntityIdentity    // [BusinessUnit, EntityType, EntityID]
    PrimaryCode     string            // Primary metric / question code (e.g. "GRAIN_YIELD")
    ClassID         string            // Parent domain class (e.g. "CLASS_GRAIN_YIELD")
    InstanceID      string            // Concrete classifier instance (e.g. "COMBINE_ROTARY")
    Characteristics map[string]string // Expanding key-space of identifying discriminators
}
```

### Segment Hierarchy (Root to Leaf):
1. **`BusinessUnit`**: Top-level administrative or domain namespace (e.g. `"Agronomy"`).
2. **`EntityType`**: Physical or logical category of the observed subject (e.g. `"PLOT"`).
3. **`EntityID`**: Unique subject identifier within the business unit and entity type (e.g. `"plot_37711542"`).
4. **`PrimaryCode`**: The core question or metric being observed (e.g. `"GRAIN_YIELD"`).
5. **`ClassID`**: The ontology class grouping matching categorical label schemas (e.g. `"CLASS_GRAIN_YIELD"`).
6. **`InstanceID`**: The concrete sensor, instrument, or calculation instance (e.g. `"COMBINE_ROTARY"`).
7. **`Characteristics`** *(Optional)*: Key-value discriminators appended as leaf path segments (e.g. `PASS=1`, `SECTION=North`).

---

## 3. Serialization, Determinism & Key-Space Invariants

### 3.1 String Representation
```
BU:EntityType:EntityID:PrimaryCode:ClassID:InstanceID[:Key1=Val1,Key2=Val2...]
```

### 3.2 Invariants & Rules
1. **Alphabetical Characteristic Sorting (Determinism)**:
   - Go map iteration order is randomized at runtime.
   - When generating path segments (`Segments()`) or string keys (`String()`), characteristic keys **MUST** be sorted alphabetically prior to emission:
     $$\text{Keys}(\text{Characteristics}) = [k_1, k_2, \dots, k_m] \quad \text{where} \quad k_1 < k_2 < \dots < k_m$$
   - This ensures identical coordinates always resolve to identical trie paths regardless of runtime map creation order.
2. **Unique Leaf Mapping**: A unique `NodeCoordinate` **MUST** map to exactly one value leaf in a `PrefixTrie`.
3. **Partition Invariance**: The data fields of a `NodeCoordinate` remain invariant under different `CoordinatePartitioner` functions; only the path segment sequence changes.

---

## 4. Characteristic Matching Precedence (Exact vs. Presence)

### Resolved Policy:
In multi-dimensional classification, an incoming observation may have characteristic labels matching rules at different specificity levels:
- **Exact (Closed) Matcher**: `Key1=foo` (Closed rule matching an exact value $\to$ +10 points).
- **Existence / Presence (Open) Matcher**: `Key1` (Open wildcard matching the presence of the key regardless of value $\to$ +1 point).

### Specification & Precedence Rules:
1. `Classifier.Classify` calculates a quantitative `SpecificityScore(m)` for every matching candidate rule:
   $$\text{Score} = (\text{count of exact closed matches} \times 10) + (\text{count of open presence matches} \times 1)$$
2. The candidate with the highest specificity score **MUST** be selected, ensuring closed exact matches take precedence over open wildcards regardless of registration order.
3. If no exact value rule matches, open presence rules serve as the fallback classifier.

---

## 5. Related Documents
- **Glossary**: [NodeCoordinate in Glossary](../glossary.md#nodecoordinate)
- **Concept**: [Inverted Prefix Trie](../concepts/inverted-prefix-trie.md)
- **Concept**: [Rete Pattern Matching & Slot Resolution](../concepts/rete-slot-resolution.md)
- **CodeArtifact**: [artifacts/trie-graph-store.md](../artifacts/trie-graph-store.md)
