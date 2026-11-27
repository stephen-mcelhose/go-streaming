# Plan: Measurement Graph Classification & Calculation Routing Engine

## 1. Goal & Architectural Vision
Build a streaming classification and calculation routing engine (`examples/measurement_graph/`) capable of:
1. Ingesting continuous streams of polymorphic measurements (`StreamingMeasurement[N]`).
2. Classifying facts across multi-dimensional ontology schemas (`ClassSelector`, `InstanceSelector`) into entity-localized graph coordinates.
3. Automatically routing and triggering multi-variable domain calculations (`StatefulCalculation`) when required inputs are satisfied.
4. Cascading derived calculation outputs forward into downstream formulas (e.g. `Raw Yield + Moisture -> Standardized Yield -> Production Index`).

---

## 2. Core Components & Hierarchy

```
[ Incoming Stream ]
       │
       ▼
[ Classifier Engine ] ──► Matches ClassSelector & InstanceSelector rules
       │
       ▼
[ Storage Layer ] ──────► Inverted Prefix Trie / RelateDB Graph
       │
       ▼
[ Calculation Router ] ─► Resolves variable dependencies across classes
       │
       ▼
[ Cascading Feedback ] ─► Feeds derived calculations back into stream
```

---

## 3. Work Breakdown

1. **Domain Types & Interfaces (`types.go`, `storage/storage.go`):**
   - `EntityIdentity`, `StreamingMeasurement[N]`, `NodeCoordinate`, `StoredMeasurement`.
   - `GraphStore` contract (`Put`, `Get`, `GetByEntity`, `GetByClass`, `InvalidateEntity`, `Retract`, `Count`, `Close`).
2. **Classifier & Placement Engine (`classifier.go`, `engine.go`):**
   - Rule evaluation for `ClassSelector` (required label schema) and `InstanceSelector` (exact match rules).
   - Ingest pipeline returning `(PlacementResult, error)`.
3. **Calculation Orchestrator & Forward Cascading (`calculations.go`, `engine.go`):**
   - Decoupled `ClassSelector` slot resolution.
   - Forward feedback loop for multi-tiered formula chains.
4. **Storage Adapters:**
   - Pure Go `PrefixTrieGraphStore` and `MemoryGraphStore`.
   - `NativeRelateDBGraphStore` (C-FFI) with pure Go fallback.
