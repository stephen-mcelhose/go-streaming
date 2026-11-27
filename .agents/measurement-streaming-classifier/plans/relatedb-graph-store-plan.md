# Plan: RelateDB Graph Storage Adapter (Pure Go & Native C-FFI)

## 1. Goal & Objectives
Integrate RelateDB as an L2 persistent graph store backing the measurement streaming engine:
1. Provide a **pure Go in-memory fallback** for testing environments without RelateDB native binaries.
2. Provide a **live native C-FFI adapter (`NativeRelateDBGraphStore`)** under the `relate` build tag utilizing `github.com/stephen-mcelhose/relate/clients/go`.
3. Support Cypher graph queries for measurement placement, node lookups, class-based traversals, and $O(1)$ entity subgraph detachments.

---

## 2. Graph Schema Mapping in RelateDB

```cypher
(:Entity {
    key: "Agronomy:PLOT:plot_101",
    bu: "Agronomy",
    entityType: "PLOT",
    entityId: "plot_101"
})
  │
  [:HAS_OBSERVATION]
  │
  ▼
(:Observation {
    nodeKey: "Agronomy:PLOT:plot_101:GRAIN_YIELD:CLASS_GRAIN_YIELD:COMBINE_1",
    primaryCode: "GRAIN_YIELD",
    classId: "CLASS_GRAIN_YIELD",
    instanceId: "COMBINE_1",
    value: 185.4,
    timestamp: 1700000000
})
```

---

## 3. Work Breakdown

1. **Pure Go Fallback (`storage/storage.go`):**
   - Provide `RelateDBGraphStore` implementing `GraphStore` when C-FFI is unavailable.
2. **Native C-FFI Driver (`storage/relatedb_native.go`):**
   - Implements `GraphStore` with CGo FFI bindings.
   - Guarded by `//go:build relate`.
   - `Put`: Executes `MERGE (e:Entity {key: $k}) ... MERGE (obs:Observation {nodeKey: $nk})`.
   - `GetByClass`: Cypher query matching `(e:Entity)-[:HAS_OBSERVATION]->(obs:Observation {classId: $cid})`.
   - `InvalidateEntity`: Cypher query executing `MATCH (e:Entity {key: $k})-[r:HAS_OBSERVATION]->(obs:Observation) DETACH DELETE e, obs`.
3. **Integration Test Suite (`relatedb_test.go`, `storage/relatedb_native_test.go`):**
   - Validates Cypher MERGE, lookups, and graph traversals against a live in-memory RelateDB instance.
