# Specification: RelateDB Graph Storage Adapter

`NativeRelateDBGraphStore` is a GraphStore implementation backed by RelateDB (using C-FFI under `//go:build relate`).

---

## 1. Cypher Translation Contract

### 1.1 Placement (`Put`)
Must execute Cypher MERGE queries creating or updating the Entity root node and the Observation node with a directed relationship:

```cypher
MERGE (e:Entity {key: $entityKey, bu: $bu, entityType: $entityType, entityId: $entityId})
MERGE (obs:Observation {nodeKey: $nodeKey})
SET obs.value = $value,
    obs.timestamp = $ts,
    obs.primaryCode = $primaryCode,
    obs.classId = $classId,
    obs.instanceId = $instanceId
MERGE (e)-[:HAS_OBSERVATION]->(obs)
```

### 1.2 Coordinate Lookup (`Get`)
Must query exact observation by its unique `nodeKey`:

```cypher
MATCH (obs:Observation {nodeKey: $nodeKey})
RETURN obs.nodeKey, obs.value, obs.timestamp, obs.primaryCode, obs.classId, obs.instanceId
```

### 1.3 Entity & Class Subgraph Traversals (`GetByEntity`, `GetByClass`)
Must query observations linked to an Entity via `HAS_OBSERVATION`:

```cypher
MATCH (e:Entity {key: $entityKey})-[:HAS_OBSERVATION]->(obs:Observation)
RETURN obs.nodeKey, obs.value, obs.timestamp, obs.primaryCode, obs.classId, obs.instanceId

MATCH (e:Entity {key: $entityKey})-[:HAS_OBSERVATION]->(obs:Observation {classId: $classId})
RETURN obs.nodeKey, obs.value, obs.timestamp, obs.primaryCode, obs.classId, obs.instanceId
```

### 1.4 Entity Invalidation & Retraction (`InvalidateEntity`, `Retract`)
Must detach and delete entity subgraphs or specific observation nodes:

```cypher
MATCH (e:Entity {key: $entityKey})
OPTIONAL MATCH (e)-[:HAS_OBSERVATION]->(obs:Observation)
DETACH DELETE e, obs

MATCH (obs:Observation {nodeKey: $nodeKey})
DETACH DELETE obs
```

---

## 2. Invariants & Strict Behavior (RFC 2119)
- **MUST** map `NodeCoordinate` strings deterministically to Cypher node properties.
- **MUST** isolate entities such that `InvalidateEntity` removes all associated observations without affecting adjacent entities.
- **MUST** return `(zero, false, nil)` when a coordinate is not found in the graph.
- **MUST** safely close native C database handles upon `Close()`.
