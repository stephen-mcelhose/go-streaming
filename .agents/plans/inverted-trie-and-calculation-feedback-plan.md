# Plan: Inverted Trie Graph Storage & Natural Calculation Feedback

## Objective
Enhance `examples/measurement_graph/` with:
1. **Inverted Prefix Trie Storage (`PrefixTrieGraphStore`)**:
   - Keyspace hierarchy: `[BusinessUnit, EntityType, EntityID, PrimaryCode, ClassID, InstanceID, ...Characteristics]`.
   - $O(\text{depth})$ path discrimination down to class/instance.
   - Localized candidate scanning for "best-fit" score resolution.
   - $O(1)$ subtree pruning for entity invalidations (`InvalidateEntity`) and fact retractions (`Retract`).
2. **Natural Calculation Output Feedback**:
   - Derived calculation outputs are `StreamingMeasurement[float64]` facts.
   - Feeding outputs back into `ClassifyAndPlace` places them into the entity's trie under their derived coordinate, automatically triggering downstream multi-tier calculations.

---

## Detailed Steps

### Step 1: Storage Layer Extensions (`examples/measurement_graph/storage/`)
- Update `GraphStore` interface with:
  - `InvalidateEntity(ctx context.Context, entity EntityIdentity) error`
  - `Retract(ctx context.Context, node NodeCoordinate) error`
- Implement `PrefixTrieGraphStore`:
  - Trie nodes with segment map and recursive prefix pruning.
  - `GetByClass(ctx, entity, classID)` searches only the `Entity` sub-tree.
- Update `MemoryGraphStore` and `NativeRelateDBGraphStore` (Cypher: `MATCH (e:Entity {key: $k}) DETACH DELETE e` / `MATCH (obs:Observation {nodeKey: $k}) DETACH DELETE obs`).

### Step 2: Engine Cascading Feedback (`examples/measurement_graph/engine.go`)
- Support automatic feed-back in `Engine.ClassifyAndPlaceCascading`:
  - Re-injects triggered calculations up to `MaxCascadeDepth` (default: 10).
  - Gathers all leaf & intermediate emitted calculations into `PlacementResult.Triggered`.
- Add `InvalidateEntity` and `Retract` methods on `Engine`.

### Step 3: Tests & RFC 2119 Validation (`examples/measurement_graph/`)
- Test trie subtree invalidation & node retraction.
- Test multi-tier calculation cascading (e.g. `Raw Yield + Moisture -> Standardized Yield -> Production Index`).
- Verify all existing and new tests pass.
