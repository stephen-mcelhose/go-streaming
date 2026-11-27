# Plan: Primary-Rooted Repartitioned Trie Projection & Invalidation

## 1. Problem & Context
- **Entity-Rooted Trie (Default):**
  - Hierarchy: `[BU, EntityType, EntityID, PrimaryCode, ClassID, InstanceID, ...Characteristics]`
  - Optimized for: Entity-level window invalidation (`InvalidateEntity`), entity calculation evaluation.
  - Limitation: Pruning a specific `PrimaryCode` (e.g. sensor recalibration), `Primary.Class`, or `Primary.Class.Instance` across all entities requires scanning every entity subtree ($O(\text{Entities})$).
- **Goal:**
  - Support repartitioning the trie along arbitrary coordinate dimensions (e.g. `PrimaryCode -> ClassID -> InstanceID -> BU -> Type -> ID`).
  - Enable $O(1)$ sub-tree pruning for:
    1. **`InvalidatePrefix("GRAIN_YIELD")`**: Drops all measurements for a metric across the entire system in $O(1)$.
    2. **`InvalidatePrefix("GRAIN_YIELD", "CLASS_YIELD")`**: Drops an entire classification tier across entities.
    3. **`InvalidatePrefix("GRAIN_YIELD", "CLASS_YIELD", "COMBINE_1")`**: Drops a faulty sensor/instance fleet across all entities.

---

## 2. Design: Generic Segment Partitioners & `Rekey`

### 2.1 Coordinate Partitioner Contract
Instead of hardcoding a new trie structure, make segment extraction a pluggable function:

```go
// CoordinatePartitioner maps a NodeCoordinate into ordered trie path segments.
type CoordinatePartitioner func(coord NodeCoordinate) []string

// EntityFirstPartitioner (Default): [BU, EntityType, EntityID, PrimaryCode, ClassID, InstanceID, ...Chars]
func EntityFirstPartitioner(c NodeCoordinate) []string

// PrimaryFirstPartitioner: [PrimaryCode, ClassID, InstanceID, BU, EntityType, EntityID, ...Chars]
func PrimaryFirstPartitioner(c NodeCoordinate) []string

// ClassFirstPartitioner: [ClassID, PrimaryCode, InstanceID, BU, EntityType, EntityID, ...Chars]
func ClassFirstPartitioner(c NodeCoordinate) []string
```

### 2.2 Pass 1: `Rekey` Function
Construct a new independent store repartitioned along the chosen projection:

```go
// Rekey constructs a new TrieCache partitioned by the given CoordinatePartitioner,
// populating it with all measurements extracted from the source store.
func Rekey(ctx context.Context, partitioner CoordinatePartitioner, src GraphStore) (*TrieCache, error)
```

- **Algorithm:**
  1. Initialize new `PrefixTrieGraphStore` configured with `partitioner`.
  2. Stream / collect all records from `src`.
  3. Insert each measurement into the repartitioned store.
  4. Wrap with `NewTrieCache(nil)` and return.
- **Benefits:**
  - Zero changes to the underlying generic trie node logic.
  - Testable and runnable as standard Go tests without invasive refactoring.
  - Independent stores can be used for ad-hoc fleet diagnostics, sensor recalibration checks, or bulk metric purges.

---

## 3. Pass 2 (Future Optimization): Live Dual-Indexed View
Once `Rekey` is verified, live streaming pipelines can mirror writes to both views via a simple dual-write decorator:

```go
type DualIndexGraphStore struct {
    entityStore  *TrieCache // Entity-First (Hot Ingest / Calculations)
    primaryStore *TrieCache // Primary-First (Sensor Fleet / Bulk Metric Invalidation)
}
```

---

## 4. Test Specifications to Write (TDD)

1. `TestPrefixTrie_PrimaryFirstPartitioner_MUST_order_metric_above_entity`:
   - Validates segment geometry: `[PrimaryCode, ClassID, InstanceID, BU, Type, ID, ...]`.
2. `TestPrefixTrie_Rekey_MUST_project_all_measurements_into_new_partitioning`:
   - Populates Entity-First trie with 10 entities and 5 metrics.
   - Invokes `Rekey(ctx, PrimaryFirstPartitioner, src)`.
   - Verifies all 50 records exist under new metric-first root paths.
3. `TestPrefixTrie_Rekey_InvalidatePrefix_MUST_prune_entire_metric_fleet_in_O1`:
   - In repartitioned primary trie, invalidates `"GRAIN_YIELD"`.
   - Verifies all 10 entities had their `"GRAIN_YIELD"` records pruned in a single operation while `"MOISTURE"` records remain intact.
