# Specification: Trie Repartitioning & Prefix Invalidation

`Rekey` projects measurements from any `GraphStore` into a repartitioned `TrieCache` (or `PrefixTrieGraphStore`) using pluggable `CoordinatePartitioner` functions. This enables arbitrary root dimensioning and $O(1)$ prefix subtree pruning across ontology boundaries.

---

## 1. Domain Types & Partitioner Contracts

### 1.1 Pluggable Partitioner Signature
```go
// CoordinatePartitioner maps a NodeCoordinate into ordered trie path segments.
type CoordinatePartitioner func(coord NodeCoordinate) []string
```

### 1.2 Standard Built-in Partitioners

| Partitioner | Path Hierarchy | Primary Optimization |
| :--- | :--- | :--- |
| **`EntityFirstPartitioner`** (Default) | `[BU, EntityType, EntityID, PrimaryCode, ClassID, InstanceID, ...Characteristics]` | Entity-centric working memory, calculation slot binding, entity window invalidation. |
| **`PrimaryFirstPartitioner`** | `[PrimaryCode, ClassID, InstanceID, BU, EntityType, EntityID, ...Characteristics]` | Metric-centric fleet monitoring, bulk sensor recalibration, $O(1)$ metric invalidation. |
| **`ClassFirstPartitioner`** | `[ClassID, PrimaryCode, InstanceID, BU, EntityType, EntityID, ...Characteristics]` | Ontology class exploration, schema-wide query filters. |

---

## 2. Invariants & Strict Behavior (RFC 2119)

### 2.1 Coordinate Partitioning Contract
- **MUST** preserve all characteristics in deterministic sorted order (`Key=Value`) as leaf segments regardless of partitioner.
- **MUST** map every unique `NodeCoordinate` to a unique trie leaf path.

### 2.2 `Rekey` Projection & Reversibility Contract
- **MUST** populate the new trie with all records retrieved via `src.All(ctx)`.
- **MUST** support point lookups (`Get`) returning identical values under the new coordinate path indexing.
- **MUST** be fully reversible across arbitrary repartitioning cycles (lossless isomorphism): `EntityFirst -> PrimaryFirst -> ClassFirst -> EntityFirst` retains 100% data integrity with identical counts and coordinate values.
- **MUST** return an error if `src` is `nil` or if reading `src.All(ctx)` fails.

### 2.3 `InvalidatePrefix` Invalidation Contract
- **MUST** prune all descendant nodes under any prefix of arbitrary depth (`depth >= 1`) in $O(1)$ pointer operations.
  - `InvalidatePrefix("GRAIN_YIELD")` prunes all entity measurements for that metric across the entire fleet in $O(1)$.
  - `InvalidatePrefix("GRAIN_YIELD", "CLASS_GRAIN_YIELD")` prunes that class tier across all entities in $O(1)$.
  - `InvalidatePrefix("GRAIN_YIELD", "CLASS_GRAIN_YIELD", "COMBINE_1")` prunes that sensor instance fleet in $O(1)$.
- **MUST** atomically decrement `count` by the exact number of pruned descendant leaves.
- **MUST NOT** affect or mutate sibling branches outside the specified prefix.
