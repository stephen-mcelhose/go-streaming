---
title: "CodeArtifact: DualIndexStore (Single-Owner Affinity Coordinator)"
type: "CodeArtifact"
tags:
  - "code-artifact"
  - "storage"
  - "dual-index"
  - "affinity-sharding"
  - "adr-0004"
  - "concurrency"
version: "1.0.0"
status: "Active"
source_files:
  - "examples/measurement_graph/storage/dual_index_store.go"
test_files:
  - "examples/measurement_graph/storage/dual_index_spec_test.go"
spec_files:
  - "examples/measurement_graph/docs/architecture/affinity-sharded-dual-trie.md"
---

# CodeArtifact: DualIndexStore (Single-Owner Affinity Coordinator)

## 1. Architectural Boundary & Intent

`DualIndexStore` (Layer 2.5 / Sharded Coordinator) implements **ADR-0004: Single-Owner Affinity Sharding**. It wraps $M$ independent `DualIndexShard` instances, each owning a single `sync.RWMutex` protecting two internal un-synchronized prefix-tries:
- `TrieByEntity`: Entity-First partitioner (`BU -> EntityType -> EntityID -> Primary -> Class -> Instance`).
- `TrieByPrimary`: Primary-First partitioner (`Primary -> Class -> Instance -> BU -> EntityType -> EntityID`).

---

## 2. Public API Surface

```go
type DualIndexStore struct { /* unexported fields */ }

func NewDualIndexStore() *DualIndexStore
func NewDualIndexStoreWithShards(shards int) *DualIndexStore

// GraphStore Interface Implementation
func (d *DualIndexStore) Put(ctx context.Context, m StoredMeasurement) error
func (d *DualIndexStore) Get(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error)
func (d *DualIndexStore) GetByEntity(ctx context.Context, entity EntityIdentity) ([]StoredMeasurement, error)
func (d *DualIndexStore) GetByClass(ctx context.Context, entity EntityIdentity, classID string) ([]StoredMeasurement, error)
func (d *DualIndexStore) InvalidateEntity(ctx context.Context, entity EntityIdentity) error
func (d *DualIndexStore) Retract(ctx context.Context, node NodeCoordinate) error
func (d *DualIndexStore) InvalidatePrefix(ctx context.Context, prefix ...string) error
func (d *DualIndexStore) All(ctx context.Context) ([]StoredMeasurement, error)
func (d *DualIndexStore) Count(ctx context.Context) (int, error)
func (d *DualIndexStore) Close() error

// Extended Fleet Operations
func (d *DualIndexStore) InvalidatePrefixCount(ctx context.Context, prefix ...string) (int, error)
func (d *DualIndexStore) ShardCount() int
func (d *DualIndexStore) GetPrimaryAxis(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error)
```

---

## 3. Structural Mechanics & Invariants

1. **Entity-Affinity Routing ($O(1)$ Hash)**:
   - All point and entity operations route by $\text{fnv32a}(\text{EntityIdentity}) \ \& \ (M - 1)$ directly to a single `DualIndexShard`.
2. **Single-Owner Mutex Isolation (Zero AB-BA Deadlocks)**:
   - A write thread acquires only the single target shard lock, updating both internal tries atomically without cross-shard lock acquisitions.
3. **Metric Hotspot Immunity**:
   - Observations for `GRAIN_YIELD` across distinct entities distribute evenly across all $M$ shards, eliminating the metric root lock bottleneck.
4. **Localized Micro-Snip Fleet Revocation ($O(k / M)$)**:
   - Fleet invalidations fan out across all $M$ shards. Each shard performs an $O(1)$ `DetachPrefix` on `TrieByPrimary` (isolating $k_i \approx k / M$ items) and $O(k_i \cdot \text{depth})$ `PruneMatching` on `TrieByEntity`.

---

## 4. Requirement-to-Test Mapping (RFC 2119)

| Requirement / Invariant | Test Identifier |
| :--- | :--- |
| **MUST** atomically place observations across both internal tries | `TestDualIndexStore_Spec/MUST_atomically_place_observations...` |
| **MUST** invalidate entire sensor fleet via localized micro-snip | `TestDualIndexStore_Spec/MUST_invalidate_entire_sensor_fleet...` |
| **MUST** invalidate entity working memory within owning shard | `TestDualIndexStore_Spec/MUST_invalidate_entity_working_memory...` |
| **MUST** execute 64 concurrent workers without data races or deadlocks | `TestDualIndexStore_Spec/MUST_execute_64_concurrent_workers...` |

---

## 5. Related Documents
- **Glossary**: [Striped Locking](../glossary.md#striped-locking), [CoordinatePartitioner](../glossary.md#coordinatepartitioner)
- **Architecture**: [Single-Owner Affinity Sharding & Dual-Trie Coordinator (ADR-0004)](../architecture/affinity-sharded-dual-trie.md)
- **CodeArtifact**: [artifacts/trie-graph-store.md](trie-graph-store.md), [artifacts/trie-cache.md](trie-cache.md)
