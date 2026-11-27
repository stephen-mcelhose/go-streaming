---
title: "CodeArtifact: TrieCache Layered Decorator"
type: "CodeArtifact"
tags:
  - "storage"
  - "cache"
  - "tiered-storage"
  - "decorator"
source_files:
  - "examples/measurement_graph/storage/trie_cache.go"
test_files:
  - "examples/measurement_graph/storage/trie_cache_test.go"
  - "examples/measurement_graph/storage/trie_cache_relate_test.go"
spec_files:
  - ".agents/measurement-streaming-classifier/specs/trie-cache-spec.md"
version: "1.0.0"
status: "Active"
---

# CodeArtifact: TrieCache Layered Decorator

## 1. Component Overview

`TrieCache` decorates any L2 backing `GraphStore` (such as persistent `NativeRelateDBGraphStore` or `MemoryGraphStore`) with an embedded L1 hot `PrefixTrieGraphStore`. It coordinates synchronous write-through, read-through cache population on L2 hits, and dual-layer entity invalidation.

---

## 2. Public API Surface & Complexity

```go
// Constructor
func NewTrieCache(l2 GraphStore) *TrieCache

// GraphStore Implementation
func (c *TrieCache) Put(ctx context.Context, m StoredMeasurement) error
func (c *TrieCache) Get(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error)
func (c *TrieCache) GetByEntity(ctx context.Context, entity EntityIdentity) ([]StoredMeasurement, error)
func (c *TrieCache) GetByClass(ctx context.Context, entity EntityIdentity, classID string) ([]StoredMeasurement, error)
func (c *TrieCache) All(ctx context.Context) ([]StoredMeasurement, error)
func (c *TrieCache) InvalidateEntity(ctx context.Context, entity EntityIdentity) error
func (c *TrieCache) Retract(ctx context.Context, node NodeCoordinate) error
func (c *TrieCache) Close() error
```

---

## 3. Specification & Test Mapping

| Operation | RFC 2119 Specification Requirement | Test Assertion |
| :--- | :--- | :--- |
| `Put` Write-Through | **MUST** write to L1 and propagate synchronously to L2 when present. | `TestTrieCache_Put_Spec/MUST_propagate_write_synchronously_to_L2_when_L2_is_configured` |
| `Put` Error Propagation | **MUST** fail-fast and propagate L2 write errors to caller without corrupting state. | `TestTrieCache_Put_Spec/MUST_fail-fast_and_propagate_L2_write_error_to_caller` |
| `Get` Hot Read | **MUST** serve from L1 hot memory without querying L2 on an L1 hit. | `TestTrieCache_Get_Spec/MUST_serve_from_L1_hot_memory_without_querying_L2_on_L1_hit` |
| `Get` Read-Through | **MUST** fallback to L2 on L1 miss and populate L1 on L2 hit. | `TestTrieCache_Get_Spec/MUST_fallback_to_L2_on_L1_miss_and_populate_L1_on_L2_hit` |
| `InvalidateEntity` | **MUST** prune L1 working memory in $O(1)$ and cascade invalidation to L2. | `TestTrieCache_InvalidationAndClose_Spec/InvalidateEntity_MUST_prune_L1_hot_memory_in_O(1)_and_cascade_to_L2` |
| Live RelateDB C-FFI | **MUST** coordinate Cypher MERGE and query lifecycles over live embedded RelateDB. | `TestTrieCache_LayeredWithRelateDB` |

---

## 4. Related Documents
- **Glossary**: [TrieCache in Glossary](../glossary.md#triecache), [RelateDB](../glossary.md#relatedb)
- **Architecture**: [Two-Tier Storage Architecture](../architecture/two-tier-storage.md)
- **CodeArtifact**: [artifacts/trie-graph-store.md](trie-graph-store.md)
