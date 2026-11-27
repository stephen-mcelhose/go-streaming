---
title: "CodeArtifact: PrefixTrieGraphStore & Rekey"
type: "CodeArtifact"
tags:
  - "storage"
  - "trie"
  - "partitioning"
  - "rekey"
source_files:
  - "examples/measurement_graph/storage/trie_store.go"
  - "examples/measurement_graph/storage/rekey.go"
test_files:
  - "examples/measurement_graph/storage/trie_store_spec_test.go"
  - "examples/measurement_graph/storage/rekey_spec_test.go"
  - "examples/measurement_graph/storage/cross_invalidation_spec_test.go"
spec_files:
  - ".agents/measurement-streaming-classifier/specs/rekey-and-partitioner-spec.md"
  - ".agents/measurement-streaming-classifier/specs/detached-subtree-invalidation-spec.md"
version: "1.0.0"
status: "Active"
---

# CodeArtifact: PrefixTrieGraphStore & Rekey

## 1. Component Overview

`PrefixTrieGraphStore` is a domain adapter implementing `GraphStore` over a generic `PrefixTrie[string, StoredMeasurement]`. It maps multi-dimensional `NodeCoordinate`s into ordered path segments using pluggable `CoordinatePartitioner` functions and provides $O(1)$ prefix invalidation, detached mini-trie extraction, and targeted point pruning.

---

## 2. Public API Surface & Complexity

```go
// Constructors
func NewPrefixTrieGraphStore() *PrefixTrieGraphStore
func NewPrefixTrieGraphStoreWithShards(shards int) *PrefixTrieGraphStore
func NewPrefixTrieGraphStoreWithPartitioner(p CoordinatePartitioner, shards int) *PrefixTrieGraphStore

// GraphStore Implementation
func (t *PrefixTrieGraphStore) Put(ctx context.Context, m StoredMeasurement) error
func (t *PrefixTrieGraphStore) Get(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error)
func (t *PrefixTrieGraphStore) GetByEntity(ctx context.Context, entity EntityIdentity) ([]StoredMeasurement, error)
func (t *PrefixTrieGraphStore) GetByClass(ctx context.Context, entity EntityIdentity, classID string) ([]StoredMeasurement, error)
func (t *PrefixTrieGraphStore) InvalidateEntity(ctx context.Context, entity EntityIdentity) error
func (t *PrefixTrieGraphStore) Retract(ctx context.Context, node NodeCoordinate) error
func (t *PrefixTrieGraphStore) All(ctx context.Context) ([]StoredMeasurement, error)
func (t *PrefixTrieGraphStore) Count(ctx context.Context) (int, error)
func (t *PrefixTrieGraphStore) Close() error

// Trie-Specific & Cross-Trie Invalidation Operations
func (t *PrefixTrieGraphStore) InvalidatePrefix(ctx context.Context, prefix ...string) error
func (t *PrefixTrieGraphStore) DetachPrefix(ctx context.Context, prefix ...string) (*PrefixTrieGraphStore, error)
func (t *PrefixTrieGraphStore) PruneMatching(ctx context.Context, other GraphStore) (int, error)
func (t *PrefixTrieGraphStore) ShardCount() int

// Partitioners & Rekeying
type CoordinatePartitioner func(coord NodeCoordinate) []string
func EntityFirstPartitioner(c NodeCoordinate) []string
func PrimaryFirstPartitioner(c NodeCoordinate) []string
func ClassFirstPartitioner(c NodeCoordinate) []string
func Rekey(ctx context.Context, partitioner CoordinatePartitioner, src GraphStore) (*TrieCache, error)
```

---

## 3. Specification & Test Mapping

| Operation | RFC 2119 Specification Requirement | Test Assertion |
| :--- | :--- | :--- |
| Coordinate Partitioning | **MUST** order coordinate segments deterministically per partitioner contract. | `TestCoordinatePartitioners_Spec` |
| `Rekey` Projection | **MUST** project all source measurements into rekeyed layout with 100% data parity. | `TestRekey_Spec/Rekey_MUST_project_all_measurements_into_repartitioned_store` |
| `Rekey` Reversibility | **MUST** be fully reversible across multiple repartitioning cycles (lossless isomorphism). | `TestRekey_Spec/Rekey_MUST_be_fully_reversible_across_multiple_repartitioning_cycles_(lossless_isomorphism)` |
| `DetachPrefix` | **MUST** clip subtree in $O(1)$ and return a valid standalone `PrefixTrieGraphStore`. | `TestPrefixTrie_DetachPrefix_Spec` |
| `PruneMatching` | **MUST** remove coordinates present in other store and leave non-matching untouched. | `TestPrefixTrie_PruneMatching_Spec` |
| Cross-Trie Invalidation | **MUST** propagate primary sensor revocation to entity store without full-table scans. | `TestPrefixTrie_CrossTrieInvalidation_Integration_Spec` |

---

## 4. Related Documents
- **Glossary**: [PrefixTrieGraphStore](../glossary.md#prefixtriegraphstore), [Rekey](../glossary.md#rekey)
- **Concept**: [Inverted Prefix Trie](../concepts/inverted-prefix-trie.md)
- **Generic Data Structure**: [artifacts/generic-prefix-trie.md](generic-prefix-trie.md)
