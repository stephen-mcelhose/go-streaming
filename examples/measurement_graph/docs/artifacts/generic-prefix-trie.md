---
title: "CodeArtifact: PrefixTrie[K, V]"
type: "CodeArtifact"
tags:
  - "storage"
  - "data-structure"
  - "generic"
  - "concurrency"
source_files:
  - "examples/measurement_graph/storage/generic_trie.go"
test_files:
  - "examples/measurement_graph/storage/generic_trie_test.go"
spec_files:
  - ".agents/measurement-streaming-classifier/specs/rekey-and-partitioner-spec.md"
  - ".agents/measurement-streaming-classifier/specs/detached-subtree-invalidation-spec.md"
version: "1.0.0"
status: "Active"
---

# CodeArtifact: PrefixTrie[K, V]

## 1. Component Overview

`PrefixTrie[K comparable, V any]` is a pure generic, thread-safe prefix trie data structure. It manages arbitrary key segment slices (`[]K`) and values (`V`) using a striped lock ring for high concurrency, non-allocating FNV-1a path hashing, and lock-free atomic counting.

---

## 2. Public API Surface & Complexity

```go
// Constructors
func NewGenericPrefixTrie[K comparable, V any]() *PrefixTrie[K, V]
func NewGenericPrefixTrieWithShards[K comparable, V any](shards int) *PrefixTrie[K, V]

// Core Operations
func (t *PrefixTrie[K, V]) Put(path []K, value V)                           // O(depth)
func (t *PrefixTrie[K, V]) Get(path []K) (V, bool)                          // O(depth)
func (t *PrefixTrie[K, V]) CollectPrefix(prefix ...K) []V                   // O(k)
func (t *PrefixTrie[K, V]) CollectAll() []V                                 // O(N)
func (t *PrefixTrie[K, V]) InvalidatePrefix(prefix ...K) int                // O(1) pointer snip
func (t *PrefixTrie[K, V]) DetachPrefix(prefix ...K) *PrefixTrie[K, V]      // O(1) pointer snip
func (t *PrefixTrie[K, V]) Delete(path []K) bool                            // O(depth)
func (t *PrefixTrie[K, V]) Count() int                                      // O(1) lock-free atomic
func (t *PrefixTrie[K, V]) ShardCount() int                                 // O(1)
func (t *PrefixTrie[K, V]) Close()                                          // O(1)
```

---

## 3. Specification & Test Mapping

| Method | RFC 2119 Specification Requirement | Test Assertion |
| :--- | :--- | :--- |
| `Put` / `Get` | **MUST** store and retrieve generic types at arbitrary key paths. | `TestGenericPrefixTrie_Spec/MUST_put_and_get_generic_types_at_arbitrary_string_paths` |
| `CollectPrefix` | **MUST** return all values residing in the prefix subtree. | `TestGenericPrefixTrie_Spec/MUST_collect_values_under_prefix` |
| `InvalidatePrefix` | **MUST** prune prefix subtree in $O(1)$ and decrement atomic count. | `TestGenericPrefixTrie_Spec/MUST_invalidate_prefix_subtree_in_O(1)` |
| `DetachPrefix` | **MUST** detach subtree in $O(1)$ into a standalone `PrefixTrie`. | `TestGenericPrefixTrie_Spec/MUST_detach_prefix_subtree_into_standalone_generic_trie_in_O(1)` |
| Concurrency | **MUST** execute concurrent reads and writes without data races. | `TestGenericPrefixTrie_Spec/MUST_execute_concurrent_writes_and_reads_without_data_races` |

---

## 4. Related Documents
- **Glossary**: [PrefixTrie in Glossary](../glossary.md#prefixtrie)
- **Concept**: [Inverted Prefix Trie](../concepts/inverted-prefix-trie.md)
- **Adapter Artifact**: [artifacts/trie-graph-store.md](trie-graph-store.md)
