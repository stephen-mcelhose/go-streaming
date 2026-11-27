# Plan: Detached Subtree Rekeying & Cross-Trie Invalidation

## 1. Objective & Vision
Enable high-efficiency, multi-axis invalidations across different trie projections (e.g. invalidating a metric/sensor in a `PrimaryFirst` trie and reflecting it immediately in an `EntityFirst` trie) without scanning or rekeying the entire graph ($O(N)$).

### Core Principles:
1. **No Synthetic DTOs**: A detached subtree is just a standalone `*PrefixTrieGraphStore` (mini-trie).
2. **$O(1)$ Pointer Snip**: Clipping a branch at arbitrary prefix depth in the source trie immediately isolates the $k$ affected facts.
3. **Targeted $O(k)$ Mini-Rekey**: Only the detached mini-trie ($k$ nodes) is rekeyed into the target partition layout ($k \ll N$), taking microseconds.
4. **Targeted Subtree Pruning**: The target store consumes the rekeyed mini-trie to prune only the matching coordinates in $O(k \cdot \text{depth})$.

```
[ Primary-First Store (N facts) ]
        │
    (DetachPrefix "GRAIN_YIELD:CLASS_1:COMBINE_1") in O(1)
        │
        ▼
[ Detached Mini-Trie ] (k facts, Primary-First layout)
        │
    Rekey(EntityFirstPartitioner, detachedMini) in O(k) (~10μs)
        │
        ▼
[ Rekeyed Mini-Trie ] (k facts, Entity-First layout)
        │
        ▼
[ Entity-First Store ] ──► PruneMatching(rekeyedMini) in O(k * depth)
```

---

## 2. API Design & Domain Contract

### 2.1 `PrefixTrieGraphStore.DetachPrefix`
```go
// DetachPrefix removes the subtree at the specified prefix path from the trie in O(1)
// and returns it as an independent, fully-functional *PrefixTrieGraphStore*.
//
// Invariants:
// - Atomically decrements source store count by subtree size.
// - Returns an independent *PrefixTrieGraphStore configured with the source partitioner.
// - Returns empty store (count 0) if prefix does not exist.
func (t *PrefixTrieGraphStore) DetachPrefix(ctx context.Context, prefix ...string) (*PrefixTrieGraphStore, error)
```

### 2.2 `PrefixTrieGraphStore.PruneMatching`
```go
// PruneMatching removes all coordinates present in otherStore from the receiver store.
// Returns the count of removed measurements.
//
// Invariants:
// - Traverses coordinates in otherStore and executes targeted point retractions on receiver.
// - Atomically decrements receiver count.
// - Does not affect non-matching sibling branches in receiver.
func (t *PrefixTrieGraphStore) PruneMatching(ctx context.Context, otherStore GraphStore) (int, error)
```

---

## 3. Protocol & Execution Lifecycle

### Step 1: Detach on Source Projection ($O(1)$ pointer detachment)
1. Acquire write lock on source prefix shard.
2. Locate target prefix node and detach pointer from parent.
3. Calculate subtree count $k$, subtract $k$ from source store `atomic.Int64 count`.
4. Wrap detached root into new `*PrefixTrieGraphStore` with count $k$ and return.

### Step 2: Mini-`Rekey` ($O(k)$ re-projection)
1. Call `Rekey(ctx, TargetPartitioner, detachedMiniStore)`.
2. Re-indexes only $k$ nodes into target segment order.

### Step 3: Targeted Prune on Destination Store ($O(k \cdot \text{depth})$)
1. Call `targetStore.PruneMatching(ctx, rekeyedMiniStore)`.
2. Removes all matching $k$ coordinates from the destination store.

---

## 4. TDD Gates (Specs to Write First)

In `examples/measurement_graph/storage/cross_invalidation_spec_test.go`:

1. **`TestPrefixTrie_DetachPrefix_Spec`**:
   - `MUST detach matching subtree in O(1) and return valid standalone PrefixTrieGraphStore`.
   - `MUST decrement source store count by exact detached subtree count`.
   - `MUST return empty store and zero count when prefix is not found`.
2. **`TestPrefixTrie_PruneMatching_Spec`**:
   - `MUST remove all coordinates present in other store and leave non-matching untouched`.
   - `MUST return count of removed items and update receiver count`.
3. **`TestPrefixTrie_CrossTrieInvalidation_Integration_Spec`**:
   - Seed `entityStore` and `primaryStore` with 50 entities $\times$ 5 metrics ($N = 250$).
   - Detach a single sensor fleet `[Primary=GRAIN_YIELD, Class=CLASS_YIELD, Instance=COMBINE_1]` from `primaryStore` ($k = 50$).
   - Mini-rekey detached store to `EntityFirstPartitioner`.
   - Prune from `entityStore`.
   - Verify `primaryStore` and `entityStore` both have exactly $N - k = 200$ items left and zero traces of `COMBINE_1`.

---

## 5. Work Breakdown & Implementation Steps
1. **TDD Step 1**: Write test specifications in `storage/cross_invalidation_spec_test.go`.
2. **Type-First Compilation Gate**: Stub `DetachPrefix` and `PruneMatching` on `PrefixTrieGraphStore` and verify `go build ./...` compiles.
3. **Leaf Implementations**:
   - Implement `DetachPrefix` in `storage/trie_store.go`.
   - Implement `PruneMatching` in `storage/trie_store.go` and `storage/trie_cache.go`.
4. **Verification**: Run complete test suite and benchmark cross-invalidation performance.
