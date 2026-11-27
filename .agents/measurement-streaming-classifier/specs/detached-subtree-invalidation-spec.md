# Specification: Detached Subtree Rekeying & Cross-Trie Invalidation

`DetachPrefix` and `PruneMatching` enable cross-trie invalidations between different coordinate projections without scanning or rekeying the full graph ($O(N)$).

---

## 1. Domain Types & Signatures

```go
// DetachPrefix removes the subtree at the specified prefix path from the trie in O(1)
// and returns it as an independent, fully-functional *PrefixTrieGraphStore*.
func (t *PrefixTrieGraphStore) DetachPrefix(ctx context.Context, prefix ...string) (*PrefixTrieGraphStore, error)

// PruneMatching removes all coordinates present in otherStore from the receiver store.
// Returns the count of removed measurements.
func (t *PrefixTrieGraphStore) PruneMatching(ctx context.Context, otherStore GraphStore) (int, error)
```

---

## 2. Invariants & Strict Behavior (RFC 2119)

### 2.1 `DetachPrefix` Contract
- **MUST** clip the matching prefix subtree in $O(1)$ pointer operations.
- **MUST** atomically decrement the source store's active count by the exact number of detached descendant leaves.
- **MUST** return an independent `*PrefixTrieGraphStore` containing only the detached subtree with its count initialized to the subtree size.
- **MUST** return an empty store (count 0) and nil error if the prefix does not exist in the source trie.

### 2.2 `PruneMatching` Contract
- **MUST** remove all coordinates found in `otherStore` from the receiver store.
- **MUST** leave all non-matching sibling branches untouched.
- **MUST** return the exact count of coordinates actually removed from the receiver store.
- **MUST** return 0 and nil error if `otherStore` is nil or empty.

### 2.3 Cross-Trie Invalidation Lifecycle
- When invalidating along an alternate axis (e.g. revoking a sensor fleet `[Primary, Class, Instance]` from `primaryStore` and pruning it from `entityStore`):
  1. `detachedMini, err := primaryStore.DetachPrefix(ctx, primary, class, instance)` ($O(1)$)
  2. `rekeyedMini, err := Rekey(ctx, EntityFirstPartitioner, detachedMini)` ($O(k)$ where $k \ll N$)
  3. `prunedCount, err := entityStore.PruneMatching(ctx, rekeyedMini)` ($O(k \cdot \text{depth})$)
- **MUST** ensure both stores end in identical states with $N - k$ active records and 0 remaining traces of the revoked fleet.
