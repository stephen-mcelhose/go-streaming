# Striped Locking Architecture for Inverted Prefix Trie

## 1. Problem & Goal
- **Current State:** Coarse global `sync.RWMutex` locks the entire trie for every read/write/prune. All concurrent entities serialize on writes.
- **Target State:** Striped/sharded lock ring partitioned by `EntityIdentity`. Concurrent writes and reads to distinct entities execute in parallel with near-zero contention.
- **Configurability:** Provide `NewPrefixTrieGraphStoreWithShards(shards int)` with a sensible default (`DefaultTrieShards = 256` or 0/1 fallback to unstriped).

---

## 2. Architecture & Locking Geometry

### 2.1 The Two Lock Boundaries
1. **Root Hierarchy Lock (`metaMu sync.RWMutex`):**
   - Guards top levels: `Root -> BusinessUnit -> EntityType`.
   - Read-locked (`RLock`) during standard operations to traverse down to the `EntityNode`.
   - Write-locked (`Lock`) only when registering a new `BusinessUnit` or `EntityType` branch (which happens only on first encounter).
2. **Entity Lock Stripe (`shards []sync.RWMutex`):**
   - Size: $N$ power-of-two (default 256).
   - Selection: `shardIdx = fnv32(entity.String()) & (N - 1)`.
   - Scope: Guards everything below `EntityNode` (`PrimaryCode -> ClassID -> InstanceID -> Characteristics`), subtree traversals, and entity invalidation.
3. **Global Operations (`Count`, `Close`):**
   - Atomically read/write `count` with `sync/atomic` (e.g. `atomic.Int64`).
   - `Close()` acquires write locks across all shards and `metaMu`.

```
                    Root (metaMu)
                   /             \
            Agronomy             Cardiology
             /     \                 |
         PLOT       FIELD         PATIENT
          |           |              |
    [Plot_1]       [Field_9]    [Patient_42]
   (Shard 12)     (Shard 88)     (Shard 12)
        |              |              |
   (Subtree)      (Subtree)      (Subtree)
```

---

## 3. Implementation Steps

1. **Hash Utility (`fnv32`):**
   - Inline non-allocating FNV-1a hash for `EntityIdentity.String()`.
2. **Atomic Counter:**
   - Replace `t.count int` with `atomic.Int64 count` to avoid locking all shards on size queries.
3. **`PrefixTrieGraphStore` Struct Refactoring:**
   ```go
   const DefaultTrieShards = 256

   type PrefixTrieGraphStore struct {
       metaMu   sync.RWMutex
       shards   []sync.RWMutex
       mask     uint32
       root     *trieNode
       count    atomic.Int64
   }
   ```
4. **Constructors:**
   - `NewPrefixTrieGraphStore()`: Defaults to 256 shards.
   - `NewPrefixTrieGraphStoreWithShards(n int)`: Normalizes to nearest power of 2. If $n \le 1$, falls back to 1 shard (single lock behavior).
5. **Methods Update:**
   - `Put`: `metaMu.RLock()` to get/create BU/EntityType $\to$ lock shard for Entity $\to$ traverse/insert into entity subtree $\to$ `count.Add(1)`.
   - `Get` / `GetByEntity` / `GetByClass`: `metaMu.RLock()` $\to$ `shard.RLock()` $\to$ query entity subtree.
   - `InvalidateEntity`: `metaMu.RLock()` $\to$ `shard.Lock()` $\to$ detach entity node $\to$ `count.Add(-deleted)`.
   - `Count`: Return `int(t.count.Load())`.
   - `Close`: Acquire all shard write locks + `metaMu.Lock()`, reset root, reset count.

---

## 4. Test Specifications to Write First (TDD)

In `storage/trie_store_test.go`:
1. `TestPrefixTrie_ShardingConfig_MUST_default_to_256_and_allow_custom_sizes`:
   - Validates shard count power-of-two rounding and single-lock fallback.
2. `TestPrefixTrie_ConcurrentDisjointEntities_MUST_execute_in_parallel_without_blocking`:
   - Spawns $G = 64$ goroutines writing to distinct entities simultaneously.
   - Verifies throughput and zero deadlocks.
3. `TestPrefixTrie_StripedInvalidation_MUST_not_stall_other_shards`:
   - Continuous high-throughput writes to Entity A while concurrently invalidating Entity B.
4. `TestPrefixTrie_GlobalCount_MUST_remain_consistent_under_concurrent_writes_and_invalidations`:
   - Cross-thread atomic verification.
