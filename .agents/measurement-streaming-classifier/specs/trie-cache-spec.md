# TrieCache Layered Graph Store Specification

`TrieCache` is a write-through, read-through caching decorator over any backing `GraphStore` (L2). It uses an inverted `PrefixTrieGraphStore` (L1) as a low-latency, thread-safe memory index while keeping L2 in sync.

---

## 1. Domain Types & Roles

| Component | Role | Complexity |
| :--- | :--- | :--- |
| **L1: `PrefixTrieGraphStore`** | Hot memory index structured by inverted coordinate segments: `[BU, EntityType, EntityID, PrimaryCode, ClassID, InstanceID, ...Characteristics]`. | $O(\text{depth})$ path navigation, $O(1)$ entity tree pruning. |
| **L2: `GraphStore`** | Persistent or cold backing store (e.g. `NativeRelateDBGraphStore`, external KV, remote service). | Persistent I/O or Cypher graph queries. |
| **`TrieCache`** | Layered facade implementing `GraphStore`. Coordinates L1 hits/misses, synchronous L2 write propagation, and dual-layer invalidation. | Transparent drop-in for `GraphStore`. |

---

## 2. Invariants & Strict Behavior Contracts

### 2.1 Read Contract (Read-Through)

For all read operations (`Get`, `GetByEntity`, `GetByClass`):

1. **L1 Hot Lookup:**
   - Query L1 `PrefixTrieGraphStore`.
   - If found (or non-empty slice for subtree queries), return immediately with zero L2 overhead.
2. **L2 Backing Fallback:**
   - If L1 misses (or returns empty slice) AND L2 is configured:
     - Query L2 backing store.
     - If found: synchronously populate L1 `PrefixTrieGraphStore` with the retrieved record(s).
     - Return the retrieved record(s) and error from L2.
3. **Nil L2 Store:**
   - If L2 is `nil`, misses in L1 immediately return `(zero, false, nil)` or `(nil, nil)`.
4. **Context Propagation:**
   - If `ctx.Err() != nil` at entry or during L2 retrieval, return immediately with `ctx.Err()`.

```
[Read: Get(coord)]
       │
   L1 Lookup
    ├── Found? ───► (Return L1 Value)
    └── Miss
          │
      L2 Backing?
       ├── Nil ───► (Return Not Found)
       └── Exists
             │
         L2 Lookup
          ├── Not Found ──► (Return Not Found)
          └── Found ──────► L1.Put(coord, val) ──► (Return Value)
```

---

### 2.2 Write Contract (Synchronous Write-Through)

For all insert/update operations (`Put`):

1. **L1 Update:**
   - Synchronously write to L1 `PrefixTrieGraphStore`.
   - If L1 write fails, return error immediately; do NOT invoke L2.
2. **L2 Synchronous Propagation:**
   - If L2 is configured, invoke `L2.Put(ctx, m)`.
   - If L2 write fails, return error to caller. (Caller knows persistence failed).
3. **Nil L2:**
   - If L2 is `nil`, succeed after L1 write.

```
[Write: Put(coord, val)]
       │
   L1.Put(coord, val)
    ├── Error ───► (Return Error, L2 untouched)
    └── Success
          │
      L2 Backing?
       ├── Nil ───► (Return Nil)
       └── Exists
             │
         L2.Put(coord, val)
          ├── Error ───► (Return Error)
          └── Success ─► (Return Nil)
```

---

### 2.3 Pruning & Eviction Contract (Dual Invalidation)

For retraction and entity-level pruning:

1. **`InvalidateEntity(ctx, entity)`:**
   - Prune the entire entity subtree in L1 via $O(1)$ root-pointer detachment.
   - If L2 is configured, synchronously invoke `L2.InvalidateEntity(ctx, entity)`.
   - If L2 returns error, propagate error upward.
2. **`Retract(ctx, node)`:**
   - Remove the specific coordinate from L1.
   - If L2 is configured, remove the coordinate from L2.
3. **`Count(ctx)`:**
   - Returns the active placed node count in L1 hot memory.
4. **`Close()`:**
   - Cleans up L1 trie pointers and closes L2 backing store if `io.Closer` / `GraphStore.Close()` is implemented.

---

## 3. Method Behavior Matrix

| Method | L1 State | L2 Backing Present? | Action Taken | Result |
| :--- | :--- | :--- | :--- | :--- |
| `Get(node)` | Hit | N/A | Return from L1. | `(StoredMeasurement, true, nil)` |
| `Get(node)` | Miss | No (`l2 == nil`) | None. | `(StoredMeasurement{}, false, nil)` |
| `Get(node)` | Miss | Yes (`l2 != nil`) | Query L2 $\to$ If found, `L1.Put(m)` $\to$ Return. | `(StoredMeasurement, found, err)` |
| `GetByEntity(e)` | Hit (`len > 0`) | N/A | Return from L1. | `([]StoredMeasurement, nil)` |
| `GetByEntity(e)` | Miss (`len == 0`) | Yes (`l2 != nil`) | Query L2 $\to$ populate all into L1 $\to$ Return. | `([]StoredMeasurement, err)` |
| `GetByClass(e, c)` | Hit (`len > 0`) | N/A | Return from L1. | `([]StoredMeasurement, nil)` |
| `GetByClass(e, c)` | Miss (`len == 0`) | Yes (`l2 != nil`) | Query L2 $\to$ populate all into L1 $\to$ Return. | `([]StoredMeasurement, err)` |
| `Put(m)` | N/A | No | `L1.Put(m)` | `nil` or L1 err |
| `Put(m)` | N/A | Yes | `L1.Put(m)` $\to$ `L2.Put(m)` | `nil` or L2 err |
| `InvalidateEntity(e)`| N/A | Yes | `L1.Invalidate(e)` $\to$ `L2.Invalidate(e)` | `nil` or L2 err |
| `Retract(node)` | N/A | Yes | `L1.Retract(node)` $\to$ `L2.Retract(node)` | `nil` or L2 err |
| `Count()` | N/A | N/A | Return `L1.Count()` | `(int, nil)` |
| `Close()` | N/A | Yes | `L1.Close()` $\to$ `L2.Close()` | `nil` or L2 err |
