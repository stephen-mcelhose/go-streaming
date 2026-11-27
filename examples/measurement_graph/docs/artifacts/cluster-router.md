---
title: "CodeArtifact: ClusterRouter & LocalClusterNode (Distributed Coordinator)"
type: "CodeArtifact"
tags:
  - "code-artifact"
  - "storage"
  - "distributed"
  - "cluster-router"
  - "entity-affinity"
  - "scatter-gather"
version: "1.0.0"
status: "Active"
source_files:
  - "examples/measurement_graph/storage/cluster_store.go"
test_files:
  - "examples/measurement_graph/storage/cluster_store_spec_test.go"
spec_files:
  - "examples/measurement_graph/docs/architecture/affinity-sharded-dual-trie.md"
---

# CodeArtifact: ClusterRouter & LocalClusterNode (Distributed Coordinator)

## 1. Architectural Boundary & Intent

`ClusterRouter` (Layer 2.7 / Distributed Sharding Gateway) distributes $M$ affinity shards across $K$ independent worker nodes (`ClusterNode`). It guarantees that:
- All observations, working memory, and calculations for an `EntityIdentity` reside on **exactly one designated node**.
- Ingest and entity-scoped reads route in $O(1)$ across network/IPC without touching non-owning nodes.
- Fleet-wide operations (`InvalidatePrefix`, `All`, `Count`) execute parallel **scatter-gather** fanouts across all cluster nodes.

---

## 2. Public API Surface

```go
type ClusterNode interface {
    GraphStore
    NodeID() string
    AssignedShards() []int
    InvalidatePrefix(ctx context.Context, prefix ...string) error
}

type LocalClusterNode struct { /* unexported fields */ }
func NewLocalClusterNode(nodeID string, totalShards int, shardIndices []int) *LocalClusterNode

type ClusterRouter struct { /* unexported fields */ }
func NewClusterRouter(totalShards int, nodes []ClusterNode) (*ClusterRouter, error)

// GraphStore Interface Implementation
func (r *ClusterRouter) Put(ctx context.Context, m StoredMeasurement) error
func (r *ClusterRouter) Get(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error)
func (r *ClusterRouter) GetByEntity(ctx context.Context, entity EntityIdentity) ([]StoredMeasurement, error)
func (r *ClusterRouter) GetByClass(ctx context.Context, entity EntityIdentity, classID string) ([]StoredMeasurement, error)
func (r *ClusterRouter) InvalidateEntity(ctx context.Context, entity EntityIdentity) error
func (r *ClusterRouter) Retract(ctx context.Context, node NodeCoordinate) error
func (r *ClusterRouter) InvalidatePrefix(ctx context.Context, prefix ...string) error
func (r *ClusterRouter) All(ctx context.Context) ([]StoredMeasurement, error)
func (r *ClusterRouter) Count(ctx context.Context) (int, error)
func (r *ClusterRouter) Close() error
```

---

## 3. Structural Mechanics & Invariants

1. **Entity Affinity Hashing**:
   - `nodeForEntity` computes `fnv32a(EntityIdentity) % totalShards`, routing requests in $O(1)$ strictly to the node hosting that shard.
2. **Scatter-Gather Parallel Fanout**:
   - `InvalidatePrefix` issues concurrent goroutines to all registered `ClusterNode` instances, synchronizing via `sync.WaitGroup` and aggregating errors.
3. **Zero Cross-Node Join Latency**:
   - Because all metric classes for an entity route to the same physical node, multi-slot calculation joins (Rete Beta network) execute locally on that node with zero network hops.

---

## 4. Requirement-to-Test Mapping (RFC 2119)

| Requirement / Invariant | Test Identifier |
| :--- | :--- |
| **MUST** route entity observations strictly to owning cluster node in $O(1)$ | `TestClusterRouter_Spec/MUST_route_entity_observations...` |
| **MUST** execute parallel scatter-gather fleet invalidations across all nodes | `TestClusterRouter_Spec/MUST_execute_parallel_scatter-gather...` |
| **MUST** invalidate entity strictly within owning node | `TestClusterRouter_Spec/MUST_invalidate_entity_strictly...` |
| **MUST** support multi-node concurrent ingestion and fleet pruning under race detector | `TestClusterRouter_Spec/MUST_support_multi-node_concurrent...` |

---

## 5. Related Documents
- **Glossary**: [ClusterRouter](../glossary.md#clusterrouter), [Entity Affinity Sharding](../glossary.md#affinity-sharding)
- **Architecture**: [Single-Owner Affinity Sharding & Dual-Trie Coordinator (ADR-0004)](../architecture/affinity-sharded-dual-trie.md)
- **CodeArtifact**: [artifacts/dual-index-store.md](dual-index-store.md)
