---
title: "CodeArtifact: SpannerGraphStore (Distributed GQL Persistence Layer)"
type: "CodeArtifact"
tags:
  - "code-artifact"
  - "storage"
  - "spanner-graph"
  - "gql"
  - "distributed"
  - "l2-persistence"
version: "1.0.0"
status: "Active"
source_files:
  - "examples/measurement_graph/storage/spanner_graph_store.go"
test_files:
  - "examples/measurement_graph/storage/spanner_graph_spec_test.go"
spec_files:
  - "examples/measurement_graph/docs/architecture/backing-store-evaluation.md"
---

# CodeArtifact: SpannerGraphStore (Distributed GQL Persistence Layer)

## 1. Architectural Boundary & Intent

`SpannerGraphStore` (Layer 3.5 / Distributed Cloud L2 Persistence) adapts **Google Cloud Spanner Graph** using standard **ISO Graph Query Language (GQL)** queries to implement the `GraphStore` contract.

It represents the streaming ontology hierarchy as a property graph:
```
(Entity:Entity {key, bu, type, id})
       │
   [:OBSERVED]
       ▼
(Observation:Observation {nodeKey, primaryCode, classID, instanceID, value, timestamp})
       │
 [:INSTANCE_OF]
       ▼
(Instance:Instance {id})
       │
  [:MEMBER_OF]
       ▼
(Class:Class {id})
```

---

## 2. Public API Surface

```go
type SpannerGraphClient interface {
    ExecuteQuery(ctx context.Context, statement string, params map[string]any) ([]map[string]any, error)
    ExecuteMutation(ctx context.Context, statement string, params map[string]any) (int64, error)
    Close() error
}

type MemorySpannerGraphClient struct { /* unexported fields */ }
func NewMemorySpannerGraphClient() *MemorySpannerGraphClient

type SpannerGraphStore struct { /* unexported fields */ }
func NewSpannerGraphStore(client SpannerGraphClient) *SpannerGraphStore

// GraphStore Interface Implementation
func (s *SpannerGraphStore) Put(ctx context.Context, m StoredMeasurement) error
func (s *SpannerGraphStore) Get(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error)
func (s *SpannerGraphStore) GetByEntity(ctx context.Context, entity EntityIdentity) ([]StoredMeasurement, error)
func (s *SpannerGraphStore) GetByClass(ctx context.Context, entity EntityIdentity, classID string) ([]StoredMeasurement, error)
func (s *SpannerGraphStore) InvalidateEntity(ctx context.Context, entity EntityIdentity) error
func (s *SpannerGraphStore) Retract(ctx context.Context, node NodeCoordinate) error
func (s *SpannerGraphStore) All(ctx context.Context) ([]StoredMeasurement, error)
func (s *SpannerGraphStore) Count(ctx context.Context) (int, error)
func (s *SpannerGraphStore) Close() error

// Extended Fleet Operations
func (s *SpannerGraphStore) InvalidateFleet(ctx context.Context, instanceID string) (int64, error)
```

---

## 3. Structural Mechanics & Invariants

1. **Declarative GQL Mutations**:
   - `Put` executes an idempotent `MERGE` statement linking `(Entity)-[:OBSERVED]->(Observation)-[:INSTANCE_OF]->(Instance)-[:MEMBER_OF]->(Class)`.
2. **Bi-Directional Query Traversal**:
   - `GetByEntity` navigates outgoing `[:OBSERVED]` edges from the `Entity` vertex.
   - `GetByClass` joins `(Entity)-[:OBSERVED]->(Obs)-[:INSTANCE_OF]->(Inst)-[:MEMBER_OF]->(Class)`.
3. **Declarative Fleet Revocation**:
   - `InvalidateFleet` executes `MATCH (inst:Instance {id: @instanceID})<-[:INSTANCE_OF]-(obs:Observation) DETACH DELETE obs`, pruning the entire sensor fleet in a single distributed mutation without full-table scans.
4. **Zero Go GC Overhead**:
   - Data persists within Spanner's distributed storage engine outside the Go application process memory.

---

## 4. Requirement-to-Test Mapping (RFC 2119)

| Requirement / Invariant | Test Identifier |
| :--- | :--- |
| **MUST** insert and retrieve observation nodes via GQL graph | `TestSpannerGraphStore_Spec/MUST_insert_and_retrieve_observation_nodes...` |
| **MUST** retrieve observations by Entity and Class ontology traversal | `TestSpannerGraphStore_Spec/MUST_retrieve_observations_by_Entity_and_Class...` |
| **MUST** execute declarative fleet invalidation (`DETACH DELETE`) across all entities | `TestSpannerGraphStore_Spec/MUST_execute_declarative_fleet_invalidation...` |
| **MUST** invalidate entity subtree in $O(1)$ declarative GQL statement | `TestSpannerGraphStore_Spec/MUST_invalidate_entity_subtree_in_O(1)...` |

---

## 5. Related Documents
- **Glossary**: [Spanner Graph](../glossary.md#spanner-graph), [RelateDB](../glossary.md#relatedb), [Cypher Query Language](../glossary.md#cypher-query-language)
- **Architecture**: [L2 Backing Store Trade-Offs & Evaluation](../architecture/backing-store-evaluation.md), [Two-Tier Storage Architecture](../architecture/two-tier-storage.md)
- **CodeArtifact**: [artifacts/trie-cache.md](trie-cache.md), [artifacts/cluster-router.md](cluster-router.md)
