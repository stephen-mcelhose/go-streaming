---
title: "CodeArtifact: Engine & Router"
type: "CodeArtifact"
tags:
  - "engine"
  - "classifier"
  - "router"
  - "calculations"
source_files:
  - "examples/measurement_graph/engine.go"
  - "examples/measurement_graph/classifier.go"
  - "examples/measurement_graph/calculations.go"
  - "examples/measurement_graph/types.go"
test_files:
  - "examples/measurement_graph/measurement_graph_test.go"
  - "examples/measurement_graph/relatedb_test.go"
spec_files:
  - ".agents/measurement-streaming-classifier/specs/measurement-graph-engine-spec.md"
version: "1.0.0"
status: "Active"
---

# CodeArtifact: Engine & Router

## 1. Component Overview

`Engine` is the top-level streaming classification and calculation orchestrator. It receives raw streaming facts (`StreamingMeasurement[N]`), evaluates ontology schema matching (`ClassSelector`, `InstanceSelector`), places classified observations into graph storage (`GraphStore`), evaluates calculation dependencies via `Router`, and executes cascading forward feedback loops.

---

## 2. Public API Surface & Complexity

```go
// Engine Constructors & Configuration
func NewEngine(store storage.GraphStore) *Engine
func (e *Engine) AddClass(cs ClassSelector)
func (e *Engine) AddInstance(is InstanceSelector)
func (e *Engine) RegisterCalculation(calc StatefulCalculation)
func (e *Engine) EnableFeedback(enabled bool)

// Streaming Execution
func (e *Engine) ClassifyAndPlace(ctx context.Context, m storage.StreamingMeasurement[float64]) (PlacementResult, error)
func (e *Engine) InvalidateEntity(ctx context.Context, entity storage.EntityIdentity) error
func (e *Engine) Retract(ctx context.Context, node storage.NodeCoordinate) error
func (e *Engine) Store() storage.GraphStore

// Classification & Routing Types
type ClassSelector struct { Metric string; ClassID string; RequiredLabels []string }
type InstanceSelector struct { Metric string; ClassID string; InstanceID string; Rules map[string]string }
type PlacementResult struct { Placed storage.NodeCoordinate; Triggered []storage.StreamingMeasurement[float64] }
type StatefulCalculation interface {
    ID() string
    Dependencies() []ClassSelector
    Execute(ctx context.Context, entity storage.EntityIdentity, inputs map[string]storage.StoredMeasurement) ([]storage.StreamingMeasurement[float64], error)
}
```

---

## 3. Specification & Test Mapping

| Operation | RFC 2119 Specification Requirement | Test Assertion |
| :--- | :--- | :--- |
| Ingest & Classification | **MUST** match measurements against `ClassSelector` and `InstanceSelector` rules into an inverted `NodeCoordinate`. | `TestMeasurementGraphClassification/MUST_classify_and_store_measurements_into_entity_node_coordinates` |
| Slot Resolution | **MUST** resolve calculation dependencies across domain `ClassSelector`s decoupled from concrete instances. | `TestMeasurementGraphClassification/MUST_trigger_calculation_by_resolving_ClassSelectors_across_different_instances` |
| Forward Cascading | **MUST** re-inject derived calculation outputs into working memory and trigger downstream formulas. | `TestMeasurementGraphClassification/MUST_cascade_calculation_outputs_forward_into_downstream_calculations` |
| Live RelateDB Support | **MUST** support live C-FFI RelateDB storage backend for classification and calculation pipelines. | `TestMeasurementGraphWithLiveRelateDB` |

---

## 4. Related Documents
- **Glossary**: [Engine](../glossary.md#engine), [Router](../glossary.md#router), [StatefulCalculation](../glossary.md#statefulcalculation)
- **Concept**: [Rete Pattern Matching & Dynamic Slot Resolution](../concepts/rete-slot-resolution.md)
- **Storage Artifact**: [artifacts/trie-graph-store.md](trie-graph-store.md)
