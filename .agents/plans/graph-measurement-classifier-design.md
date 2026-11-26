# Design: Graph-Based Measurement Classification & Node Selection

## 1. Domain Vocabulary (Standardized)

We establish clean, generic streaming terminology:

| Term | Role / Definition | Example |
|---|---|---|
| **`EntityIdentity`** | Identifies the physical or logical entity observed. | `BusinessUnit: "Agronomy"`, `EntityType: "FIELD_TRIAL"`, `EntityID: "plot_37711542"` |
| **`Measurement[N]`** | An incoming streaming data point answering a primary metric question with labels and numeric secondaries. | `Primary: {Key: "GRAIN_YIELD", Value: 185.4}`, `Labels: [TAXA=Corn, METHOD=Machine]`, `Secondaries: [MOISTURE=14.2]` |
| **`ClassSelector`** | A schema contract defining the set of required categorical label keys for a metric. | `Metric: "GRAIN_YIELD"`, `RequiredLabels: ["TAXA", "METHOD"]` |
| **`InstanceSelector`** | A concrete classification rule binding a specific instance ID based on exact values (closed) or wildcard presence (open). | `InstanceID: "C4220-I2"`, `ClosedRules: {TAXA: "Corn", METHOD: "Machine"}` |
| **`NodeSelector`** | The resolved graph coordinate `(EntityID, InstanceID, MetricKey)` that identifies a variable slot for downstream calculations. | `Entity: "plot_37711542"`, `InstanceID: "C4220-I2"`, `Metric: "GRAIN_YIELD"` |
| **`StatefulCalculation`** | A downstream model or formula that binds to one or more `NodeSelectors` and triggers when all required variables are present. | `YIELD_ADJUSTED = f(GRAIN_YIELD[C4220-I2], MOISTURE[C5343-I1])` |

---

## 2. Graph Hierarchy

```
[ Root Catalog ]
       │
   (:Metric {name: "GRAIN_YIELD"})
       │
   [:HAS_CLASS]
       ▼
   (:ClassSelector {classID: "C4220", schema: ["TAXA", "METHOD"]})
       │
   [:HAS_INSTANCE]
       ▼
   (:InstanceSelector {instanceID: "C4220-I2", closed: {TAXA: "Corn", METHOD: "Machine"}})
       │
       ▼ (Dynamic Stream Binding on Ingest)
   (:Entity {businessUnit: "Agronomy", entityID: "plot_37711542"})
       │
   [:HAS_MEASUREMENT]
       ▼
   (:MeasurementNode {
       instanceID: "C4220-I2",
       metric: "GRAIN_YIELD",
       value: 185.4,
       timestamp: 1700000000
   })
```

---

## 3. Streaming Flow

1. **Ingest**: `Receiver[Measurement[N]]` streams in measurements.
2. **Tree Walk / Classification**:
   - Look up `Metric` node in $\mathcal{O}(1)$.
   - Find candidate `ClassSelector` nodes whose required labels are satisfied.
   - Match `InstanceSelector` whose closed value constraints match the measurement's labels.
3. **Node Selection & Placement**:
   - If an instance matches, resolve target `NodeSelector(Entity, InstanceID, Metric)`.
   - Store / upsert the measurement into the Entity's graph subgraph under that `NodeSelector`.
4. **Stateful Calculation Trigger**:
   - Calculation engine checks if the entity's graph contains all required `NodeSelectors` to execute.
