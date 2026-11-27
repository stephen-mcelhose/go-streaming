---
title: "Concept: Forward-Chaining Calculation Cascades"
type: "Concept"
tags:
  - "concept"
  - "forward-chaining"
  - "calculations"
  - "cascade"
  - "feedback-loop"
version: "1.0.0"
status: "Active"
---

# Forward-Chaining Calculation Cascades

## 1. Theoretical Foundation

**Forward chaining** ([Wikipedia Reference](https://en.wikipedia.org/wiki/Forward_chaining)) is a data-driven inference method where asserting new facts into working memory triggers matching rules, which in turn assert new derived facts. This process repeats iteratively until no new rules match or a termination goal is reached.

In streaming measurement processing, domain calculations often exist in hierarchical dependency tiers:
- **Tier 1 (Raw Ingest)**: Sensor readings arrive from physical devices (e.g. `Raw Yield`, `Moisture`, `Tractor Speed`).
- **Tier 2 (Primary Derived)**: Low-level formulas compute standardized metrics (e.g. `Standardized Grain Yield` at 15.5% standard moisture).
- **Tier 3 (Composite Aggregates)**: Multi-variable business models evaluate performance (e.g. `Plot Production Index` combining standardized yield, soil nitrogen, and elevation).

---

## 2. The Reactive Feedback Topology

Instead of orchestrating multi-tiered formulas through complex batch query schedules or external workflow DAGs, the calculation engine uses a **natural feedback loop**:

```
[ External Stream: Producer ]
  Measurement: PLOT:plot_101 [GRAIN_YIELD=185.4]
                     │
                     ▼
  ┌─────────────────────────────────────────────────────────────┐
  │ 1. Engine.ClassifyAndPlace (Ingest Step)                    │
  │    - Alpha Network classifies -> NodeCoordinate             │
  │    - Placed into Entity Working Memory (PrefixTrie)         │
  └──────────────────────────────┬──────────────────────────────┘
                                 │
                                 ▼
  ┌─────────────────────────────────────────────────────────────┐
  │ 2. Router.CheckAndTrigger (Tier 1 Rule Match)               │
  │    - Evaluates ClassSelector slots for plot_101             │
  │    - Slots satisfied: [CLASS_GRAIN_YIELD, CLASS_MOISTURE]   │
  │    - Executes: "StandardizedYieldCalculation"               │
  │    - Emits derived fact: PLOT:plot_101 [ADJ_YIELD=187.59]   │
  └──────────────────────────────┬──────────────────────────────┘
                                 │
             (Forward Cascading: Re-inject into Ingest)
                                 │
                                 ▼
  ┌─────────────────────────────────────────────────────────────┐
  │ 3. Engine.ClassifyAndPlace (Feedback Step - Tier 2)         │
  │    - Derived fact classified as CLASS_ADJUSTED_YIELD        │
  │    - Placed into Entity Working Memory (PrefixTrie)         │
  └──────────────────────────────┬──────────────────────────────┘
                                 │
                                 ▼
  ┌─────────────────────────────────────────────────────────────┐
  │ 4. Router.CheckAndTrigger (Tier 2 Rule Match)               │
  │    - Evaluates slots: [CLASS_ADJUSTED_YIELD, CLASS_NITROGEN]│
  │    - Slots satisfied! Executes: "ProductionIndexCalc"       │
  │    - Emits: PLOT:plot_101 [PROD_INDEX=94.2]                 │
  └──────────────────────────────┬──────────────────────────────┘
                                 │
                                 ▼
                    (Terminal State Reached)
```

---

## 3. Structural Properties & Invariants

1. **Derived Facts are First-Class Facts**:
   - The output of `StatefulCalculation.Execute()` is a standard `StreamingMeasurement[float64]`.
   - The engine does not distinguish between hardware sensor readings and derived calculation outputs during classification.
2. **Synchronous In-Memory Execution**:
   - Cascading occurs immediately in-memory within the calling goroutine's stack without thread handoffs or message queue latency.
3. **Loop Termination & Bounded Depth**:
   - To guard against circular calculation dependencies (e.g. $A \to B \to A$), the engine enforces a maximum cascade depth (default: $\text{MaxCascadeDepth} = 10$).
   - If recursion exceeds this threshold, the cascade halts safely and reports an error.
4. **Cohesive Return Payload**:
   - `ClassifyAndPlace` returns a `PlacementResult` containing both the initial placed coordinate and the complete slice of all derived measurements triggered across the entire cascade.

---

## 4. Related Documents & Specifications
- **Glossary**: [Forward Chaining in Glossary](../glossary.md#forward-chaining)
- **Concept**: [Rete Pattern Matching & Slot Resolution](rete-slot-resolution.md)
- **CodeArtifact**: [artifacts/classifier-engine.md](../artifacts/classifier-engine.md)
