---
title: "Definition: StatefulCalculation"
type: "Definition"
tags:
  - "definition"
  - "domain-model"
  - "calculations"
  - "routing"
  - "formulas"
version: "1.0.0"
status: "Active"
---

# Definition: StatefulCalculation

## 1. Formal Definition

A `StatefulCalculation` is a domain-specific mathematical model, formula, or state accumulator that executes reactively over streaming observations. It declares multi-variable input requirements across domain ontology classes ([ClassSelector](../glossary.md#classselector)) and evaluates when all required inputs are present for a given subject entity.

---

## 2. Interface Contract & Semantic Specification

```go
type StatefulCalculation interface {
    // ID returns a unique identifier for the calculation rule (e.g. "StandardizedYieldCalc").
    ID() string

    // Dependencies returns the set of required ontology classes needed to evaluate the formula.
    Dependencies() []ClassSelector

    // Execute evaluates the mathematical formula against the resolved input observations
    // and returns zero or more derived streaming measurements.
    Execute(
        ctx context.Context,
        entity storage.EntityIdentity,
        inputs map[string]storage.StoredMeasurement,
    ) ([]storage.StreamingMeasurement[float64], error)
}
```

---

## 3. Operational Semantics & Invariants

1. **Class-Based Binding (Hardware Decoupling)**:
   - A calculation **MUST** declare input slots via `ClassSelector` (e.g. `CLASS_GRAIN_YIELD`), never concrete hardware IDs (`COMBINE_ROTARY_01`).
   - The engine resolves matching candidate observations dynamically from the entity's working memory subtree.
2. **Deterministic Input Map**:
   - The `inputs` map provided to `Execute` is keyed by `ClassSelector.ClassID`.
   - `Execute` is only invoked when `len(inputs) == len(Dependencies())` (all slots satisfied).
3. **First-Class Output Emission**:
   - Every returned derived measurement is a standard `StreamingMeasurement[float64]`.
   - When forward chaining is enabled (`Engine.EnableFeedback(true)`), emitted derived measurements are re-ingested into working memory to trigger downstream dependent formulas.
4. **Purity & Context Safety**:
   - `Execute` **MUST** respect `ctx.Done()` and abort immediately if context is canceled.
   - `Execute` **MUST NOT** perform external blocking I/O or network requests.

---

## 4. Related Documents
- **Glossary**: [StatefulCalculation in Glossary](../glossary.md#statefulcalculation), [ClassSelector](../glossary.md#classselector)
- **Concept**: [Rete Pattern Matching & Dynamic Slot Resolution](../concepts/rete-slot-resolution.md)
- **Concept**: [Forward-Chaining Calculation Cascades](../concepts/forward-chaining.md)
- **CodeArtifact**: [artifacts/classifier-engine.md](../artifacts/classifier-engine.md)
