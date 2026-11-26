# Comparative Synthesis: Calculation Engines, Dynamic Constrainers & Rete Graph Routing

## 1. Architectural Insights from `p360-performance` and `stephen-streaming-calculations`

### A. The Legacy / Microservice Challenge (`p360-performance`)
- **Poll / Query Architecture**: In `calculation-trigger-api` and `calculated-process-executions`, calculations were historically orchestrated via batch queries or RPC fetching (`List(targets, taxa, businessUnits)`).
- **Target & Exclusion Coupling**: Finding if a calculation can run required making database roundtrips to look up calculation transactions, question exclusions, and business unit approvals.

### B. The Streaming Breakthrough (`stephen-streaming-calculations`)
- **Prefix Working Memory (`ObservationCache`)**: Uses `EntityType:EntityName:QGI:ObservationName` to index streaming inputs under a shared entity prefix.
- **Dynamic Sibling Constraining (`VariableTemplateConstrainer`)**: When an observation binds to slot $A$ (e.g. `Taxa=Corn`), it dynamically narrows sibling unbound slots ($B, C, \dots$) by injecting the observed contextual constraints.
- **Prefix Projection (`ProjectedObservationCache`)**: A calculation queries only its declared prefix subspace rather than scanning entire databases.

---

## 2. Our Unified Reactive Model in `go-streaming`

By combining the **Rete pattern network**, **best-fit slot selection**, and **dynamic sibling template constraining**, we achieve a zero-database streaming calculation router:

```
[ Incoming Stream: Producer ]
  Measurement: PLOT:plot_37711542 [GRAIN_YIELD=185.4]{TAXA=Corn, METHOD=Combine}
               │
               ▼
   ┌────────────────────────────────────────────────────────┐
   │        Rete Alpha Network (Working Memory)             │
   │  - Evaluates hard constraints (MUST) & soft fit (score)│
   │  - Binds to entity working memory                      │
   └───────────────────────────┬────────────────────────────┘
                               │
                               ▼
   ┌────────────────────────────────────────────────────────┐
   │        Beta Network & Dynamic Constrainer              │
   │  - Slot A binds: [GRAIN_YIELD]{TAXA=Corn}              │
   │  - Constrainer narrows Slot B: [MOISTURE]{TAXA=Corn}   │
   │  - Evaluates complete set satisfaction                 │
   └───────────────────────────┬────────────────────────────┘
                               │
                  (All Required Slots Satisfied)
                               │
                               ▼
   ┌────────────────────────────────────────────────────────┐
   │      Stateful Execution & Emission (Stream 2)          │
   │  - Computes Standardized Yield via LaTeX/pure closure  │
   │  - Emits: PLOT:plot_37711542 [ADJ_YIELD=187.59]        │
   └────────────────────────────────────────────────────────┘
```

---

## 3. Dynamic Sibling Constrainer Implementation

```go
// SiblingConstrainer dynamically binds contextual labels learned from prior slot bindings.
type SiblingConstrainer interface {
    Constrain(boundSlot string, boundObs StoredObservation, unboundPattern PatternSelector) PatternSelector
}

// AutoTaxonConstrainer propagates contextual taxonomy/metadata from the first bound observation
// to all remaining unbound slot patterns.
type AutoContextConstrainer struct {
    PropagateKeys []string // e.g. ["TAXA", "BUSINESS_UNIT", "LOCATION"]
}

func (c AutoContextConstrainer) Constrain(boundSlot string, boundObs StoredObservation, unboundPattern PatternSelector) PatternSelector {
    for _, k := range c.PropagateKeys {
        if val, exists := boundObs.Labels[k]; exists {
            // Inject closed requirement into unbound sibling pattern
            unboundPattern.Rules = append(unboundPattern.Rules, SlotRule{
                Key:   k,
                Value: val,
                Kind:  ConstraintExact,
            })
        }
    }
    return unboundPattern
}
```
