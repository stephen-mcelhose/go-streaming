# Design: Rete-like Measurement Graph with Best-Fit Slot Resolution

## 1. The Core Problem: Dynamic Resolution Under Incomplete Information

In real-world declarative calculations, the calculation author writes an abstract pattern like:
$$\text{PLOT}[\text{GRAIN\_YIELD}]\{\text{TAXA}=\text{Corn}, \text{METHOD}\}$$

They do **not** hardcode a specific schema class sequence ($C_{4220}$) or instance identifier ($I_2$). The calculation engine must evaluate the graph of available measurements for that entity and select the **best-fit instance slot** matching the pattern.

---

## 2. Best-Fit Selection Algorithm

When a calculation requires a slot matching `EntityType[PRIMARY]{CONSTRAINTS...}`:

### A. Hard Constraints (MUST Match)
1. **Primary Metric Match**: Measurement's `Primary.Key` must equal the pattern's `PRIMARY_CODE`.
2. **Entity Match**: Measurement's `Identity.EntityType` and `EntityID` must match the working memory entity.
3. **Closed Key-Value Rules (`KEY=VALUE`)**: If the pattern specifies `TAXA=Corn`, any candidate measurement missing `TAXA` or having `TAXA != Corn` is **disqualified** (score $= -\infty$).
4. **Open Key Presence Rules (`KEY`)**: If the pattern specifies `METHOD`, any candidate measurement missing the `METHOD` label is **disqualified**.

### B. Specificity & Best-Fit Scoring (Soft Tie-Breaking)
Among all qualified measurements in the entity's working memory for that metric:
$$\text{Score}(M) = w_{\text{exact}} \cdot N_{\text{exact\_matches}} + w_{\text{presence}} \cdot N_{\text{open\_matches}} + w_{\text{secondaries}} \cdot N_{\text{matching\_secondaries}} - w_{\text{unmatched}} \cdot N_{\text{unmatched\_extra\_labels}}$$

- **Highest Specificity Wins**: An instance matching `[GRAIN_YIELD]{TAXA=Corn, METHOD=Combine, HARVESTER=JohnDeere}` is a *better fit* for `[GRAIN_YIELD]{TAXA=Corn, METHOD}` than a generic instance `[GRAIN_YIELD]{TAXA=Corn, METHOD}` because it provides more concrete context while satisfying all constraints.
- **Recency Tie-Breaking**: If scores are identical, the measurement with the latest `Timestamp` wins.

---

## 3. Rete-like Alpha/Beta Network Architecture

```
Incoming Stream (Raw Measurements)
               │
               ▼
   [ Alpha Network: 1-Input Index & Ingestion ]
   - Extracts EntityIdentity, Metric, Labels, Secondaries
   - Indexes into Entity Working Memory (Alpha Memories partitioned by Entity:Metric)
               │
               ▼
   [ Working Memory Store: EntityGraph ]
   - Holds all active observation slots for Entity E:
     • E:PLOT_101 -> GRAIN_YIELD -> [Instance_1, Instance_2]
     • E:PLOT_101 -> MOISTURE    -> [Instance_A]
               │
               ▼
   [ Beta Network: Multi-Slot Join & Best-Fit Selector ]
   - Calculation Rule R requires:
       Slot 1: PLOT[GRAIN_YIELD]{TAXA=Corn, METHOD}
       Slot 2: PLOT[MOISTURE]{TAXA=Corn}
   - For Entity E:
       1. Query Alpha Memory for GRAIN_YIELD -> Run Best-Fit Scorer -> Selects Instance_1
       2. Query Alpha Memory for MOISTURE    -> Run Best-Fit Scorer -> Selects Instance_A
       3. If ALL required slots have a valid best-fit match -> TRIGGER READY
               │
               ▼
   [ Execution & Firing Node ]
   - Binds resolved best-fit values into calculation equation/closure
   - Fires StatefulCalculation
   - Emits derived Measurement to Downstream Stream
```

---

## 4. Generic Go Data Model & Implementation Contracts

```go
// ConstraintType distinguishes exact value matches from presence matches.
type ConstraintType int

const (
    ConstraintExactMatch ConstraintType = iota // KEY=VALUE (MUST match exact value)
    ConstraintKeyPresent                       // KEY (MUST exist on measurement)
)

// SlotConstraint represents a single parsed constraint inside {...}.
type SlotConstraint struct {
    Key   string
    Value string
    Type  ConstraintType
}

// SlotPattern represents a declarative slot selector: EntityType[PRIMARY]{CONSTRAINTS...}.
type SlotPattern struct {
    EntityType   string           // e.g. "PLOT"
    PrimaryCode  string           // e.g. "GRAIN_YIELD"
    Constraints  []SlotConstraint // Parsed rules inside {...}
}

// MatchScore evaluates a measurement against this pattern:
// - Returns (score, true) if all MUST constraints pass.
// - Returns (0, false) if any constraint fails.
func (p SlotPattern) MatchScore(m StoredMeasurement) (int, bool) {
    score := 0
    for _, c := range p.Constraints {
        switch c.Type {
        case ConstraintExactMatch:
            actualVal, exists := m.Labels[c.Key]
            if !exists || actualVal != c.Value {
                return 0, false // Disqualified
            }
            score += 10 // High weight for exact matching
        case ConstraintKeyPresent:
            _, inLabels := m.Labels[c.Key]
            _, inSec := m.Secondaries[c.Key]
            if !inLabels && !inSec {
                return 0, false // Disqualified
            }
            score += 5 // Moderate weight for key presence
        }
    }
    // Additional specificity bonus for matching secondary numeric values
    score += len(m.Labels) + len(m.Secondaries)
    return score, true
}
```
