# Conceptual Mapping: Domain Entity & Dynamic Cache Binding

## 1. Route Table Statistics
- **Total Lines**: 104,244
- **Primary Questions**: 5,557
- **Classes ($C_{\text{seq}}$)**: 13,088
- **Instances / QGIs ($C_{\text{seq}}\text{-}I_{\text{seq}}$)**: 34,083

---

## 2. Generic Concept $\leftrightarrow$ Domain Mapping

| Generic `go-streaming` Concept | Q&A / Business Streaming Domain | Concrete Example |
|---|---|---|
| **Identity / Partition Keys** | Entity Properties | `BusinessUnit: "CropScience"`, `EntityType: "SET_ENTRY"`, `EntityName: "37711542"`, `GroupID: "TRIAL_99"` |
| **`Primary: KeyValue[string, Number]`** | Primary Question Code & Observed Numeric Value | `Key: "SC"`, `Value: 400.0` or `Key: "RTLC"`, `Value: 0.0` |
| **`Labels: []KeyValue[string, string]`** | Categorical answers / Metadata tags | `[{Key: "ANSM", Value: "System Calculated"}, {Key: "TAXA", Value: "Corn"}]` |
| **`Secondaries: []KeyValue[string, Number]`**| Auxiliary numeric measurements | `[{Key: "PULSE", Value: 72.0}, {Key: "TEMP_C", Value: 36.8}]` |
| **Class Selector ($C_{\text{seq}}$)** | Question Group Schema (Required Question Codes) | `[SC]{ANSM, SCTYPE, TAXA}` |
| **Instance Selector ($C_{\text{seq}}\text{-}I_{\text{seq}}$)** | Question Group Instance (QGI Binding Rule) | `[SC]{ANSM=User Collected, SCTYPE=Final Stand, TAXA=Corn}` |
| **Bound Route Target** | Specific QGI identifier | `C5343-I6` |
| **Prefix Cache Key** | Observation Cache Coordinates | `SET_ENTRY:37711542:C5343-I6:SC` $\to$ `400.0` |

---

## 3. Dynamic Cache Binding Architecture

```
Incoming Stream (Raw Measurement)
  Measurement:
    Identity: {EntityType: "SET_ENTRY", EntityName: "37711542", BusinessUnit: "CP", GroupID: "G1"}
    Primary:  {Key: "SC", Value: 400.0}
    Labels:   [ANSM=User Collected, SCTYPE=Final Stand, TAXA=Corn]
    Secondaries: [...]
         │
         ▼
[ RouteTable / Matcher (O(1) Primary lookup + Bitset/Rule match) ]
  Evaluates selectors for primary "SC":
    Class C5343: required keys {ANSM, SCTYPE, TAXA} -> MATCH
    Instance I6: {ANSM=User Collected, SCTYPE=Final Stand, TAXA=Corn} -> MATCH!
  Result: Bound QGI = "C5343-I6"
         │
         ▼
[ Dynamic Cache Key Synthesizer ]
  Key: "{EntityType}:{EntityName}:{QGI}:{PrimaryQuestion}"
  Key: "SET_ENTRY:37711542:C5343-I6:SC"
         │
         ▼
[ Prefix Observation Cache (Trie / Map) ]
  Stores / Updates: ("SET_ENTRY:37711542:C5343-I6:SC", 400.0)
         │
         ▼
[ Downstream Stateful Calculation / Trigger Stream ]
  Queries cache prefix `SET_ENTRY:37711542:*` to reconcile equation variables
  Evaluates stateful LaTeX or math expression when all required QGIs are present
```

---

## 4. Execution Plan for `feat/measurement-streaming-classifier`

1. **Step 1: Enriched Generic `Measurement` & `Identity` Types**
   - Define `Identity` struct (`BusinessUnit`, `EntityType`, `EntityName`, `GroupID`).
   - Define generic `Measurement[N OrderedNumber]` with `Identity`, `Primary`, `Labels`, and `Secondaries`.
2. **Step 2: Compiled Route Table & Dynamic Classifier**
   - High-performance in-memory Route Table:
     - Map `PrimaryQuestion` $\to$ List of candidate `ClassRoutes`.
     - Fast label matching (exact value for closed rules, key existence for open rules).
     - Returns matched `QGI` (e.g., `C5343-I6`).
3. **Step 3: Dynamic Streaming Observation Cache Adapter**
   - Trie-based in-memory prefix store: `Store(ctx, key, measurement)` / `ValuesUnder(ctx, prefix)`.
   - Streaming Cache Sink: takes incoming `Receiver[Measurement[N]]`, classifies to QGI, and dynamically updates cache.
4. **Step 4: Table-Driven Behavioral Tests (RFC 2119)**
   - Test matching with open/closed rules.
   - Test prefix query retrieval by Entity and Group.
   - Test end-to-end streaming binding pipeline with runnable `Example_dynamicMeasurementCacheBinding`.
