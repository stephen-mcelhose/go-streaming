# Specification: Measurement Graph Classification & Calculation Engine

## 1. Domain Types

| Type | Definition |
|---|---|
| `EntityIdentity` | Triplet `[BusinessUnit, EntityType, EntityID]` defining observation subject. |
| `NodeCoordinate` | Ontology address: `BU:EntityType:EntityID:PrimaryCode:ClassID:InstanceID[:Key=Value...]`. |
| `StreamingMeasurement[N]` | Incoming observation with primary metric, categorical labels, numeric secondaries, and timestamp. |
| `StoredMeasurement` | Placed observation residing in working memory at an exact `NodeCoordinate`. |
| `ClassSelector` | Ontology class contract declaring required label keys for a metric question. |
| `InstanceSelector` | Concrete classification rule mapping label key-value pairs to an `InstanceID`. |
| `PlacementResult` | Result of ingest: placed coordinates and any triggered calculation emissions. |

---

## 2. Behavioral Specifications (RFC 2119)

### 2.1 Ingest & Classification Contract
- **MUST** match incoming measurements against `ClassSelector`s by ensuring all required label keys are present.
- **MUST** evaluate `InstanceSelector` rules to bind the observation to a concrete `InstanceID`.
- **MUST** construct an inverted `NodeCoordinate` and store the measurement into the configured `GraphStore`.

### 2.2 Calculation Routing & Forward Feedback Contract
- **MUST** evaluate calculation dependencies against `ClassSelector`s rather than physical `InstanceID`s.
- **MUST** trigger calculation execution when all required domain classes are satisfied for an entity.
- **MUST** support forward chaining (`EnableFeedback(true)`) where derived calculation outputs are placed into working memory and automatically trigger downstream dependent calculations.

### 2.3 Subtree Pruning & Lifecycle Contract
- **MUST** support $O(1)$ subtree invalidation (`InvalidateEntity`) by pruning all coordinates under `[BU, EntityType, EntityID]`.
- **MUST** support single-coordinate point retraction (`Retract`).
