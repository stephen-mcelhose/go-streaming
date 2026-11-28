---
title: "Research: Spanner Graph & ISO/IEC 39075:2024 (GQL)"
type: "Concept"
tags:
  - "research"
  - "spanner-graph"
  - "gql"
  - "iso-standard"
  - "graph-database"
  - "ontology"
version: "1.0.0"
status: "Active"
---

# Research: Cloud Spanner Graph & ISO/IEC 39075:2024 GQL

## 1. Executive Summary

This research document analyzes Google Cloud Spanner Graph and its foundation in the newly ratified **ISO/IEC 39075:2024 Graph Query Language (GQL)** international standard. It details how Spanner Graph's property graph model, interleaving schema, and declarative GQL syntax map to our streaming measurement graph engine, along with the testing methodology required for cloud-native graph integrations.

---

## 2. Standards & Authoritative References

- **ISO/IEC 39075:2024 Standard (Ratified April 2024)**:
  - Official ISO Publication: [ISO/IEC 39075:2024 — Information technology — Database languages — GQL](https://www.iso.org/standard/76120.html)
  - Working Group & Draft Specs: [GQL Standards Committee](https://www.gqlstandards.org/)
  - *Significance*: GQL is the first major new ISO database query language standard since SQL was standardized in 1986. It unifies Cypher, PGQL, and GSQL into an official international standard for property graph querying and mutation.
- **Google Cloud Spanner Graph Official Documentation**:
  - [Google Cloud Spanner Graph Overview](https://cloud.google.com/spanner/docs/graph/overview)
  - [Spanner Graph GQL Syntax & Query Reference](https://cloud.google.com/spanner/docs/graph/gql-syntax)
  - [Spanner Graph Schema Definition (DDL)](https://cloud.google.com/spanner/docs/graph/schema-overview)

---

## 3. Spanner Graph Ontology Architecture

In Spanner Graph, graph schemas are defined on top of relational tables using declarative `CREATE PROPERTY GRAPH` statements. 

### 3.1 DDL Schema Definition for Measurement Graph

```sql
-- 1. Base Node Tables
CREATE TABLE Entity (
    business_unit STRING(64) NOT NULL,
    entity_type   STRING(64) NOT NULL,
    entity_id     STRING(128) NOT NULL,
    entity_key    STRING(256) NOT NULL,
) PRIMARY KEY (entity_key);

CREATE TABLE Observation (
    node_key      STRING(512) NOT NULL,
    entity_key    STRING(256) NOT NULL,
    primary_code  STRING(64) NOT NULL,
    class_id      STRING(64) NOT NULL,
    instance_id   STRING(64) NOT NULL,
    val           FLOAT64 NOT NULL,
    ts            TIMESTAMP NOT NULL,
) PRIMARY KEY (node_key);

CREATE TABLE Class (
    class_id      STRING(64) NOT NULL,
) PRIMARY KEY (class_id);

CREATE TABLE Instance (
    instance_id   STRING(64) NOT NULL,
    class_id      STRING(64) NOT NULL,
) PRIMARY KEY (instance_id);

-- 2. Property Graph Definition (ISO GQL Schema)
CREATE PROPERTY GRAPH MeasurementOntology
  NODE TABLES (
    Entity,
    Observation,
    Class,
    Instance
  )
  EDGE TABLES (
    Observation AS Observed
      SOURCE KEY (entity_key) REFERENCES Entity (entity_key)
      DESTINATION KEY (node_key) REFERENCES Observation (node_key)
      LABEL OBSERVED,
    Observation AS InstanceOf
      SOURCE KEY (node_key) REFERENCES Observation (node_key)
      DESTINATION KEY (instance_id) REFERENCES Instance (instance_id)
      LABEL INSTANCE_OF,
    Instance AS MemberOf
      SOURCE KEY (instance_id) REFERENCES Instance (instance_id)
      DESTINATION KEY (class_id) REFERENCES Class (class_id)
      LABEL MEMBER_OF
  );
```

---

## 4. GQL Query & Invalidation Patterns

### 4.1 Ingestion (`MERGE` / `INSERT`)
```sql
GRAPH MeasurementOntology
MERGE (e:Entity {key: @entityKey})
SET e.businessUnit = @bu, e.entityType = @type, e.entityID = @id
MERGE (c:Class {id: @classID})
MERGE (inst:Instance {id: @instanceID})
MERGE (inst)-[:MEMBER_OF]->(c)
MERGE (obs:Observation {nodeKey: @nodeKey})
SET obs.primaryCode = @primaryCode,
    obs.classID = @classID,
    obs.instanceID = @instanceID,
    obs.value = @val,
    obs.timestamp = @ts
MERGE (e)-[:OBSERVED]->(obs)
MERGE (obs)-[:INSTANCE_OF]->(inst)
```

### 4.2 Multi-Hop Class Join for Calculation Slots (Beta Network)
```sql
GRAPH MeasurementOntology
MATCH (e:Entity {key: @entityKey})-[:OBSERVED]->(obs:Observation)-[:INSTANCE_OF]->(inst:Instance)-[:MEMBER_OF]->(c:Class {id: @classID})
RETURN obs.nodeKey, obs.primaryCode, obs.classID, obs.instanceID, obs.value, obs.timestamp
```

### 4.3 Declarative Sensor Fleet Revocation
```sql
GRAPH MeasurementOntology
MATCH (inst:Instance {id: @instanceID})<-[:INSTANCE_OF]-(obs:Observation)
DETACH DELETE obs
```

---

## 5. Testing Strategy: Unit Double vs. Live Integration Testing

### 5.1 Why the Test Double Exists (Unit Spec Gate)
Real Spanner Graph runs on Google Cloud infrastructure. Testing cloud dependencies directly in standard CI/CD introduces:
- Network flake and multi-second RPC latency.
- GCP credential/ADC authentication requirements.
- Cloud resource provisioning and billing costs.

The `SpannerGraphClient` interface abstracts the network transport, allowing `MemorySpannerGraphClient` to validate GQL semantics in nanoseconds under `go test -race` without credentials.

### 5.2 Live Integration Testing (`//go:build spanner_integration`)
For live end-to-end integration testing against real Cloud Spanner or the Google Cloud Spanner Emulator:

1. **Spanner Emulator Support**:
   - The official Google Cloud Spanner emulator can be run locally via Docker:
     ```bash
     docker run -d -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator
     ```
   - *Note on Spanner Emulator*: As of mid-2024, Cloud Spanner Emulator's support for full `GRAPH` GQL syntax is in preview; full distributed query planner validation requires a live Spanner instance.
2. **Integration Test Suite Structure**:
   - Live integration tests are gated behind the `//go:build spanner_integration` build tag (similar to `//go:build relate` for native RelateDB).
   - They authenticate via Application Default Credentials (ADC) or `SPANNER_EMULATOR_HOST`, provision a test database with `MeasurementOntology` DDL, and run live multi-hop traversals.

---

## 6. Related Documents
- **Glossary**: [Spanner Graph](../glossary.md#spanner-graph), [Cypher Query Language](../glossary.md#cypher-query-language)
- **Architecture**: [L2 Backing Store Trade-Offs & Evaluation](backing-store-evaluation.md), [Two-Tier Storage Architecture](two-tier-storage.md)
- **CodeArtifact**: [artifacts/spanner-graph-store.md](../artifacts/spanner-graph-store.md)
