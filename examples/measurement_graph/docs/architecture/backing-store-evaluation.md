---
title: "Architectural: L2 Backing Store Trade-Offs & Evaluation"
type: "Architectural"
tags:
  - "architecture"
  - "storage"
  - "backing-store"
  - "evaluation"
  - "relatedb"
  - "spanner-graph"
  - "dynamodb"
  - "redis"
version: "1.0.0"
status: "Active"
---

# Architectural Evaluation: L2 Backing Store Trade-Offs & Storage Selection

## 1. Executive Summary & Problem Formulation

The streaming measurement classifier and calculation routing engine operates on **two fundamentally orthogonal query axes**:

1. **Entity-Axis (Hot Streaming Path)**:
   - High-throughput point reads and subtree lookups (`BU:EntityType:EntityID:*`).
   - Powers the Rete Beta network by resolving required input observation slots for a single entity (e.g. `plot_101`).
   - Requires $< 5.0\ \mu\text{s}$ latency to sustain $> 100,000\ \text{ops/sec}$ streaming pipelines.
2. **Metric / Sensor Fleet Axis (Cold Governance & Revocation Path)**:
   - Cross-cutting operations targeting arbitrary depths across all entities (e.g. revoking faulty sensor `COMBINE_1` across 10,000 active plots).
   - In standard key-value or entity-first databases, this forces an catastrophic $O(N)$ full-dataset scan.

This document evaluates five major backing store paradigms to determine the optimal L2 persistent layer for single-node sidecars and distributed cluster deployments.

---

## 2. Comprehensive Backing Store Trade-Off Matrix

| Paradigm | Example Technologies | L2 Point Read / Write | Entity Subtree Query (`plot_101:*`) | Fleet Revocation (`COMBINE_1`) | Zero-GC / Off-Heap | Ideal Deployment Scale & Verdict |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **1. Embedded Native Graph DB** | **[RelateDB](https://github.com/stephen-mcelhose/relate)** (Rust/C-FFI), Kùzu | **Ultra-Fast** ($< 5\ \mu\text{s}$ C-FFI) | **Native pointer traversal** ($O(\text{edges})$) | **Cypher `DETACH DELETE`** in native space ($< 1\ \text{ms}$) | **Yes** (Rust/C++ heap, zero Go GC overhead) | **Optimal for Single-Host & Container Sidecars** ($\le 100\text{M}$ facts). |
| **2. Distributed Cloud Graph DB** | **Spanner Graph**, Neo4j Aura | **Medium** ($5\text{--}15\ \text{ms}$ network RPC) | **Indexed Graph Traversal** | **Declarative Cypher / GQL** across distributed storage | **Yes** (Runs outside Go process) | **Optimal for Global Multi-Region L2 Persistence** ($> 1\text{B}$ facts). |
| **3. Distributed Key-Value** | AWS DynamoDB, Redis, Bigtable | **Fast** ($1\text{--}3\ \text{ms}$ network) | **Prefix Range Scan** (`PK = entity`) | **Catastrophic ($O(N)$ full table scan)** or requires dual-write secondary GSI tables | **Yes** | **Poor Fit**: High operational complexity for secondary index syncing and high cost for full scans. |
| **4. Distributed In-Memory Cache** | Redis Cluster, Dragonfly | **Sub-millisecond** ($0.5\text{--}1\ \text{ms}$) | `HGETALL` or `SCAN` | Lua script sweep or manual secondary `SET` union | **Yes** | **Sub-optimal**: Lacks durable disk graph persistence; secondary set syncing risks consistency drift. |
| **5. Relational SQL** | PostgreSQL, MySQL, CockroachDB | **Medium** ($2\text{--}10\ \text{ms}$) | `SELECT ... WHERE entity_id = ?` | `DELETE WHERE instance_id = ?` (Fast if B-Tree indexed) | **Yes** | **Viable Alternative**: Solid all-rounder, but lacks natural multi-hop graph ontology semantics and requires schema migrations. |

---

## 3. Deep-Dive Paradigm Analysis

### 3.1 Distributed Key-Value Stores (DynamoDB / Bigtable)
- **The Inverted Index Dilemma**:
  - In a KV store keyed by `PK = BU:EntityType:EntityID`, fetching observations for `plot_101` is a fast single-partition range query.
  - However, revoking a sensor fleet (`COMBINE_1`) requires either:
    1. An $O(N)$ full table scan across millions of rows (prohibitively expensive and slow).
    2. A Global Secondary Index (GSI) keyed by `PrimaryCode:ClassID:InstanceID`.
- **The Dual-Write Problem**: Updating two tables in DynamoDB requires 2-phase transactions (transact-write APIs) which double write latency ($2\times$) and increase cloud costs.

### 3.2 Relational SQL (PostgreSQL / CockroachDB)
- **Strengths**: Creating a secondary B-Tree index on `(primary_code, class_id, instance_id)` makes both entity lookups and fleet deletions $O(\log N)$.
- **Weaknesses**: Relational row models do not naturally represent arbitrary hierarchical taxonomy graphs (`Category -> Class -> Instance -> Characteristics`). Extending characteristic keys requires JSONB columns (slower indexing) or complex EAV (Entity-Attribute-Value) schema joins.

### 3.3 Embedded Native Graph DB (RelateDB via C-FFI) — The L2 Winner for Sidecars
1. **Bi-Directional Index-Free Adjacency**:
   - The relationship `(Entity)-[:HAS_OBSERVATION]->(Obs)-[:OBSERVED_BY]->(Instance)` is indexed bi-directionally in compiled Rust memory.
   - Traversing from the Entity axis down to observations, or from the Sensor Instance axis across all entities, costs zero secondary index lookups.
2. **True $O(1)$ Native Fleet Revocation**:
   - Purging an entire sensor fleet executes via native Cypher:
     ```cypher
     MATCH (inst:Instance {id: "COMBINE_1"})<-[:OBSERVED_BY]-(obs:Observation)
     DETACH DELETE obs
     ```
   - This executes directly inside Rust/C++ heap memory, bypassing Go garbage collection and preventing GC mark-assist thrashing.
3. **Zero Network Latency**:
   - Operating in-process via C-FFI achieves sub-5-microsecond read/write execution ($1000\times$ faster than network RPCs).

---

## 4. Recommended Multi-Tier Architecture by Scale

### Tier 1: Single-Node / Container Sidecar ($\le 100\text{M}$ Measurements)
- **L1 Hot Memory**: Go `DualIndexStore` ($M = 256$ affinity shards) serving as an in-memory sliding window for live Rete calculation slot binding.
- **L2 Persistence**: In-process **RelateDB** (Rust/C-FFI) retaining historical observations and handling heavy off-heap Cypher deletions.

### Tier 2: Distributed Global Multi-Region Cluster ($> 1\text{B}$ Measurements)
- **L1 / Compute Tier**: Go `ClusterRouter` distributing entity-affinity pods across physical worker nodes (stateless computation + localized Rete formula evaluation).
- **L2 Persistence Tier**: **Spanner Graph** or **Distributed RelateDB** cluster providing multi-region durability, global audit trails, and distributed graph queries.

---

## 5. Related Documents
- **Glossary**: [RelateDB](../glossary.md#relatedb), [Cypher Query Language](../glossary.md#cypher-query-language), [Two-Tier Storage Architecture](../glossary.md#two-tier-storage-architecture)
- **Architecture**: [Two-Tier Storage Architecture](two-tier-storage.md), [Single-Owner Affinity Sharding (ADR-0004)](affinity-sharded-dual-trie.md)
- **CodeArtifact**: [artifacts/trie-cache.md](../artifacts/trie-cache.md), [artifacts/cluster-router.md](../artifacts/cluster-router.md)
