# Research: Relate Graph Database (`relatedb.ai` / `relatedb/relate`)

## Executive Summary
**Relate** (`github.com/relatedb/relate`, `relatedb.ai`) is an embedded, in-memory property graph database engine written in **Rust** (released by Neo4j Labs). Positioned as the **"SQLite for property graphs"**, it runs in-process without daemon subprocesses, supports **Cypher 25 / GQL / openCypher**, and stores graphs using an in-memory **Compressed Sparse Row (CSR)** layout backed by a **Write-Ahead Log (WAL)** and snapshot checkpoints (Parquet, JSON, single-file `.rl`).

---

## 1. Core Architecture & Storage Model

```
                    ┌─────────────────────────────────────────┐
                    │               Host Process              │
                    │  (Go / Python / Rust / TypeScript / CLI)│
                    └────────────────────┬────────────────────┘
                                         │ C ABI v2 (ADR-0502) / cgo
                    ┌────────────────────▼────────────────────┐
                    │            Relate Engine (Rust)         │
                    │   - Cypher 25 / GQL Parser & Planner    │
                    │   - In-Memory CSR Graph Store           │
                    │   - Vector & Fulltext Indexes           │
                    └───────────────┬─────────────────────────┘
                                    │ WAL / Snapshots
                    ┌───────────────▼─────────────────────────┐
                    │            Durable Storage              │
                    │   - WAL (MDBWAL01 / MDBWAL02)           │
                    │   - Checkpoints (.parquet / .json / .rl)│
                    │   - Schema (.cypherlite/schema/)        │
                    └─────────────────────────────────────────┘
```

### Storage Engines & Formats
1. **In-Memory**: RAM only; zero disk writes (ideal for ephemeral testing & fast streaming buffers).
2. **Directory / Parquet (Default)**: WAL appended per write; periodic checkpoints write columnar **Parquet** shards.
3. **JSON Directory**: Human-readable `graph.json` snapshots alongside WAL.
4. **Single-File (`.rl`)**: Self-contained archive bundling catalog, snapshot, and active WAL.
5. **Markdown / Obsidian Mode**: Uses Markdown notes with `[[wikilinks]]` directly as nodes and edges.

### Durability & Concurrency Modes
- `full` (default): `fsync` barrier before acknowledging writes.
- `everysecond`: Asynchronous flush with $\le 1\text{s}$ loss window for extreme write throughput.
- `ReadOnly` & `Follow`: Multiple processes can read or tail the WAL of an active writer.

---

## 2. Cypher & Query Language Support

- **Standard Compliance**: Implements **Cypher 25**, **GQL**, and **openCypher** (tested against openCypher TCK).
- **Core Operations**: `MATCH`, `OPTIONAL MATCH`, `CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE`, `FOREACH`, `INSERT`.
- **Subqueries & Composition**: `WITH`, `UNWIND`, `UNION`, `CALL { ... }` subqueries.
- **Path Queries**: Quantified path patterns (`(a)-[:STEP*1..5]->(b)`), shortest paths, path selectors.
- **Integrated Vector & Geospatial**: `CREATE VECTOR INDEX` for hybrid semantic/graph traversal, 2D/3D WGS-84 point queries.

---

## 3. Go Client Integration

- **Package**: `github.com/relatedb/relate/clients/go` (via Cgo wrapping C ABI v2).
- **Zero JSON Overhead**: Uses typed value walkers over the native binary C-FFI boundary (`librelate_ffi`).
- **Connection Model**: Embedded in-process database (`relate.Open(...)`) or client connecting via Bolt / HTTP Query API v2.

---

## 4. Mapping Relate to Q&A / Measurement Stream Graph

The Q&A Route Table hierarchy ($5{,}557$ Primary Questions, $13{,}088$ Classes, $34{,}083$ QGIs) maps cleanly into a Property Graph schema in Relate:

```
(:PrimaryQuestion {code: "SC"})
       │
   [:HAS_CLASS]
       ▼
(:QuestionClass {classSeq: "C5343", selector: "[SC]{ANSM, SCTYPE, TAXA}"})
       │
   [:HAS_INSTANCE]
       ▼
(:QuestionGroupInstance {qgi: "C5343-I6", selector: "[SC]{ANSM=User Collected, SCTYPE=Final Stand, TAXA=Corn}"})
       │
   [:BOUND_TO_TARGET]
       ▼
(:CalculationTarget {targetCode: "RTLP", formula: "..."})
```

### Graph Capabilities for Streaming:
1. **Rete-Like Forward Inference in Cypher**:
   ```cypher
   MATCH (pq:PrimaryQuestion {code: $primaryCode})-[:HAS_CLASS]->(c:QuestionClass)-[:HAS_INSTANCE]->(i:QuestionGroupInstance)
   WHERE ALL(req IN c.requiredKeys WHERE req IN keys($measurementLabels))
   RETURN i.qgi AS boundQGI
   ```
2. **Dependency & Lineage Traversal**: Trace which downstream models depend on an incoming observation.
3. **Observation Cache Graph**: Link historical measurements by entity (`SET_ENTRY:37711542`) and query subgraphs on the fly.
