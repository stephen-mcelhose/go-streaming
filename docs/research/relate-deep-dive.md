# Deep Dive: Relate Graph Database (`relatedb/relate`)

## Executive Summary
**Relate** is an embedded in-memory property graph database engine written in **Rust** (released by Neo4j Labs). Designed as the "SQLite for property graphs", it runs in-process with a typed **C ABI v2** (ADR-0502), in-memory **Compressed Sparse Row (CSR)** graph representation, and durability backed by a local **Write-Ahead Log (WAL)** and columnar **Parquet** snapshot checkpoints.

---

## 1. Engine & Crate Architecture

The workspace is cleanly factored across three tiers (ADR-0103):
```
relate/ (Engine Core)
  ├── relate-model      # CypherValue representation & types
  ├── relate-parser     # Cypher lexer, parser, AST
  ├── relate-catalog    # Interned labels, relationship types, and property keys
  ├── relate-planner    # Logical query planner & MATCH optimizer
  ├── relate-executor   # Physical execution & in-memory CSR graph
  └── relate-storage    # WAL (MDBWAL01/02) + Snapshot codecs (Parquet / JSON)

surfaces/ (Adapters)
  ├── relate-ffi        # Stable C ABI v2 (`relate_*` symbols)
  ├── relate-bolt       # Neo4j-compatible Bolt server protocol
  ├── relate-arrow      # CypherValue ↔ Apache Arrow zero-copy conversions
  └── relate-python/mcp # Python native extension & MCP server for AI agents

clients/ (SDKs)
  ├── go                # Cgo bindings over C ABI v2 (zero JSON serialization)
  ├── java, dotnet, swift, javascript (WASM with OPFS)
```

---

## 2. Go Client (`clients/go`) Mechanics

- **No JSON Overhead**: Non-Rust clients communicate across `librelate_ffi` using a typed C ABI value walker (ADR-0502) rather than JSON serialization.
- **Connection Model**:
  ```go
  // In-memory ephemeral graph
  db, err := relate.Open("memory://")
  defer db.Close()

  conn, err := db.Connect()
  defer conn.Close()

  res, err := conn.QueryParams(`
      CREATE (p:PrimaryQuestion {code: $code})
      RETURN p.code AS code
  `, map[string]any{"code": "SC"})
  ```
- **Explicit Transactions & Namespaces**:
  - `conn.Begin()`, `conn.Commit()`, `conn.Rollback()`
  - Multi-tenancy / metadata isolation via namespaces (`neo4j`, `system`, `system_ontologies`).

---

## 3. High-Performance Q&A Streaming Graph Schema

To model the full route table ($5{,}557$ Primary Questions, $13{,}088$ Classes, $34{,}083$ QGIs) and stream measurements dynamically:

### Cypher Graph Schema:
```cypher
// 1. Primary Question Node
(:PrimaryQuestion {code: "SC"})

// 2. Class Selector Node (Schema / Required secondary keys)
(:QuestionClass {classSeq: "C5343", selector: "[SC]{ANSM, SCTYPE, TAXA}", requiredKeys: ["ANSM", "SCTYPE", "TAXA"]})

// 3. Instance Selector Node (Exact closed values vs open wildcards)
(:QuestionGroupInstance {
    qgi: "C5343-I6",
    selector: "[SC]{ANSM=User Collected, SCTYPE=Final Stand, TAXA=Corn}",
    closedBindings: {ANSM: "User Collected", SCTYPE: "Final Stand", TAXA: "Corn"},
    openKeys: []
})

// 4. Entity Observation State Node
(:Entity {type: "SET_ENTRY", name: "37711542"})-[:HAS_OBSERVATION]->(:Observation {
    qgi: "C5343-I6",
    primaryCode: "SC",
    value: 400.0,
    timestamp: 1700000000
})
```

### Dynamic Forward-Inference Query in Cypher:
```cypher
MATCH (pq:PrimaryQuestion {code: $primaryCode})-[:HAS_CLASS]->(c:QuestionClass)-[:HAS_INSTANCE]->(i:QuestionGroupInstance)
WHERE ALL(reqKey IN c.requiredKeys WHERE reqKey IN keys($labels))
  AND ALL(key IN keys(i.closedBindings) WHERE $labels[key] = i.closedBindings[key])
RETURN i.qgi AS boundQGI
```

---

## 4. Integration with `go-streaming`

```
Stream Ingress (Measurement[N])
         │
         ▼
 [ Relate Router (In-Memory Cypher Matcher) ]
   - Executes parameterized MATCH query against in-memory graph
   - Resolves QGI target ("C5343-I6")
         │
         ▼
 [ Relate Graph Observation Store (MERGE Entity -> Observation) ]
   - Graph accumulates active multi-entity observation state
         │
         ▼
 [ Stateful Calculation Trigger Stream ]
   - Downstream models query complete entity subgraphs for evaluation
```
