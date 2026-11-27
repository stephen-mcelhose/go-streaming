---
title: "Measurement Streaming Classifier — Documentation Management Guidelines"
type: "Guidelines"
tags:
  - "documentation"
  - "process"
  - "okf"
  - "standards"
version: "1.2.0"
status: "Active"
---

# Documentation Management Guidelines

## 1. Document Types & Schemas

All documentation in `examples/measurement_graph/docs/` and `.agents/measurement-streaming-classifier/` MUST use OKF (Open Knowledge Format) YAML frontmatter with explicit typing, status, and tagging.

### Allowed Document Types & Mandatory Invariants

| Document Type | Purpose | Structural & Verification Invariants |
| :--- | :--- | :--- |
| **`Glossary`** | Defines ubiquitous language, internal models, and external paradigms. | 1. **MUST** group terms into logical headings.<br>2. **MUST** order terms lexicographically (A–Z) within each heading.<br>3. External concepts **MUST** link to verifiable URLs (Wikipedia, official docs, papers).<br>4. Internal domain models **SHOULD** cross-link to relevant `CodeArtifact` and `Concept` docs. |
| **`Concept`** | Explains theory, algorithms, and computational paradigms. | 1. External concepts **MUST** link to verifiable web URLs.<br>2. **SHOULD** cross-link to related `CodeArtifact` docs.<br>3. **MUST** include ASCII/Mermaid structural diagrams and trade-off matrices. |
| **`Architectural`** | Documents cross-cutting component design, tiering, and data flows. | 1. **MUST** include visual/ASCII/Mermaid sequence or topology diagrams.<br>2. **MUST** detail invariants, failure modes, concurrency models, and lifecycle bounds. |
| **`CodeArtifact`** | Bridge between architecture/specs and Go implementation files. | 1. **MUST** use YAML lists for `source_files`, `test_files`, and `spec_files`.<br>2. **MUST** enforce the Single Cohesive Component Invariant (no bundling unrelated components).<br>3. **MUST** document public API surface, algorithmic complexity, and RFC 2119 requirement-to-test mapping. |
| **`Definition`** | Standalone, rigorous definition of a specific domain term, entity schema, boundary contract, or classification rule. | 1. **MUST** define scope, exact semantics, formal properties, and boundaries.<br>2. **MUST** document valid states, invalid states, and invariants.<br>3. **SHOULD** link to relevant `Glossary`, `Concept`, or `CodeArtifact` documents. |

---

## 2. OKF YAML Frontmatter Standard

### For `Glossary`, `Concept`, `Architectural`, `Definition`:
```yaml
---
title: "Document Title"
type: "Glossary | Concept | Architectural | Definition | Guidelines"
tags:
  - "tag1"
  - "tag2"
version: "1.0.0"
status: "Active | Draft | Superceded | Deprecated"
---
```

### For `CodeArtifact`:
```yaml
---
title: "CodeArtifact: ComponentName"
type: "CodeArtifact"
tags:
  - "tag1"
source_files:
  - "relative/path/to/source.go"
  - "relative/path/to/companion.go"
test_files:
  - "relative/path/to/source_test.go"
spec_files:
  - "relative/path/to/spec.md"
version: "1.0.0"
status: "Active"
---
```

---

## 3. Cohesion Rules for `CodeArtifact` Multi-File Lists

1. **Single Cohesive Subject**: All `source_files` in a `CodeArtifact` MUST belong to the same package and implement a single component boundary (e.g. `trie_store.go` + `rekey.go`).
2. **Distinct Layers Require Separate Artifacts**: Distinct tiers (e.g. generic data structures vs domain storage adapters vs C-FFI drivers) MUST NOT be merged into a single artifact.
3. **Bidirectional Traceability**: Every requirement stated in a `spec_file` MUST map to a public method in `source_files` and a runnable assertion in `test_files`.

---

## 4. Lexicographical Ordering Rules for Glossaries
- Top-level headings categorize terms (e.g. `## 1. External Systems & Core Paradigms`, `## 2. Core Domain Entities & Classifier Models`, `## 3. Storage Architecture, Caching & Partitioning`).
- Inside each heading, terms MUST be listed in strict alphabetical order (A–Z).
- Each term entry MUST include:
  1. **Definition**: Crisp, domain-grounded explanation.
  2. **Type / Interface**: Associated Go type (if internal).
  3. **External Reference / Link**: Verifiable URL (if external paradigm/library).
  4. **Related Terms**: Markdown cross-links to related glossary items or code artifacts.
