---
title: "Measurement Streaming Classifier — Documentation Management Guidelines"
type: "Guidelines"
tags:
  - "documentation"
  - "process"
  - "okf"
  - "standards"
version: "1.0.0"
status: "Active"
---

# Documentation Management Guidelines

## 1. Document Types & Schemas

All documentation in `examples/measurement_graph/docs/` and `.agents/measurement-streaming-classifier/` MUST use OKF (Open Knowledge Format) YAML frontmatter with explicit typing, status, and tagging.

### Allowed Document Types

| Document Type | Purpose | Structural Requirements |
| :--- | :--- | :--- |
| **`Glossary`** | Defines ubiquitous language, internal models, and external dependencies. | MUST group terms into logical headings; MUST order terms lexicographically within each heading unless a domain hierarchy is explicitly documented; MUST cross-link related terms. |
| **`Concept`** | Explains foundational domain or algorithmic theory (e.g. Inverted Prefix Tries, Forward Chaining, Rete Slot Resolution). | MUST include domain context, theoretical formulation, visual/ASCII architecture, and trade-off matrices. |
| **`Architectural`** | Documents cross-cutting component design, tiering, and data flows. | MUST describe component roles, invariants, concurrency models, and sequence/dataflow diagrams. |
| **`CodeArtifact`** | Documents concrete packages, interfaces, or structs. | MUST link to source code files, enumerate public API contracts, and specify algorithmic complexity. |
| **`Definition`** | Single-topic deep dive into a specific domain entity or metric formula. | MUST detail mathematical recurrence/formulas, inputs, outputs, and validation rules. |

---

## 2. OKF YAML Frontmatter Standard

Every markdown document MUST lead with:

```yaml
---
title: "Document Title"
type: "Glossary | Concept | Architectural | CodeArtifact | Definition | Guidelines"
tags:
  - "tag1"
  - "tag2"
version: "1.0.0"
status: "Draft | Active | Superceded | Deprecated"
---
```

---

## 3. Lexicographical Ordering Rules for Glossaries
- Top-level headings categorize terms (e.g. `## External Systems & Paradigms`, `## Core Domain Entities`, `## In-Memory Storage & Operations`).
- Inside each heading, terms MUST be listed in strict alphabetical order (A–Z).
- Each term entry MUST include:
  1. **Definition**: Crisp, domain-grounded explanation.
  2. **Type / Interface**: Associated Go type (if applicable).
  3. **Related Concepts**: Markdown interlinks to related terms or specifications.
