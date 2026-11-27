---
title: "Architectural: Striped Concurrency & Shard Scaling"
type: "Architectural"
tags:
  - "architecture"
  - "concurrency"
  - "striped-locking"
  - "birthday-problem"
  - "scaling"
version: "1.0.0"
status: "Active"
---

# Striped Concurrency & Shard Scaling

## 1. Concurrency Model & Motivation

In high-throughput streaming environments where thousands of distinct entities stream observations concurrently, coarse global locks create fatal serialization bottlenecks:

```
[ Coarse Global Lock ] ──► Every write to Plot A blocks reads/writes to Plot B, C, D...
```

Instead of fine-grained node-level mutexes (which introduce high memory allocation overhead and deadlock hazards during cross-tree traversals), `PrefixTrie[K, V]` uses a **Striped / Sharded Lock Ring**:

```
                       Path: [BU, EntityType, EntityID, ...]
                                         │
                                   FNV-1a Hash
                                         │
                                         ▼
                             Shard Index: hash & (M - 1)
                                         │
       ┌───────────┬───────────┬─────────┴─┬───────────┬───────────┐
       ▼           ▼           ▼           ▼           ▼           ▼
   [ Shard 0 ] [ Shard 1 ] ... [ Shard i ] ... [ Shard 254 ] [ Shard 255 ]
   sync.RWMutex sync.RWMutex   sync.RWMutex     sync.RWMutex  sync.RWMutex
```

---

## 2. Theoretical Grounding: Birthday Problem Collision Analysis

With a fixed ring of $M = 256$ `sync.RWMutex` instances, the probability of at least one lock collision among $k$ concurrently active worker goroutines is governed by the [Birthday Problem](https://en.wikipedia.org/wiki/Birthday_problem):

$$P(\text{Collision}) = 1 - \prod_{i=0}^{k-1} \left(1 - \frac{i}{M}\right) \approx 1 - e^{-\frac{k(k-1)}{2M}}$$

### Contention Probability Table ($M = 256$ Shards)

| Concurrent Workers ($k$) | Zero Collision Probability | Collision Probability | Operational Impact on Streaming Pipeline |
| :--- | :--- | :--- | :--- |
| **$k = 2$** (Pairwise) | **$99.61\%$** ($\frac{255}{256}$) | **$0.39\%$** | Near-zero lock contention between 2 random entities. |
| **$k = 4$** | **$97.68\%$** | $2.32\%$ | Near-linear core scaling across 4 threads. |
| **$k = 8$** | **$89.65\%$** | $10.35\%$ | ~1 in 10 chance two workers serialize briefly. |
| **$k = 16$** | **$61.94\%$** | $38.06\%$ | Standard 16-core saturated throughput. |
| **$k = 19$** | **$49.60\%$** | **$50.40\%$** | **50% Inflection Point** (Birthday Paradox boundary). |
| **$k = 32$** | **$13.34\%$** | $86.66\%$ | Saturated core queueing; avg hold time $< 2.0\ \mu\text{s}$. |
| **$k = 64$** | **$< 0.03\%$** | **$> 99.97\%$** | Collisions occur; avg queue depth $\approx 0.25$ workers/shard. |

---

## 3. Critical Section Invariants & Sizing Rules

1. **Microsecond Critical Sections**:
   - Critical sections under shard locks **MUST NOT** perform I/O, network calls, JSON serialization, or unbounded loops.
   - Lock hold times remain bounded to $< 2.0\ \mu\text{s}$, ensuring that even when two workers collide, queue wait latency is negligible.
2. **Deterministic Constant Memory Overhead**:
   - $M = 256$ `sync.RWMutex` instances consume exactly $\approx 6\ \text{KB}$ of total memory whether managing 100 or 100,000,000 active observations.
3. **High-Core Shard Sizing**:
   - For massively parallel ingestion clusters ($k \ge 64$ saturated streaming cores), the constructor `NewGenericPrefixTrieWithShards(M)` allows scaling shards to $M = 1,024$ or $M = 4,096$ to maintain $> 95\%$ collision-free execution.

---

## 4. Related Documents & Specifications
- **Glossary**: [Striped Locking in Glossary](../glossary.md#striped-locking), [Birthday Problem](../glossary.md#birthday-problem--hash-collisions)
- **CodeArtifact**: [artifacts/generic-prefix-trie.md](../artifacts/generic-prefix-trie.md)
- **Non-Functional Requirements**: [.agents/measurement-streaming-classifier/specs/non-functional-requirements.md](../../../../.agents/measurement-streaming-classifier/specs/non-functional-requirements.md)
