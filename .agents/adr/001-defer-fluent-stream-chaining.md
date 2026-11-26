# ADR 001: Defer Fluent Stream Chaining in Favor of Primitive Combinators

## Context and Problem Statement
Streaming operations currently take explicit `Receiver[I]` and `Sender[O]` parameters. Composing multiple operations requires wiring intermediate channel streams or goroutines. We need to decide whether to introduce a fluent method-chaining builder (`stream.From(...).Map(...).Filter(...).To(...)`) or retain explicit primitive function signatures.

## Decision Drivers
- Type simplicity and alignment with idiomatic Go function composition.
- Keeping core interface surface minimal (`Receiver`, `Sender`).
- Avoiding unnecessary channel allocations or rigid concurrency models inside chaining wrappers until caller usage patterns stabilize.

## Considered Options
1. Retain primitive functions with explicit `Receiver`/`Sender` arguments.
2. Introduce a monadic/fluent `Pipeline[T]` wrapper struct on top of primitives.
3. Redesign the core `Stream[T]` interface to have method receivers for all operators.

## Decision Outcome (Y-Statement)
In the context of the initial streaming library design,
facing the need for ergonomics versus interface simplicity,
we decided to **retain primitive function combinators with explicit `Receiver`/`Sender` arguments**
and neglected **fluent chaining builder wrappers**,
to achieve **minimal core surface area and clear separation of transport from transformation**,
accepting that **callers must wire intermediate streams manually for multi-stage pipelines**.

## Revisit Criteria
We will revisit adding a fluent chaining builder (e.g., `pipeline.From(rx).Map(fn).To(tx)`) when:
1. Callers demonstrate recurring boiler-plate in 3+ stage streaming pipelines.
2. If revisited, fluent chaining **MUST** be implemented strictly as lightweight ergonomic wrappers around existing primitives, without introducing new core stream interfaces.
