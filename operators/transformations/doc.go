// Package transformations provides context-aware, element-wise stream transformation operators.
//
// # Operators
//
//   - Map: Transforms elements 1-to-1 from type I to type O using func(ctx, I) (O, error).
//   - FlatMap: Expands 1 input element into 0..N output elements by returning a sub-stream Receiver[O].
//   - Filter: Evaluates a predicate per element, passing elements that evaluate to true and dropping false.
//   - Reduce: Bounded stream accumulator. Consumes the entire stream and emits a single final state object O.
//   - Scan: Unbounded stream accumulator. Emits the intermediate accumulated state O after each incoming item I.
//
// # Context and Error Propagation
//
// All transformation operators are context-aware. If the caller's context is canceled or the mapper/predicate/reducer
// returns an error, processing stops immediately and the error is returned to the caller.
package transformations
