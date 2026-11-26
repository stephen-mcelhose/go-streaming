// Package pure provides ergonomic streaming wrappers for pure, infallible functions.
//
// In mathematical, scientific, and deterministic data pipelines, transformation functions
// are pure functions (e.g., f(x) -> y) that cannot fail by definition.
// The pure package allows callers to supply pure functions (e.g. func(I) O, func(T) bool)
// without error-handling boilerplate, while returning any transport or context cancellation errors.
package pure
