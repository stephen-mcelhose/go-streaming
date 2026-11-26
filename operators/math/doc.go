// Package math provides pure, error-free streaming wrappers for mathematical, statistical, and signal-processing pipelines.
//
// # Why the Math Package?
//
// In mathematical, scientific, and data analysis pipelines, computations are mathematically pure functions
// (such as vector dot products, signal filtering, statistical accumulations, or coordinate transformations).
// Wrapping every numerical operation with error checks or returning (T, error) creates cognitive overhead
// that obscures the underlying mathematics.
//
// The math package provides clean functional signatures (e.g. func(x float64) float64) while delegating
// underlying stream execution to the context-aware primitive operators. Any transport-level errors encountered
// (such as broken streams or canceled contexts) panic immediately so computation halts cleanly.
package math
