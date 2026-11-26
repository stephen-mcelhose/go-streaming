// Package control provides flow control, rate limiting, debouncing, and retry operators.
//
// # Operators
//
//   - Throttle: Fixed-interval throughput limiter. Spaces emitted elements by at least a configured interval.
//   - Debounce: Emits an element only after a quiet duration has elapsed without new arrivals, squashing high-frequency bursts.
//   - RetrySender: Wraps an underlying Sender with exponential/constant retry backoff and optional fallback stream routing (e.g. DLQ).
package control
