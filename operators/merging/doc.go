// Package merging provides combinators for merging, concatenating, interleaving, and sorting multiple streams.
//
// # Operators
//
//   - Merge: Concurrent fan-in. Multiple streams are consumed in parallel, forwarding items to a single sender as they arrive.
//   - Concat: Sequential concatenation. Consumes bounded streams one after another (R1 -> R2 -> ... -> Rn).
//   - Interleave: Round-robin fair fan-in. Alternates pulling 1 item from each active stream (A1, B1, A2, B2, ...).
//   - Zip: Pairwise combination. Pairs elements 1:1 from two streams (A, B) -> O.
//   - MergeSort: Ordered k-way merge. Uses a priority queue heap to merge pre-sorted streams into a single sorted output.
package merging
