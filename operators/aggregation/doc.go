// Package aggregation provides grouping, batching, and time-windowing operators for high-throughput streams.
//
// # Operators
//
//   - Buffer: Count-based grouping. Gathers incoming elements into slices of fixed size bufferSize.
//   - TumblingWindow: Time-based grouping. Gathers elements arriving within non-overlapping, continuous time windows.
//   - Batch: Hybrid micro-batching. Gathers elements using both maximum item capacity (MaxSize) and maximum flush wait time (MaxWait).
//     Commonly used for database bulk-insert and broker batching optimization.
//
// # Concurrency and Memory
//
// Aggregation operators maintain bounded memory buffers up to the configured limits and release buffers
// downstream immediately upon trigger or stream completion.
package aggregation
