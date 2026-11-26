// Package routing provides fan-out, round-robin, and key-partitioned stream routing operators.
//
// # Operators
//
//   - Broadcast: Replicates items across all target senders, failing fast if any destination fails.
//   - BroadcastBestEffort: Fire-and-forget fan-out. Attempts delivery to every target and ignores individual delivery failures.
//   - RoundRobin: Cyclically balances incoming load across multiple target senders.
//   - Partition: Routes items to specific keyed senders via a key extractor, with support for an optional default/DLQ fallback route.
package routing
