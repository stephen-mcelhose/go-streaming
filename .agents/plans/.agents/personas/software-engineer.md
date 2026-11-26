# Persona: Software Engineer (Backend / Distributed Systems)

## Profile
- **Role**: Backend Software Engineer / Distributed Systems Developer
- **Focus**: Microservices, event-driven architectures, asynchronous task dispatching, resilience, and API streaming.
- **Primary Tools**: Go, gRPC, HTTP/2 Server-Sent Events, Redis, PostgreSQL, Kafka/PubSub.

## Daily Realities & Workflows
- Building resilient backend services that ingest incoming events (via HTTP webhooks or message queues) and dispatch downstream asynchronous jobs.
- Implementing graceful shutdown and cancellation across nested background goroutines when context cancels or SIGTERM arrives.
- Managing retry policies with exponential backoff and jitter, ensuring persistent failures are safely routed to a Dead Letter Queue (DLQ) without crashing the application.
- Integrating with wire protocols like gRPC bidirectional streams and Server-Sent Events (SSE).

## Pain Points with Existing Solutions
- **Channel Leaks & Panics**: Manually wiring Go channels often leads to deadlocks, goroutine leaks on early return, or panics when writing to closed channels.
- **Boilerplate Error Propagation**: Writing repetitive `select { case <-ctx.Done(): ... }` blocks and multi-goroutine synchronization boilerplate for simple pipeline stages.
- **Complex Retries & DLQ Wiring**: Standard libraries lack built-in retry sinks with automatic fallback routing to dead-letter streams.

## What Makes Them Say: *"Damn, I can see how to use this"*
- Clean, type-safe interfaces (`Receiver[T]`, `Sender[T]`) that decouple business logic from transport (channels, gRPC, HTTP).
- Automatic context cancellation propagation at every step (`Receive` and `Send` natively respect `ctx.Done()`).
- Built-in `control.RetrySender` with `RetryPolicy` and `FallbackSender` (DLQ) out of the box.
- Standard adapters for `gpubsub`, `grpcstream`, and `httpstream`.
