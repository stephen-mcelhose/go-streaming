# Plan: Personas Research, Definition, and Example Alignment

## Context & Objectives
To make `go-streaming` genuinely compelling ("damn, as a [persona] I can see how to use this"), we need to define distinct user personas based on real industry challenges, pain points, mental models, and standard tooling. Only after establishing rigorous persona profiles will we revisit and expand the runnable examples and cookbook documentation to address their specific workflows.

---

## Target Personas

1. **Software Engineer (Backend / Distributed Systems)**
   - *Domain*: Microservices, asynchronous command processing, webhook ingestion, resilient event fan-out, dead-letter routing, backpressure, gRPC / HTTP SSE streaming.
   - *Pain Points*: Managing channel leaks, unhandled panics on channel close, complex retry/backoff boilerplate, wiring dead-letter queues, context cancellation propagation across goroutines.
   - *Key Desires*: Type safety, clean cancellation, predictable lifecycle management (`io.Closer`), error bubbling without crashing worker pools.

2. **Data Engineer (ETL / Event Streaming / Lakehouse Pipelines)**
   - *Domain*: Clickstream processing, CDC ingestion (Debezium/Kafka/PubSub), JSONL/Parquet micro-batching, object storage (S3/GCS) multipart dumping, schema enrichment, partitioned routing.
   - *Pain Points*: High-volume buffer exhaustion, out-of-order records, inefficient single-record network writes vs micro-batching, failure isolation when single records fail in large streams.
   - *Key Desires*: Micro-batching by count/time (`aggregation.Batch`), zero-allocation pipeline transforms, pre-sorted merge (`MergeSort` for time-series backfills), JSONL and broker adapters.

3. **Network / Systems Engineer (Infrastructure / Observability / SecOps)**
   - *Domain*: Packet capture (PCAP/NetFlow/sFlow), syslog streaming, high-frequency telemetry, DDoS alert throttling, multi-destination fan-out (SIEM + incident alerting + log archive).
   - *Pain Points*: Notification storm / alert fatigue during outages, dropped packets on blocking sinks, protocol demuxing, fire-and-forget logging without slowing traffic processing.
   - *Key Desires*: `BroadcastBestEffort` (non-blocking telemetry replication), `Throttle` and `Debounce` (alert squashing), `Partition` by protocol/subnet, high throughput with low latency.

4. **Mathematician / DSP & Signal Processing Engineer**
   - *Domain*: Digital signal processing (DSP), FFT/frequency domain transformations, phase angle calculations, complex arithmetic ($z = a + bi$), audio/RF filtering, numerical stability.
   - *Pain Points*: Error-tuple clutter `(float64, error)` obscuring mathematical formulas, lack of native support for Go `complex64`/`complex128`, cumbersome vector operations.
   - *Key Desires*: Pure function mapping `pure.Map` ($f(x) \to y$), clean linear vector accumulation `math.Sum`, high numerical precision without error wrapping inside formulas.

5. **Data Scientist / Quantitative Analyst (Online ML / Streaming Stats)**
   - *Domain*: Online feature engineering, real-time metrics, streaming anomaly detection, running Exponential Moving Average (EMA), Welford's algorithm for numerically stable running variance/standard deviation, dynamic threshold triggering.
   - *Pain Points*: Inability to compute single-pass statistical moments over unbounded streams without loading history into memory, numerical drift in variance calculations.
   - *Key Desires*: Single-pass online algorithms (`math.Stats`, `math.EMA`), running state emission (`pure.Scan`), dynamic filtering against rolling stats.

---

## Action Plan

### Step 1: Write Persona Documentation
- Create `.agents/personas/` directory.
- Create deep persona specification files:
  - `.agents/personas/software-engineer.md`
  - `.agents/personas/data-engineer.md`
  - `.agents/personas/network-engineer.md`
  - `.agents/personas/mathematician-dsp.md`
  - `.agents/personas/data-scientist.md`

### Step 2: Revisit & Enhance Runnable Examples
- Build comprehensive, realistic end-to-end runnable scenarios in `examples/` aligned directly with each persona's specific workflow:
  - `software_engineer_example_test.go`: Webhook receiver -> validation -> async worker pool -> exponential retry with DLQ + gRPC push.
  - `data_engineer_example_test.go`: CDC log ingestion -> partition by tenant -> micro-batching -> GCS/S3 JSONL staging -> sorted multi-partition backfill (`MergeSort`).
  - `network_engineer_example_test.go`: NetFlow packet capture -> protocol demuxing -> alert burst debouncing & rate limiting -> broadcast to SIEM & incident pager.
  - `mather_example_test.go`: Complex RF/audio sample stream -> phase/magnitude transformation -> vector power sum -> FIR filter emulation with `pure.Scan`.
  - `data_scientist_example_test.go`: Live stock/financial tick stream -> rolling EMA calculation -> Welford variance & Z-score thresholding -> online anomaly alert stream.

### Step 3: Update README and Documentation
- Document the persona guide and cookbook in `README.md`.
- Verify all tests and examples compile and pass cleanly.
- Commit using conventional commits and push.
