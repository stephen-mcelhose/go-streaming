# Persona: Data Engineer (ETL / Event Streaming / Lakehouse Pipelines)

## Profile
- **Role**: Data Engineer / Analytics Platform Engineer
- **Focus**: Real-time ETL, Change Data Capture (CDC), clickstream ingestion, log compaction, Lakehouse staging (S3/GCS), and multi-partition data ordering.
- **Primary Tools**: Go, Kafka, Google Cloud Pub/Sub, S3 / Google Cloud Storage, JSONL, Parquet, Snowflake / BigQuery.

## Daily Realities & Workflows
- Ingesting high-throughput change-data streams or raw clickstreams from brokers.
- Cleaning, filtering, and enriching unstructured payloads into strict domain schemas.
- Micro-batching high-volume single events by count ($N$ records) or timeout ($T$ ms) to optimize network efficiency when writing to object storage or analytical data warehouses.
- Handling out-of-order partitioned events and merging pre-sorted historical partition files into globally sorted backfill streams.

## Pain Points with Existing Solutions
- **Memory Buffer Exhaustion**: Unbounded memory buffering causes OOM crashes under bursty traffic when downstream network sinks are slow.
- **Inefficient Single-Record I/O**: Sending single records over HTTP/cloud storage APIs generates high overhead and costs compared to micro-batching.
- **Complex Multi-Partition Sorting**: Merging sorted event logs from multiple partitions (e.g., Kafka partitions with timestamps) requires custom priority queue / heap implementations that are error-prone.

## What Makes Them Say: *"Damn, I can see how to use this"*
- `aggregation.Batch` with hybrid count and timeout triggers (`MaxSize` + `MaxWait`) for micro-batching.
- `merging.MergeSort` which merges pre-sorted partitions via an efficient min-heap comparator out of the box.
- `adapters/jsonl` and `adapters/objectstorage` for direct JSON Lines streaming to and from cloud object stores.
- `operators/routing.Partition` to shard records by tenant or key into downstream warehouse queues.
