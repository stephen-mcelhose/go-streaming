# Persona: Network / Systems Engineer (Infrastructure / Observability / SecOps)

## Profile
- **Role**: Network Engineer / Observability & SecOps Engineer
- **Focus**: Packet inspection, NetFlow/sFlow ingestion, syslog streaming, real-time threat detection, and telemetry fan-out.
- **Primary Tools**: Go, PCAP / eBPF, Syslog, Prometheus, SIEM (Elasticsearch, Splunk), PagerDuty / Webhook alert managers.

## Daily Realities & Workflows
- Capturing live packets or NetFlow telemetry from network interfaces and demuxing traffic by protocol (TCP, UDP, ICMP) or subnet.
- Replicating critical telemetry concurrently across multiple destinations: primary SIEM analytics, long-term compliance storage, and real-time IDS monitoring.
- Managing alert storms during network incidents (e.g., SYN floods or port scans) by debouncing burst events and rate-limiting outbound webhook notifications to incident response pagers.

## Pain Points with Existing Solutions
- **Backpressure Blocking Critical Pipelines**: If one slow downstream destination (like a cold archival store) blocks, it stalls the entire packet capture loop and causes kernel packet drops.
- **Alert Fatigue & Pager Flooding**: Burst incidents generate thousands of repetitive alerts per second, overwhelming on-call engineers and rate-limiting notification APIs.
- **Protocol Demuxing Boilerplate**: Writing bespoke goroutine dispatchers to route packets to specialized worker pools based on headers.

## What Makes Them Say: *"Damn, I can see how to use this"*
- `routing.BroadcastBestEffort`: Non-blocking, fire-and-forget telemetry fan-out ensuring a slow SIEM logger never stalls the core network pipeline.
- `control.Throttle` & `control.Debounce`: Native operators to space incident pages and consolidate burst alarms into quiet-window triggers.
- `routing.Partition`: Demuxing network packets by protocol or port with default/unclassified fallback routing.
