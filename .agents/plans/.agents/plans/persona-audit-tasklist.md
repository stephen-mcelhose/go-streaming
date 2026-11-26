# Master Persona & Usability Audit Task List

## Guiding Standard
Every persona example must:
1. **Tell an Authentic End-to-End Story**: Reflect real daily workflows and domain challenges without artificial "replays" or disconnected toy snippets.
2. **Expose No Dead Code / Dangling Sinks**: Every stage in the pipeline must genuinely flow into downstream operators, transformations, sinks, and verified output.
3. **Trigger the Target Reaction**: Make a practitioner in that field look at the code and say: *"Damn, I see exactly how this solves my specific domain pain point."*

---

## Persona Review & Action Plan

- [x] **Data Scientist / Quantitative Analyst** (`examples/data_scientist_example_test.go`)
  - [x] Replaced multi-pass batch replay with single-pass online Welford statistics + EMA.
  - [x] Enriched raw ticks into real-time $Z$-score scored feature vectors via `pure.Scan`.
  - [x] Verified 100% single-pass execution and output.

- [ ] **Mathematician / DSP & Signal Processing Engineer** (`examples/mather_example_test.go`)
  - [ ] **Flaw**: Step 1 calculates `cmplx.Phase` into a dangling unread channel; Step 2 creates an unrelated second channel for sum.
  - [ ] **Refactor**: Build an authentic **IQ Radio Demodulation & Energy Integration Pipeline**:
    1. Ingest complex IQ baseband samples $z[n] = I[n] + jQ[n]$.
    2. Convert to Polar coordinates (Magnitude $|z|$, Instantaneous Phase $\theta = \text{arg}(z)$) using `pure.Map`.
    3. Perform instantaneous frequency discrimination ($\Delta \theta = \theta[n] - \theta[n-1]$) via stateful `pure.Scan`.
    4. Compute total signal power/energy $E = \sum |z[n]|^2$ with `mathops.Sum` and running energy profile with `mathops.RunningSum`.
    5. Print verified outputs for demodulated FM/PM signals and total integrated RF energy.

- [ ] **Software Engineer / Distributed Systems Developer** (`examples/software_engineer_example_test.go`)
  - [ ] **Audit**: Ensure the webhook -> validation -> async worker dispatch -> exponential backoff retry with Dead-Letter Queue (DLQ) fallback cleanly showcases context cancellation, error bubbling, and zero goroutine leaks.
  - [ ] **Enhancement**: Make sure both `Example_softwareEngineer` and any multi-stage workers use standard `streaming.Stream` contracts cleanly with explicit output verification.

- [ ] **Data Engineer / Lakehouse & ETL Platform Engineer** (`examples/data_engineer_example_test.go`)
  - [ ] **Audit**: Verify CDC/JSONL stream -> enrichment -> tenant-based key partitioning -> micro-batching (`aggregation.Batch`) -> Object Storage framing -> multi-partition time-series sorting (`merging.MergeSort`).
  - [ ] **Enhancement**: Connect the streaming stages so the data truly flows from the JSONL adapter through the partitioner into the micro-batcher and into the sorted backfill reader.

- [ ] **Network Engineer / Observability & SecOps Engineer** (`examples/network_engineer_example_test.go`)
  - [ ] **Audit**: Verify raw NetFlow/PCAP stream -> protocol demuxing (`routing.Partition`) -> non-blocking fan-out (`routing.BroadcastBestEffort` to SIEM & IDS) -> alert flood squashing (`control.Throttle` & `control.Debounce`).
  - [ ] **Enhancement**: Ensure timing in tests is deterministic, non-flaky, and cleanly demonstrates burst packet handling and notification throttling.

- [ ] **Global Documentation & ADR Alignment**
  - [ ] Update `README.md` to reflect all updated persona pipelines with crystal-clear code snippets.
  - [ ] Validate `go test -v ./...` and `gofmt -s -w .` across the entire repo.
  - [ ] Commit and push with descriptive conventional commit messages.
