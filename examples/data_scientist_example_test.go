package examples_test

import (
	"context"
	"fmt"

	"github.com/stephen-mcelhose/go-streaming"
	mathops "github.com/stephen-mcelhose/go-streaming/operators/math"
	"github.com/stephen-mcelhose/go-streaming/operators/pure"
)

// MetricObservation represents an incoming telemetry or business observation.
type MetricObservation struct {
	UserID    string
	LatencyMS float64
}

// Example_dataScientist demonstrates how a Data Scientist performs continuous online anomaly
// detection using Exponential Moving Average (EMA) and summarizes batch distribution metrics (Welford Stats).
func Example_dataScientist() {
	ctx := context.Background()

	// Ingress stream of latency measurements
	latencies := latencyStream()

	// Step 1: Compute real-time Exponential Moving Average (EMA).
	// EMA is a stateful Scan operation (recurrence: EMA_t = alpha * X_t + (1 - alpha) * EMA_{t-1})
	// rather than a stateless 1:1 Map. It continuously recalculates and emits an updated smoothed
	// metric for every single incoming observation as it arrives on the stream.
	emaStream := make(chan float64, 5)
	_ = mathops.EMA(ctx, 0.3, streaming.NewChanStream(latencies, nil), streaming.NewChanStream(nil, emaStream))
	close(emaStream)

	// Step 2: Compute full summary statistics (mean, variance, stddev) over baseline samples using Welford's algorithm
	batchSamples := batchSampleStream()

	statsSink := make(chan mathops.StatsSnapshot, 1)
	_ = mathops.Stats(ctx, streaming.NewChanStream(batchSamples, nil), streaming.NewChanStream(nil, statsSink))
	close(statsSink)

	stats := <-statsSink

	// Step 3: Classify anomalies using pure transformation
	anomalyCount := 0
	checkCh := checkStream()

	threshold := stats.Mean + 1.5*stats.StdDev
	outAnomalies := make(chan float64, 5)

	_ = pure.Filter(ctx, func(lat float64) bool {
		return lat > threshold
	}, streaming.NewChanStream(checkCh, nil), streaming.NewChanStream(nil, outAnomalies))
	close(outAnomalies)

	for anom := range outAnomalies {
		anomalyCount++
		fmt.Printf("Detected Anomaly: %.1fms (threshold: %.1fms)\n", anom, threshold)
	}

	fmt.Printf("Dataset Stats: Count=%d | Mean=%.1fms | StdDev=%.1fms\n", stats.Count, stats.Mean, stats.StdDev)

	// Output:
	// Detected Anomaly: 250.0ms (threshold: 230.9ms)
	// Dataset Stats: Count=5 | Mean=131.0ms | StdDev=66.6ms
}

// latencyStream generates a live stream of service execution latencies in milliseconds.
// For Data Scientists and ML Engineers, this simulates incoming real-time telemetry from production
// inference microservices or API gateways, containing steady-state operations (~100ms) interrupted
// by an abrupt flash degradation (250.0ms latency spike).
func latencyStream() <-chan float64 {
	latencies := make(chan float64, 5)
	latencies <- 100.0
	latencies <- 105.0
	latencies <- 98.0
	latencies <- 250.0 // Flash latency spike
	latencies <- 102.0
	close(latencies)
	return latencies
}

// batchSampleStream provides a historical window or calibration partition of metric observations.
// For Data Scientists, this models the bounded reference dataset used to estimate population parameters
// (sample mean mu and standard deviation sigma) via single-pass Welford moments without caching arrays in RAM.
func batchSampleStream() <-chan float64 {
	batchSamples := make(chan float64, 5)
	batchSamples <- 100.0
	batchSamples <- 105.0
	batchSamples <- 98.0
	batchSamples <- 250.0
	batchSamples <- 102.0
	close(batchSamples)
	return batchSamples
}

// checkStream provides the real-time scoring stream evaluated against dynamic statistical thresholds.
// In streaming feature engineering and online quality monitoring, each point in this stream is tested
// against an empirical boundary (e.g. mu + 1.5*sigma) to detect covariate drift or SLA violations on-the-fly.
func checkStream() <-chan float64 {
	checkCh := make(chan float64, 5)
	checkCh <- 100.0
	checkCh <- 105.0
	checkCh <- 98.0
	checkCh <- 250.0
	checkCh <- 102.0
	close(checkCh)
	return checkCh
}
