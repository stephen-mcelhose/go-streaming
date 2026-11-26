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
	latencies := make(chan float64, 5)
	latencies <- 100.0
	latencies <- 105.0
	latencies <- 98.0
	latencies <- 250.0 // Latency spike
	latencies <- 102.0
	close(latencies)

	// Step 1: Compute real-time Exponential Moving Average (EMA)
	emaStream := make(chan float64, 5)
	_ = mathops.EMA(ctx, 0.3, streaming.NewChanStream(latencies, nil), streaming.NewChanStream(nil, emaStream))
	close(emaStream)

	// Step 2: Compute full summary statistics (mean, variance, stddev) over the batch
	batchSamples := make(chan float64, 5)
	batchSamples <- 100.0
	batchSamples <- 105.0
	batchSamples <- 98.0
	batchSamples <- 250.0
	batchSamples <- 102.0
	close(batchSamples)

	statsSink := make(chan mathops.StatsSnapshot, 1)
	_ = mathops.Stats(ctx, streaming.NewChanStream(batchSamples, nil), streaming.NewChanStream(nil, statsSink))
	close(statsSink)

	stats := <-statsSink

	// Step 3: Classify anomalies using pure transformation
	anomalyCount := 0
	checkCh := make(chan float64, 5)
	checkCh <- 100.0
	checkCh <- 105.0
	checkCh <- 98.0
	checkCh <- 250.0
	checkCh <- 102.0
	close(checkCh)

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
