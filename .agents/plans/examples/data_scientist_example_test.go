package examples_test

import (
	"context"
	"fmt"

	"github.com/stephen-mcelhose/go-streaming"
	mathops "github.com/stephen-mcelhose/go-streaming/operators/math"
	"github.com/stephen-mcelhose/go-streaming/operators/pure"
)

// Example_dataScientistWorkflow demonstrates how a Data Scientist / Quantitative Analyst
// builds an online real-time stream analytics and anomaly detection system:
// 1. Ingests high-frequency tick/latency measurements.
// 2. Computes online Exponential Moving Average (EMA) to smooth noise.
// 3. Computes single-pass Welford summary statistics (Mean, Variance, StdDev, Min, Max).
// 4. Filters real-time anomalies using dynamic statistical boundaries (mu + 1.5*sigma).
func Example_dataScientistWorkflow() {
	ctx := context.Background()

	// Ingress stream of financial execution latency observations (in ms)
	latencies := make(chan float64, 5)
	latencies <- 100.0
	latencies <- 105.0
	latencies <- 98.0
	latencies <- 250.0 // Flash latency spike
	latencies <- 102.0
	close(latencies)

	// Step 1: Real-time Exponential Moving Average (EMA) with smoothing weight alpha=0.3
	emaStream := make(chan float64, 5)
	_ = mathops.EMA(ctx, 0.3, streaming.NewChanStream(latencies, nil), streaming.NewChanStream(nil, emaStream))
	close(emaStream)

	// Step 2: Single-pass Welford summary statistics over the observation dataset
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

	// Step 3: Classify anomalies in real-time stream using dynamic statistical threshold
	checkCh := make(chan float64, 5)
	checkCh <- 100.0
	checkCh <- 105.0
	checkCh <- 98.0
	checkCh <- 250.0
	checkCh <- 102.0
	close(checkCh)

	threshold := stats.Mean + 1.5*stats.StdDev
	anomaliesOut := make(chan float64, 5)

	_ = pure.Filter(ctx, func(lat float64) bool {
		return lat > threshold
	}, streaming.NewChanStream(checkCh, nil), streaming.NewChanStream(nil, anomaliesOut))
	close(anomaliesOut)

	for spike := range anomaliesOut {
		fmt.Printf("Alert: Detected Anomaly Spike: %.1fms (threshold: %.1fms)\n", spike, threshold)
	}

	fmt.Printf("Distribution Stats: N=%d | Mean=%.1fms | StdDev=%.1fms | Min=%.1fms | Max=%.1fms\n",
		stats.Count, stats.Mean, stats.StdDev, stats.Min, stats.Max)

	// Output:
	// Alert: Detected Anomaly Spike: 250.0ms (threshold: 230.9ms)
	// Distribution Stats: N=5 | Mean=131.0ms | StdDev=66.6ms | Min=98.0ms | Max=250.0ms
}
