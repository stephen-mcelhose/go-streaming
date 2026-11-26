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

// Example_dataScientist demonstrates an end-to-end stream analytics pipeline:
// 1. Ingests a continuous stream of response latency measurements.
// 2. Applies an Exponential Moving Average (EMA) to smooth out short-term fluctuations.
// 3. Calculates summary statistics (mean and standard deviation) across baseline samples.
// 4. Sets a dynamic threshold (mean + 1.5 * stddev) and filters for outlier latency spikes.
func Example_dataScientist() {
	ctx := context.Background()

	// 1. Receive the incoming stream of latency measurements
	latencies := latencyStream()

	// 2. Smooth the stream in real time with an Exponential Moving Average (EMA).
	// EMA recalculates a weighted running average for every sample as it arrives:
	// EMA_current = (alpha * value) + ((1 - alpha) * EMA_previous).
	// Higher alpha values give more weight to recent measurements.
	emaStream := make(chan float64, 5)
	_ = mathops.EMA(ctx, 0.3, streaming.NewChanStream(latencies, nil), streaming.NewChanStream(nil, emaStream))
	close(emaStream)

	// 3. Compute baseline statistics (mean, variance, standard deviation) across sample data.
	// This uses Welford's algorithm to calculate accurate variance in a single pass without storing all data in memory.
	batchSamples := batchSampleStream()

	statsSink := make(chan mathops.StatsSnapshot, 1)
	_ = mathops.Stats(ctx, streaming.NewChanStream(batchSamples, nil), streaming.NewChanStream(nil, statsSink))
	close(statsSink)

	stats := <-statsSink

	// 4. Filter live traffic against our dynamic threshold to catch anomalies.
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

// latencyStream returns a stream of server response times measured in milliseconds.
// It contains regular requests running near 100ms, plus one sudden slow request at 250ms.
func latencyStream() <-chan float64 {
	latencies := make(chan float64, 5)
	latencies <- 100.0
	latencies <- 105.0
	latencies <- 98.0
	latencies <- 250.0 // Slow request
	latencies <- 102.0
	close(latencies)
	return latencies
}

// batchSampleStream provides a representative set of baseline latency numbers.
// We use this set to establish standard operating metrics (mean and standard deviation).
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

// checkStream provides live incoming requests that we evaluate against our calculated anomaly threshold.
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
