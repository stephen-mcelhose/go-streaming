package examples_test

import (
	"context"
	"fmt"
	"math"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/pure"
)

// MetricTick represents an incoming real-time telemetry observation (e.g. latency, sensor reading, price).
type MetricTick struct {
	Timestamp int64
	Value     float64
}

// ScoredTick represents an enriched feature vector produced by online stream scoring.
type ScoredTick struct {
	Timestamp int64
	Value     float64
	EMA       float64
	Mean      float64
	StdDev    float64
	ZScore    float64
	IsAnomaly bool
}

// onlineStatsAccumulator maintains single-pass running moments (Welford) + EMA.
type onlineStatsAccumulator struct {
	count  int
	mean   float64
	m2     float64
	ema    float64
	alpha  float64
	scored ScoredTick
}

// Example_dataScientist demonstrates how a Data Scientist builds an online, single-pass
// feature engineering and anomaly detection pipeline:
// 1. Ingests high-frequency continuous metric ticks.
// 2. Uses pure.Scan to maintain single-pass running statistics (Welford's algorithm + EMA).
// 3. Scores every incoming tick in real-time with an instantaneous Z-score: Z = (X - Mean) / StdDev.
// 4. Flags anomalies on-the-fly (Z > 2.5) without replaying history or buffering unbounded windows.
func Example_dataScientist() {
	ctx := context.Background()

	// Ingress stream of continuous observations (normal baseline ~50ms, then a shock spike at t=6)
	rawTicks := make(chan MetricTick, 7)
	samples := []float64{50.0, 52.0, 49.0, 51.0, 48.0, 125.0, 53.0}
	for i, v := range samples {
		rawTicks <- MetricTick{Timestamp: int64(1000 + i*10), Value: v}
	}
	close(rawTicks)

	// Step 1: Online single-pass feature extraction & Z-score scoring via pure.Scan
	initialState := onlineStatsAccumulator{alpha: 0.3}

	scoredCh := make(chan onlineStatsAccumulator, 7)
	_ = pure.Scan(ctx, initialState, func(acc onlineStatsAccumulator, tick MetricTick) onlineStatsAccumulator {
		acc.count++
		val := tick.Value

		// Update EMA: EMA_t = alpha * X_t + (1 - alpha) * EMA_{t-1}
		if acc.count == 1 {
			acc.ema = val
		} else {
			acc.ema = acc.alpha*val + (1.0-acc.alpha)*acc.ema
		}

		// Calculate prior standard deviation before updating moments to avoid self-bias
		priorMean := acc.mean
		priorStdDev := 0.0
		if acc.count > 2 && acc.m2 > 0 {
			priorStdDev = math.Sqrt(acc.m2 / float64(acc.count-1))
		}

		// Welford's algorithm for online mean and M2 update
		delta := val - acc.mean
		acc.mean += delta / float64(acc.count)
		delta2 := val - acc.mean
		acc.m2 += delta * delta2

		currentStdDev := 0.0
		if acc.count > 1 {
			currentStdDev = math.Sqrt(acc.m2 / float64(acc.count-1))
		}

		// Instantaneous Z-score scoring against established baseline
		zScore := 0.0
		isAnomaly := false
		if acc.count > 3 && priorStdDev > 0 {
			zScore = (val - priorMean) / priorStdDev
			if zScore > 2.5 {
				isAnomaly = true
			}
		}

		acc.scored = ScoredTick{
			Timestamp: tick.Timestamp,
			Value:     val,
			EMA:       acc.ema,
			Mean:      acc.mean,
			StdDev:    currentStdDev,
			ZScore:    zScore,
			IsAnomaly: isAnomaly,
		}

		return acc
	}, streaming.NewChanStream(rawTicks, nil), streaming.NewChanStream(nil, scoredCh))
	close(scoredCh)

	// Step 2: Transform accumulator stream into pure scored feature stream
	featureCh := make(chan ScoredTick, 7)
	_ = pure.Map(ctx, func(acc onlineStatsAccumulator) ScoredTick {
		return acc.scored
	}, streaming.NewChanStream(scoredCh, nil), streaming.NewChanStream(nil, featureCh))
	close(featureCh)

	// Step 3: Stream consumption & real-time alerting
	for feat := range featureCh {
		if feat.IsAnomaly {
			fmt.Printf("[ALERT] Outlier detected at ts=%d: Value=%.1f (Z-Score=%.2f > 2.5, EMA=%.1f, BaselineMean=%.1f)\n",
				feat.Timestamp, feat.Value, feat.ZScore, feat.EMA, feat.Mean)
		} else {
			fmt.Printf("[NORMAL] ts=%d: Value=%.1f | EMA=%.1f | RunningMean=%.1f\n",
				feat.Timestamp, feat.Value, feat.EMA, feat.Mean)
		}
	}

	// Output:
	// [NORMAL] ts=1000: Value=50.0 | EMA=50.0 | RunningMean=50.0
	// [NORMAL] ts=1010: Value=52.0 | EMA=50.6 | RunningMean=51.0
	// [NORMAL] ts=1020: Value=49.0 | EMA=50.1 | RunningMean=50.3
	// [NORMAL] ts=1030: Value=51.0 | EMA=50.4 | RunningMean=50.5
	// [NORMAL] ts=1040: Value=48.0 | EMA=49.7 | RunningMean=50.0
	// [ALERT] Outlier detected at ts=1050: Value=125.0 (Z-Score=47.43 > 2.5, EMA=72.3, BaselineMean=62.5)
	// [NORMAL] ts=1060: Value=53.0 | EMA=66.5 | RunningMean=61.1
}
