package math_test

import (
	"context"
	"fmt"

	"github.com/stephen-mcelhose/go-streaming"
	mathops "github.com/stephen-mcelhose/go-streaming/operators/math"
)

// ExampleRunningSum demonstrates computing a running cumulative sum across a live financial ticker stream.
func ExampleRunningSum() {
	ctx := context.Background()

	ticks := make(chan float64, 3)
	ticks <- 100.50
	ticks <- -25.25
	ticks <- 50.00
	close(ticks)

	runningTotals := make(chan float64, 3)

	go func() {
		defer close(runningTotals)
		_ = mathops.RunningSum(ctx, streaming.NewChanStream(ticks, nil), streaming.NewChanStream(nil, runningTotals))
	}()

	for val := range runningTotals {
		fmt.Printf("Cumulative Position: $%.2f\n", val)
	}

	// Output:
	// Cumulative Position: $100.50
	// Cumulative Position: $75.25
	// Cumulative Position: $125.25
}

// ExampleStats demonstrates calculating summary statistics (mean, variance, min/max) over a sensor batch.
func ExampleStats() {
	ctx := context.Background()

	voltages := make(chan float64, 4)
	voltages <- 3.2
	voltages <- 3.3
	voltages <- 3.1
	voltages <- 3.6
	close(voltages)

	statsOut := make(chan mathops.StatsSnapshot, 1)

	go func() {
		defer close(statsOut)
		_ = mathops.Stats(ctx, streaming.NewChanStream(voltages, nil), streaming.NewChanStream(nil, statsOut))
	}()

	s := <-statsOut
	fmt.Printf("Readings: %d | Mean: %.2fV | Min: %.2fV | Max: %.2fV\n", s.Count, s.Mean, s.Min, s.Max)

	// Output:
	// Readings: 4 | Mean: 3.30V | Min: 3.10V | Max: 3.60V
}
