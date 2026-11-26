package pure_test

import (
	"context"
	"fmt"
	"math"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/pure"
)

// Coordinate represents a 2D Cartesian point.
type Coordinate struct {
	X float64
	Y float64
}

// ExampleMap demonstrates pure mathematical Euclidean distance calculation from origin (0, 0).
func ExampleMap() {
	ctx := context.Background()

	points := make(chan Coordinate, 3)
	points <- Coordinate{X: 3, Y: 4}
	points <- Coordinate{X: 5, Y: 12}
	points <- Coordinate{X: 8, Y: 15}
	close(points)

	distances := make(chan float64, 3)

	go func() {
		defer close(distances)
		// Clean mathematical expression without error boilerplate in user callback
		_ = pure.Map(ctx, func(p Coordinate) float64 {
			return math.Sqrt(p.X*p.X + p.Y*p.Y)
		}, streaming.NewChanStream(points, nil), streaming.NewChanStream(nil, distances))
	}()

	for d := range distances {
		fmt.Printf("Distance: %.1f\n", d)
	}

	// Output:
	// Distance: 5.0
	// Distance: 13.0
	// Distance: 17.0
}

// ExampleScan demonstrates calculating running statistical sum across pure numerical observation streams.
func ExampleScan() {
	ctx := context.Background()

	observations := make(chan float64, 3)
	observations <- 10.0
	observations <- 20.0
	observations <- 30.0
	close(observations)

	runningTotals := make(chan float64, 3)

	go func() {
		defer close(runningTotals)
		_ = pure.Scan(ctx, 0.0, func(runningTotal, val float64) float64 {
			return runningTotal + val
		}, streaming.NewChanStream(observations, nil), streaming.NewChanStream(nil, runningTotals))
	}()

	for total := range runningTotals {
		fmt.Printf("Running Total: %.1f\n", total)
	}

	// Output:
	// Running Total: 10.0
	// Running Total: 30.0
	// Running Total: 60.0
}
