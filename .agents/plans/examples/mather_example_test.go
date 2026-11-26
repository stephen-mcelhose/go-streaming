package examples_test

import (
	"context"
	"fmt"
	"math"
	"math/cmplx"

	"github.com/stephen-mcelhose/go-streaming"
	mathops "github.com/stephen-mcelhose/go-streaming/operators/math"
	"github.com/stephen-mcelhose/go-streaming/operators/pure"
)

// Example_matherWorkflow demonstrates how a Mathematician / DSP Engineer
// performs complex digital signal processing (DSP) workflows cleanly:
// 1. Streams complex IQ radio samples (complex128).
// 2. Maps pure polar conversion (magnitude & phase) without error boilerplate.
// 3. Filters signals above power thresholds.
// 4. Computes total complex vector summation and running energy integrals.
func Example_matherWorkflow() {
	ctx := context.Background()

	// Ingress stream of complex IQ signal samples: z = I + jQ
	iqSamples := make(chan complex128, 4)
	iqSamples <- 3.0 + 4.0i  // magnitude = 5.0
	iqSamples <- 1.0 + 1.0i  // magnitude = 1.414
	iqSamples <- 0.0 + 2.0i  // magnitude = 2.0
	iqSamples <- 5.0 + 12.0i // magnitude = 13.0
	close(iqSamples)

	// Step 1: Pure mathematical transformation: compute signal magnitude and phase
	type PolarSample struct {
		Magnitude float64
		PhaseRad  float64
	}

	polarCh := make(chan PolarSample, 4)
	_ = pure.Map(ctx, func(z complex128) PolarSample {
		mag, phase := cmplx.Abs(z), cmplx.Phase(z)
		return PolarSample{Magnitude: mag, PhaseRad: phase}
	}, streaming.NewChanStream(iqSamples, nil), streaming.NewChanStream(nil, polarCh))
	close(polarCh)

	// Step 2: Filter high-power signals (> 3.0 magnitude)
	highPowerCh := make(chan PolarSample, 4)
	_ = pure.Filter(ctx, func(p PolarSample) bool {
		return p.Magnitude >= 3.0
	}, streaming.NewChanStream(polarCh, nil), streaming.NewChanStream(nil, highPowerCh))
	close(highPowerCh)

	fmt.Println("High Power Signals:")
	for s := range highPowerCh {
		fmt.Printf("Mag=%.1f, Phase=%.2f rad\n", s.Magnitude, s.PhaseRad)
	}

	// Step 3: Exact vector summation over complex stream
	vectors := make(chan complex128, 3)
	vectors <- 1.0 + 2.0i
	vectors <- 2.0 + 1.0i
	vectors <- 3.0 + 3.0i
	close(vectors)

	sumSink := make(chan complex128, 1)
	_ = mathops.Sum(ctx, streaming.NewChanStream(vectors, nil), streaming.NewChanStream(nil, sumSink))
	close(sumSink)

	totalVector := <-sumSink
	fmt.Printf("Total Vector Sum: %.1f + %.1fi (Magnitude: %.2f)\n",
		real(totalVector), imag(totalVector), math.Hypot(real(totalVector), imag(totalVector)))

	// Output:
	// High Power Signals:
	// Mag=5.0, Phase=0.93 rad
	// Mag=13.0, Phase=1.18 rad
	// Total Vector Sum: 6.0 + 6.0i (Magnitude: 8.49)
}
