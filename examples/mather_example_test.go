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

// RawPolar represents a complex number converted into polar coordinates (magnitude, phase angle, and power).
type RawPolar struct {
	Index     int
	Magnitude float64
	Phase     float64
	Power     float64
}

// PolarSample represents a processed signal sample including the change in phase from the previous sample.
type PolarSample struct {
	Index        int
	Magnitude    float64 // Signal amplitude |z|
	PhaseRad     float64 // Phase angle in radians
	InstPower    float64 // Signal power (|z|^2)
	DeltaPhaseHz float64 // Change in phase angle compared to the previous sample
}

// phaseDiscriminatorState stores the previous phase value to measure step-by-step changes.
type phaseDiscriminatorState struct {
	prevPhase float64
	hasPrev   bool
	sample    PolarSample
}

// Example_mather demonstrates how to process a stream of complex numbers:
// 1. Ingests a stream of complex signal values (z = real + imaginary * i).
// 2. Uses pure.Map to convert each value to polar form (magnitude, phase, and power) without needing extra error returns.
// 3. Uses pure.Scan to track changes in phase between consecutive samples.
// 4. Uses mathops.Sum to calculate the total power across all samples in the stream.
func Example_mather() {
	ctx := context.Background()

	// Ingress stream of complex numbers
	iqCh := iqPulseStream()

	// Step 1: Convert complex numbers to polar form (Magnitude, Phase, and Power)
	polarCh := make(chan RawPolar, 4)
	var sampleIdx int
	_ = pure.Map(ctx, func(z complex128) RawPolar {
		mag := cmplx.Abs(z)
		phase := cmplx.Phase(z)
		power := mag * mag
		out := RawPolar{Index: sampleIdx, Magnitude: mag, Phase: phase, Power: power}
		sampleIdx++
		return out
	}, streaming.NewChanStream(iqCh, nil), streaming.NewChanStream(nil, polarCh))
	close(polarCh)

	// Step 2: Track phase differences between consecutive samples using pure.Scan.
	// pure.Scan keeps running state from one item to the next.
	discriminatedCh := make(chan phaseDiscriminatorState, 4)
	_ = pure.Scan(ctx, phaseDiscriminatorState{}, func(st phaseDiscriminatorState, raw RawPolar) phaseDiscriminatorState {
		var deltaPhase float64
		if st.hasPrev {
			delta := raw.Phase - st.prevPhase
			// Normalize phase difference so it stays within the range [-pi, +pi]
			for delta > math.Pi {
				delta -= 2 * math.Pi
			}
			for delta < -math.Pi {
				delta += 2 * math.Pi
			}
			deltaPhase = delta
		}

		st.hasPrev = true
		st.prevPhase = raw.Phase
		st.sample = PolarSample{
			Index:        raw.Index,
			Magnitude:    raw.Magnitude,
			PhaseRad:     raw.Phase,
			InstPower:    raw.Power,
			DeltaPhaseHz: deltaPhase,
		}
		return st
	}, streaming.NewChanStream(polarCh, nil), streaming.NewChanStream(nil, discriminatedCh))
	close(discriminatedCh)

	// Step 3: Print each calculated sample and collect power values for summation
	powerValues := make(chan float64, 4)
	fmt.Println("Demodulated Baseband Stream:")
	for state := range discriminatedCh {
		s := state.sample
		powerValues <- s.InstPower
		fmt.Printf("Sample %d: |z|=%.2f, Phase=%.2f rad, Power=%.1f, DeltaPhase=%.2f rad/sample\n",
			s.Index, s.Magnitude, s.PhaseRad, s.InstPower, s.DeltaPhaseHz)
	}
	close(powerValues)

	// Step 4: Sum all power values to find total energy across the entire sequence
	totalEnergySink := make(chan float64, 1)
	_ = mathops.Sum(ctx, streaming.NewChanStream(powerValues, nil), streaming.NewChanStream(nil, totalEnergySink))
	close(totalEnergySink)

	totalEnergy := <-totalEnergySink
	fmt.Printf("Total Integrated Pulse Energy: %.1f Joules (normalized)\n", totalEnergy)

	// Output:
	// Demodulated Baseband Stream:
	// Sample 0: |z|=1.00, Phase=0.00 rad, Power=1.0, DeltaPhase=0.00 rad/sample
	// Sample 1: |z|=2.00, Phase=1.57 rad, Power=4.0, DeltaPhase=1.57 rad/sample
	// Sample 2: |z|=3.00, Phase=3.14 rad, Power=9.0, DeltaPhase=1.57 rad/sample
	// Sample 3: |z|=5.00, Phase=0.93 rad, Power=25.0, DeltaPhase=-2.21 rad/sample
	// Total Integrated Pulse Energy: 39.0 Joules (normalized)
}

// iqPulseStream returns a four-sample stream of complex numbers (z = real + imaginary * i):
// - Sample 0: 1.0 + 0.0i -> Magnitude = 1.0, Phase = 0.0 radians
// - Sample 1: 0.0 + 2.0i -> Magnitude = 2.0, Phase = +1.57 radians (+pi/2)
// - Sample 2: -3.0 + 0.0i -> Magnitude = 3.0, Phase = +3.14 radians (+pi)
// - Sample 3: 3.0 + 4.0i -> Magnitude = 5.0, Phase = +0.93 radians
func iqPulseStream() <-chan complex128 {
	iqCh := make(chan complex128, 4)
	samples := []complex128{
		1.0 + 0.0i,
		0.0 + 2.0i,
		-3.0 + 0.0i,
		3.0 + 4.0i,
	}
	for _, s := range samples {
		iqCh <- s
	}
	close(iqCh)
	return iqCh
}
