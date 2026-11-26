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

// RawPolar represents an IQ baseband sample converted into polar magnitude and phase.
type RawPolar struct {
	Index     int
	Magnitude float64
	Phase     float64
	Power     float64
}

// PolarSample represents a demodulated complex baseband IQ sample in polar coordinates.
type PolarSample struct {
	Index        int
	Magnitude    float64 // Envelope amplitude |z|
	PhaseRad     float64 // Phase angle in radians [-pi, pi]
	InstPower    float64 // Instantaneous power |z|^2
	DeltaPhaseHz float64 // Discrete frequency discriminator: Delta theta
}

// phaseDiscriminatorState tracks prior sample phase for frequency demodulation.
type phaseDiscriminatorState struct {
	prevPhase float64
	hasPrev   bool
	sample    PolarSample
}

// Example_mather demonstrates how a Mathematician / DSP (Digital Signal Processing) Engineer
// processes complex baseband RF/audio IQ streams (complex128):
// 1. Ingests raw baseband In-phase / Quadrature (IQ) analytic signal samples: z[n] = I[n] + jQ[n].
// 2. Applies pure.Map without error-tuple overhead to compute polar magnitude & instantaneous power.
// 3. Implements single-pass FM/PM phase discrimination via pure.Scan to extract instantaneous frequency shifts.
// 4. Computes total integrated signal energy across the RF burst using mathops.Sum.
func Example_mather() {
	ctx := context.Background()

	// Ingress stream of complex baseband IQ samples
	iqCh := iqPulseStream()

	// Step 1: Demodulate IQ samples into Polar Coordinates (Magnitude, Phase, Instantaneous Power)
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

	// Step 2: Instantaneous Frequency Demodulation via pure.Scan
	// Frequency discriminator: Delta theta = unwrap(theta[n] - theta[n-1])
	discriminatedCh := make(chan phaseDiscriminatorState, 4)
	_ = pure.Scan(ctx, phaseDiscriminatorState{}, func(st phaseDiscriminatorState, raw RawPolar) phaseDiscriminatorState {
		var deltaPhase float64
		if st.hasPrev {
			delta := raw.Phase - st.prevPhase
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

	// Step 3: Stream consumption & Demodulated Telemetry Output
	powerValues := make(chan float64, 4)
	fmt.Println("Demodulated Baseband Stream:")
	for state := range discriminatedCh {
		s := state.sample
		powerValues <- s.InstPower
		fmt.Printf("Sample %d: |z|=%.2f, Phase=%.2f rad, Power=%.1f, DeltaPhase=%.2f rad/sample\n",
			s.Index, s.Magnitude, s.PhaseRad, s.InstPower, s.DeltaPhaseHz)
	}
	close(powerValues)

	// Step 4: Total Integrated RF Pulse Energy Calculation via mathops.Sum
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

// iqPulseStream generates an analytic RF IQ pulse burst (complex128):
// - Sample 0: Carrier baseline (1.0 + 0.0i) -> |z|=1.0, Phase=0
// - Sample 1: Chirp phase shift (0.0 + 2.0i) -> |z|=2.0, Phase=+pi/2 (+1.57 rad)
// - Sample 2: Further phase shift (-3.0 + 0.0i) -> |z|=3.0, Phase=+pi (+3.14 rad)
// - Sample 3: High power burst (3.0 + 4.0i) -> |z|=5.0, Phase=+0.93 rad
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
