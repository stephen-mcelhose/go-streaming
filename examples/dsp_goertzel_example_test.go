package examples_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/pure"
)

// GoertzelConfig configures single-frequency tone detection parameters.
type GoertzelConfig struct {
	TargetFreqHz float64 // The specific tone frequency we want to detect (e.g. 1000 Hz)
	SampleRateHz float64 // Sample rate of the incoming audio/sensor stream (e.g. 8000 Hz)
	BlockSize    int     // Number of samples to evaluate per detection window (N)
}

// ToneDetection holds the estimated power and presence of the target tone in a block.
type ToneDetection struct {
	BlockIndex     int
	TargetFreqHz   float64
	EstimatedPower float64
	NormalizedMag  float64
	ToneDetected   bool
}

// goertzelState maintains the 2nd-order IIR filter state for the Goertzel algorithm.
type goertzelState struct {
	coeff      float64
	s1         float64
	s2         float64
	count      int
	blockIdx   int
	blockSize  int
	targetFreq float64
	detection  ToneDetection
	hasBlock   bool
}

// StreamingGoertzel detects whether a specific tone frequency is present in an audio/signal stream
// without needing to compute a full, heavy Fast Fourier Transform (FFT).
func StreamingGoertzel(
	ctx context.Context,
	cfg GoertzelConfig,
	powerThreshold float64,
	in streaming.Receiver[float64],
	out streaming.Sender[ToneDetection],
) error {
	omega := 2.0 * math.Pi * (cfg.TargetFreqHz / cfg.SampleRateHz)
	coeff := 2.0 * math.Cos(omega)

	initial := goertzelState{
		coeff:      coeff,
		blockSize:  cfg.BlockSize,
		targetFreq: cfg.TargetFreqHz,
	}

	stateCh := make(chan goertzelState, 5)

	go func() {
		defer close(stateCh)
		_ = pure.Scan(ctx, initial, func(st goertzelState, sample float64) goertzelState {
			st.hasBlock = false

			// 2nd-order IIR Goertzel recurrence: s0 = x[n] + coeff*s1 - s2
			s0 := sample + st.coeff*st.s1 - st.s2
			st.s2 = st.s1
			st.s1 = s0
			st.count++

			if st.count >= st.blockSize {
				// Compute power at block boundary: P = s1^2 + s2^2 - coeff*s1*s2
				power := st.s1*st.s1 + st.s2*st.s2 - st.coeff*st.s1*st.s2
				normMag := (2.0 * math.Sqrt(power)) / float64(st.blockSize)

				st.blockIdx++
				st.detection = ToneDetection{
					BlockIndex:     st.blockIdx,
					TargetFreqHz:   st.targetFreq,
					EstimatedPower: power,
					NormalizedMag:  normMag,
					ToneDetected:   normMag >= powerThreshold,
				}
				st.hasBlock = true

				// Reset filter state for next block
				st.s1 = 0
				st.s2 = 0
				st.count = 0
			}

			return st
		}, in, streaming.NewChanStream(nil, stateCh))
	}()

	for st := range stateCh {
		if st.hasBlock {
			if err := out.Send(ctx, st.detection); err != nil {
				return err
			}
		}
	}

	return nil
}

func TestGoertzel(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST detect presence of matching frequency and reject non-matching tone", func(t *testing.T) {
		const sampleRate = 8000.0
		const targetFreq = 1000.0
		const blockSize = 80 // 10ms block

		cfg := GoertzelConfig{
			TargetFreqHz: targetFreq,
			SampleRateHz: sampleRate,
			BlockSize:    blockSize,
		}

		inCh := make(chan float64, blockSize*2)

		// Block 1: Pure 1000 Hz sine wave (amplitude = 1.0)
		for n := 0; n < blockSize; n++ {
			tVal := float64(n) / sampleRate
			inCh <- math.Sin(2.0 * math.Pi * targetFreq * tVal)
		}

		// Block 2: 250 Hz sine wave (off-target tone)
		for n := 0; n < blockSize; n++ {
			tVal := float64(n) / sampleRate
			inCh <- math.Sin(2.0 * math.Pi * 250.0 * tVal)
		}
		close(inCh)

		outCh := make(chan ToneDetection, 2)
		err := StreamingGoertzel(ctx, cfg, 0.5, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh))
		require.NoError(t, err)
		close(outCh)

		var detections []ToneDetection
		for d := range outCh {
			detections = append(detections, d)
		}

		require.Equal(t, 2, len(detections))
		// Block 1 should detect target tone with normalized magnitude near 1.0
		require.True(t, detections[0].ToneDetected)
		require.InDelta(t, 1.0, detections[0].NormalizedMag, 0.15)

		// Block 2 should reject off-target frequency
		require.False(t, detections[1].ToneDetected)
		require.InDelta(t, 0.0, detections[1].NormalizedMag, 0.15)
	})
}

// Example_dspGoertzel demonstrates using the Goertzel algorithm to detect whether a 1000 Hz dial-tone
// is present in a stream of audio samples without running an expensive full FFT.
func Example_dspGoertzel() {
	ctx := context.Background()

	const sampleRate = 8000.0
	const targetFreq = 1000.0
	const blockSize = 80

	cfg := GoertzelConfig{
		TargetFreqHz: targetFreq,
		SampleRateHz: sampleRate,
		BlockSize:    blockSize,
	}

	samples := make(chan float64, blockSize*2)

	// Generate Block 1: 1000 Hz tone (active dial tone)
	for n := 0; n < blockSize; n++ {
		tVal := float64(n) / sampleRate
		samples <- math.Sin(2.0 * math.Pi * 1000.0 * tVal)
	}

	// Generate Block 2: silence (noise near 0)
	for n := 0; n < blockSize; n++ {
		samples <- 0.01
	}
	close(samples)

	detections := make(chan ToneDetection, 2)
	_ = StreamingGoertzel(ctx, cfg, 0.5, streaming.NewChanStream(samples, nil), streaming.NewChanStream(nil, detections))
	close(detections)

	for det := range detections {
		fmt.Printf("Block #%d (%.0f Hz): Detected=%t (Magnitude: %.2f)\n",
			det.BlockIndex, det.TargetFreqHz, det.ToneDetected, det.NormalizedMag)
	}

	// Output:
	// Block #1 (1000 Hz): Detected=true (Magnitude: 1.00)
	// Block #2 (1000 Hz): Detected=false (Magnitude: 0.00)
}
