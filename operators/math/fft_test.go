package math_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	mathops "github.com/stephen-mcelhose/go-streaming/operators/math"
)

func TestFFT(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST reject windows that are not power of 2", func(t *testing.T) {
		inCh := make(chan []complex128, 1)
		inCh <- make([]complex128, 7) // 7 is not power of 2
		close(inCh)

		outCh := make(chan mathops.SpectrumFrame, 1)
		cfg := mathops.FFTConfig{SampleRateHz: 8000.0}

		err := mathops.StreamingFFT(ctx, cfg, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh))
		require.Error(t, err)
		require.Contains(t, err.Error(), "power of 2")
	})

	t.Run("MUST identify exact peak frequency bin for pure sine wave", func(t *testing.T) {
		const sampleRate = 8000.0
		const targetTone = 1000.0
		const n = 64

		// Generate a 64-sample pure 1000 Hz tone
		samples := make([]complex128, n)
		for i := 0; i < n; i++ {
			tVal := float64(i) / sampleRate
			val := math.Sin(2.0 * math.Pi * targetTone * tVal)
			samples[i] = complex(val, 0)
		}

		inCh := make(chan []complex128, 1)
		inCh <- samples
		close(inCh)

		outCh := make(chan mathops.SpectrumFrame, 1)
		cfg := mathops.FFTConfig{SampleRateHz: sampleRate}

		err := mathops.StreamingFFT(ctx, cfg, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh))
		require.NoError(t, err)
		close(outCh)

		frame := <-outCh
		require.Equal(t, 1, frame.FrameIndex)
		require.Equal(t, n, frame.N)

		// Target frequency (1000 Hz) should be in bin k = 1000 / (8000 / 64) = 8
		require.Equal(t, 8, frame.PeakBin.BinIndex)
		require.InDelta(t, 1000.0, frame.PeakBin.FrequencyHz, 1e-6)
		require.InDelta(t, 1.0, frame.PeakBin.Magnitude, 1e-3)
	})
}

// ExampleStreamingFFT demonstrates analyzing the frequency spectrum of a time-domain signal
// to identify its dominant frequency components (like finding musical notes or carrier tones).
func ExampleStreamingFFT() {
	ctx := context.Background()

	const sampleRate = 8000.0
	const n = 16 // 16-point FFT: bin resolution = 8000 / 16 = 500 Hz per bin

	// Ingress: 16 samples of a 1500 Hz tone (falls on bin k = 3, since 3 * 500 Hz = 1500 Hz)
	chunk := make([]complex128, n)
	for i := 0; i < n; i++ {
		tVal := float64(i) / sampleRate
		val := math.Sin(2.0 * math.Pi * 1500.0 * tVal)
		chunk[i] = complex(val, 0)
	}

	inCh := make(chan []complex128, 1)
	inCh <- chunk
	close(inCh)

	outCh := make(chan mathops.SpectrumFrame, 1)
	cfg := mathops.FFTConfig{SampleRateHz: sampleRate}

	_ = mathops.StreamingFFT(ctx, cfg, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh))
	close(outCh)

	frame := <-outCh
	fmt.Printf("Spectrum Frame #%d (N=%d points):\n", frame.FrameIndex, frame.N)
	fmt.Printf("Dominant Frequency Peak: %.0f Hz (Bin #%d, Amplitude: %.2f)\n",
		frame.PeakBin.FrequencyHz, frame.PeakBin.BinIndex, frame.PeakBin.Magnitude)

	// Output:
	// Spectrum Frame #1 (N=16 points):
	// Dominant Frequency Peak: 1500 Hz (Bin #3, Amplitude: 1.00)
}
