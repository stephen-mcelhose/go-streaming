package math

import (
	"context"
	"fmt"
	"math"
	"math/cmplx"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

// SpectrumBin represents an evaluated discrete frequency bin in the Fourier spectrum.
type SpectrumBin struct {
	BinIndex     int        // Frequency bin index k (0 <= k < N/2)
	FrequencyHz  float64    // Center frequency in Hertz: k * (SampleRate / N)
	ComplexVal   complex128 // Raw complex Fourier coefficient X[k]
	Magnitude    float64    // Normalized amplitude spectrum: (2 / N) * |X[k]|
	Power        float64    // Power spectrum |X[k]|^2
	PhaseRadians float64    // Phase angle in radians: arg(X[k])
}

// SpectrumFrame represents the complete Fast Fourier Transform (FFT) analysis of an N-point window.
type SpectrumFrame struct {
	FrameIndex int
	N          int
	Bins       []SpectrumBin
	PeakBin    SpectrumBin
}

// FFTConfig configures the streaming Fast Fourier Transform operator.
type FFTConfig struct {
	SampleRateHz float64 // Sampling frequency of the incoming discrete signal (e.g. 8000 Hz)
}

// CooleyTukeyFFT computes the 1D Radix-2 Decimation-in-Time Fast Fourier Transform (FFT) in O(N log N) time.
//
// Mathematical Formulation:
//
//	X[k] = \sum_{n=0}^{N-1} x[n] \cdot e^{-j 2\pi k n / N}
//	     = E[k] + e^{-j 2\pi k / N} \cdot O[k]
//
// Academic Citations:
//   - J. W. Cooley, J. W. Tukey, "An algorithm for the machine calculation of complex Fourier series",
//     Mathematics of Computation, 19(90):297–301, 1965. https://doi.org/10.1090/S0025-5718-1965-0178586-1
//   - Wikipedia: https://en.wikipedia.org/wiki/Cooley%E2%80%93Tukey_FFT_algorithm
func CooleyTukeyFFT(samples []complex128) []complex128 {
	n := len(samples)
	if n <= 1 {
		out := make([]complex128, n)
		copy(out, samples)
		return out
	}

	// Radix-2 divide and conquer: split into even and odd indices
	even := make([]complex128, n/2)
	odd := make([]complex128, n/2)
	for i := 0; i < n/2; i++ {
		even[i] = samples[2*i]
		odd[i] = samples[2*i+1]
	}

	fftEven := CooleyTukeyFFT(even)
	fftOdd := CooleyTukeyFFT(odd)

	out := make([]complex128, n)
	for k := 0; k < n/2; k++ {
		angle := -2.0 * math.Pi * float64(k) / float64(n)
		twiddle := cmplx.Rect(1.0, angle) * fftOdd[k]
		out[k] = fftEven[k] + twiddle
		out[k+n/2] = fftEven[k] - twiddle
	}
	return out
}

// StreamingFFT transforms rolling windows of time-domain signal chunks into full frequency SpectrumFrames.
// Windows MUST have length N as a power of 2 (e.g. 8, 16, 32, 64, 128, 256, 512, 1024).
//
// Academic Citations:
//   - J. W. Cooley, J. W. Tukey, "An algorithm for the machine calculation of complex Fourier series",
//     Mathematics of Computation, 1965. https://doi.org/10.1090/S0025-5718-1965-0178586-1
func StreamingFFT(
	ctx context.Context,
	cfg FFTConfig,
	in streaming.Receiver[[]complex128],
	out streaming.Sender[SpectrumFrame],
) error {
	var frameIdx int

	return transformations.Map(ctx, func(ctx context.Context, chunk []complex128) (SpectrumFrame, error) {
		n := len(chunk)
		if n == 0 || (n&(n-1)) != 0 {
			return SpectrumFrame{}, fmt.Errorf("fft window size must be a positive power of 2, got %d", n)
		}

		fftResult := CooleyTukeyFFT(chunk)
		frameIdx++

		// Analyze positive frequency half-spectrum (0 to N/2 inclusive)
		halfN := n / 2
		bins := make([]SpectrumBin, halfN+1)
		var peakBin SpectrumBin

		for k := 0; k <= halfN; k++ {
			cVal := fftResult[k]
			rawMag := cmplx.Abs(cVal)

			// Normalization: DC component (k=0) and Nyquist (k=N/2) scaled by 1/N; AC scaled by 2/N
			var normMag float64
			if k == 0 || k == halfN {
				normMag = rawMag / float64(n)
			} else {
				normMag = (2.0 * rawMag) / float64(n)
			}

			freqHz := float64(k) * (cfg.SampleRateHz / float64(n))
			bin := SpectrumBin{
				BinIndex:     k,
				FrequencyHz:  freqHz,
				ComplexVal:   cVal,
				Magnitude:    normMag,
				Power:        rawMag * rawMag,
				PhaseRadians: cmplx.Phase(cVal),
			}
			bins[k] = bin

			if bin.Magnitude > peakBin.Magnitude {
				peakBin = bin
			}
		}

		return SpectrumFrame{
			FrameIndex: frameIdx,
			N:          n,
			Bins:       bins,
			PeakBin:    peakBin,
		}, nil
	}, in, out)
}
