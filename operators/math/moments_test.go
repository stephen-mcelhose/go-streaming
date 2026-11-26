package math_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	mathops "github.com/stephen-mcelhose/go-streaming/operators/math"
)

func TestMoments(t *testing.T) {
	type testCase struct {
		name              string
		samples           []float64
		wantCount         int64
		wantMeanNear      float64
		wantVarianceNear  float64
		wantSkewNear      float64
		wantKurtNear      float64
		assertSideEffects func(t *testing.T, m *mathops.Moments)
	}

	testCases := []testCase{
		{
			name:             "MUST return zeroes for empty Moments accumulator",
			samples:          nil,
			wantCount:        0,
			wantMeanNear:     0.0,
			wantVarianceNear: 0.0,
			wantSkewNear:     0.0,
			wantKurtNear:     0.0,
		},
		{
			name:             "MUST return zero skewness and kurtosis for count < 4",
			samples:          []float64{10.0, 20.0, 30.0},
			wantCount:        3,
			wantMeanNear:     20.0,
			wantVarianceNear: 100.0,
			wantSkewNear:     0.0, // Symmetric samples
			wantKurtNear:     0.0, // N < 4 returns 0
		},
		{
			name:             "MUST calculate zero skewness for perfectly symmetric distribution",
			samples:          []float64{1.0, 2.0, 3.0, 4.0, 5.0},
			wantCount:        5,
			wantMeanNear:     3.0,
			wantVarianceNear: 2.5,
			wantSkewNear:     0.0,  // Exact symmetry
			wantKurtNear:     1.70, // Platykurtic uniform discrete
			assertSideEffects: func(t *testing.T, m *mathops.Moments) {
				require.InDelta(t, -1.30, m.ExcessKurtosis(), 0.01)
			},
		},
		{
			name:             "MUST calculate positive skewness for right-tailed distribution",
			samples:          []float64{10.0, 10.0, 11.0, 12.0, 50.0}, // Big positive outlier at 50
			wantCount:        5,
			wantMeanNear:     18.6,
			wantVarianceNear: 308.8,
			wantSkewNear:     1.47, // Strong positive skew
			wantKurtNear:     3.19,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m := mathops.NewMoments()
			for _, s := range tc.samples {
				m.Update(s)
			}

			require.Equal(t, tc.wantCount, m.Count())
			require.InDelta(t, tc.wantMeanNear, m.Mean(), 1e-1)
			require.InDelta(t, tc.wantVarianceNear, m.Variance(), 1e-1)
			require.InDelta(t, tc.wantSkewNear, m.Skewness(), 1e-1)
			require.InDelta(t, tc.wantKurtNear, m.Kurtosis(), 1e-1)

			if tc.assertSideEffects != nil {
				tc.assertSideEffects(t, m)
			}
		})
	}
}

func TestMomentsOperators(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST compute MomentsStats over stream cleanly", func(t *testing.T) {
		inCh := make(chan float64, 5)
		for _, v := range []float64{1.0, 2.0, 3.0, 4.0, 5.0} {
			inCh <- v
		}
		close(inCh)

		outCh := make(chan mathops.MomentsSnapshot, 1)
		err := mathops.MomentsStats(ctx, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh))
		require.NoError(t, err)
		close(outCh)

		snap := <-outCh
		require.Equal(t, int64(5), snap.Count)
		require.InDelta(t, 3.0, snap.Mean, 1e-6)
		require.InDelta(t, 0.0, snap.Skewness, 1e-6)
	})
}

// ExampleMoments demonstrates detecting asymmetric latency spikes (positive skewness) on a live stream.
func ExampleMoments() {
	// Web latency measurements: steady baseline at ~20ms with an occasional slow query at 95ms
	latencies := []float64{20.0, 22.0, 19.0, 21.0, 20.0, 95.0}

	m := mathops.NewMoments()
	for _, l := range latencies {
		m.Update(l)
	}

	snap := m.Snapshot()
	fmt.Printf("Samples: %d | Mean: %.1fms | StdDev: %.1fms\n", snap.Count, snap.Mean, snap.StdDev)
	fmt.Printf("Skewness: %.2f (Right-tailed asymmetry) | Kurtosis: %.2f\n", snap.Skewness, snap.Kurtosis)

	// Output:
	// Samples: 6 | Mean: 32.8ms | StdDev: 30.5ms
	// Skewness: 1.78 (Right-tailed asymmetry) | Kurtosis: 4.19
}
