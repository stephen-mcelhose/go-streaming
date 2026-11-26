package mathops_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	mathops "github.com/stephen-mcelhose/go-streaming/operators/math"
)

func TestWelford(t *testing.T) {
	type testCase struct {
		name              string
		samples           []float64
		wantCount         int
		wantMean          float64
		wantVarianceNear  float64
		wantStdDevNear    float64
		wantMin           float64
		wantMax           float64
		assertSideEffects func(t *testing.T, w *mathops.Welford)
	}

	testCases := []testCase{
		{
			name:             "MUST return zeroes for empty Welford accumulator",
			samples:          nil,
			wantCount:        0,
			wantMean:         0.0,
			wantVarianceNear: 0.0,
			wantStdDevNear:   0.0,
			wantMin:          0.0,
			wantMax:          0.0,
		},
		{
			name:             "MUST return single sample properties for count 1",
			samples:          []float64{42.0},
			wantCount:        1,
			wantMean:         42.0,
			wantVarianceNear: 0.0,
			wantStdDevNear:   0.0,
			wantMin:          42.0,
			wantMax:          42.0,
		},
		{
			name:             "MUST calculate exact sample variance and mean for multiple samples",
			samples:          []float64{10.0, 20.0, 30.0, 40.0, 50.0},
			wantCount:        5,
			wantMean:         30.0,
			wantVarianceNear: 250.0, // sample variance: sum((x-30)^2)/4 = 1000/4 = 250
			wantStdDevNear:   math.Sqrt(250.0),
			wantMin:          10.0,
			wantMax:          50.0,
			assertSideEffects: func(t *testing.T, w *mathops.Welford) {
				require.Equal(t, 200.0, w.PopulationVariance())
				// ZScore of 30 (mean) is 0
				require.InDelta(t, 0.0, w.ZScore(30.0), 1e-6)
				// ZScore of 50 is (50 - 30) / sqrt(250) = 20 / 15.811388 = ~1.2649
				require.InDelta(t, 20.0/math.Sqrt(250.0), w.ZScore(50.0), 1e-4)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			w := mathops.NewWelford()
			for _, s := range tc.samples {
				w.Update(s)
			}

			require.Equal(t, tc.wantCount, w.Count())
			require.InDelta(t, tc.wantMean, w.Mean(), 1e-6)
			require.InDelta(t, tc.wantVarianceNear, w.Variance(), 1e-6)
			require.InDelta(t, tc.wantStdDevNear, w.StdDev(), 1e-6)
			require.Equal(t, tc.wantMin, w.Min())
			require.Equal(t, tc.wantMax, w.Max())

			if tc.assertSideEffects != nil {
				tc.assertSideEffects(t, w)
			}
		})
	}
}
