package math_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	mathops "github.com/stephen-mcelhose/go-streaming/operators/math"
)

func TestBivariateWelford(t *testing.T) {
	type testCase struct {
		name              string
		pairs             []mathops.Pair[float64, float64]
		wantCount         int64
		wantMeanXNear     float64
		wantMeanYNear     float64
		wantCovNear       float64
		wantCorrNear      float64
		assertSideEffects func(t *testing.T, bw *mathops.BivariateWelford)
	}

	testCases := []testCase{
		{
			name:          "MUST return zeroes for empty BivariateWelford accumulator",
			pairs:         nil,
			wantCount:     0,
			wantMeanXNear: 0.0,
			wantMeanYNear: 0.0,
			wantCovNear:   0.0,
			wantCorrNear:  0.0,
		},
		{
			name: "MUST return zero covariance and correlation for single pair",
			pairs: []mathops.Pair[float64, float64]{
				{X: 10.0, Y: 20.0},
			},
			wantCount:     1,
			wantMeanXNear: 10.0,
			wantMeanYNear: 20.0,
			wantCovNear:   0.0,
			wantCorrNear:  0.0,
		},
		{
			name: "MUST calculate exact positive correlation for perfectly correlated variables (Y = 2X + 1)",
			pairs: []mathops.Pair[float64, float64]{
				{X: 1.0, Y: 3.0},
				{X: 2.0, Y: 5.0},
				{X: 3.0, Y: 7.0},
				{X: 4.0, Y: 9.0},
				{X: 5.0, Y: 11.0},
			},
			wantCount:     5,
			wantMeanXNear: 3.0,
			wantMeanYNear: 7.0,
			wantCovNear:   5.0, // Cov(X, 2X+1) = 2*Var(X) = 2*2.5 = 5.0
			wantCorrNear:  1.0, // Perfect correlation r = 1.0
			assertSideEffects: func(t *testing.T, bw *mathops.BivariateWelford) {
				require.InDelta(t, 2.5, bw.VarianceX(), 1e-6)
				require.InDelta(t, 10.0, bw.VarianceY(), 1e-6)
				require.InDelta(t, 4.0, bw.PopulationCovariance(), 1e-6) // (5.0 * 4) / 5 = 4.0
			},
		},
		{
			name: "MUST calculate negative correlation for inversely correlated variables",
			pairs: []mathops.Pair[float64, float64]{
				{X: 1.0, Y: 10.0},
				{X: 2.0, Y: 8.0},
				{X: 3.0, Y: 6.0},
				{X: 4.0, Y: 4.0},
				{X: 5.0, Y: 2.0},
			},
			wantCount:     5,
			wantMeanXNear: 3.0,
			wantMeanYNear: 6.0,
			wantCovNear:   -5.0,
			wantCorrNear:  -1.0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bw := mathops.NewBivariateWelford()
			for _, p := range tc.pairs {
				bw.Update(p.X, p.Y)
			}

			require.Equal(t, tc.wantCount, bw.Count())
			require.InDelta(t, tc.wantMeanXNear, bw.MeanX(), 1e-6)
			require.InDelta(t, tc.wantMeanYNear, bw.MeanY(), 1e-6)
			require.InDelta(t, tc.wantCovNear, bw.Covariance(), 1e-6)
			require.InDelta(t, tc.wantCorrNear, bw.Correlation(), 1e-6)

			if tc.assertSideEffects != nil {
				tc.assertSideEffects(t, bw)
			}
		})
	}
}

func TestBivariateOperators(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST compute streaming Pearson correlation cleanly", func(t *testing.T) {
		pairCh := make(chan mathops.Pair[float64, float64], 3)
		pairCh <- mathops.Pair[float64, float64]{X: 10.0, Y: 100.0}
		pairCh <- mathops.Pair[float64, float64]{X: 20.0, Y: 200.0}
		pairCh <- mathops.Pair[float64, float64]{X: 30.0, Y: 300.0}
		close(pairCh)

		outCh := make(chan float64, 1)
		err := mathops.PearsonCorrelation(ctx, streaming.NewChanStream(pairCh, nil), streaming.NewChanStream(nil, outCh))
		require.NoError(t, err)
		close(outCh)

		corr := <-outCh
		require.InDelta(t, 1.0, corr, 1e-6)
	})

	t.Run("MUST compute streaming Covariance cleanly", func(t *testing.T) {
		pairCh := make(chan mathops.Pair[float64, float64], 3)
		pairCh <- mathops.Pair[float64, float64]{X: 1.0, Y: 2.0}
		pairCh <- mathops.Pair[float64, float64]{X: 2.0, Y: 4.0}
		pairCh <- mathops.Pair[float64, float64]{X: 3.0, Y: 6.0}
		close(pairCh)

		outCh := make(chan float64, 1)
		err := mathops.Covariance(ctx, streaming.NewChanStream(pairCh, nil), streaming.NewChanStream(nil, outCh))
		require.NoError(t, err)
		close(outCh)

		cov := <-outCh
		require.InDelta(t, 2.0, cov, 1e-6)
	})
}

// ExampleBivariateWelford demonstrates calculating the real-time correlation between CPU usage and server latency.
func ExampleBivariateWelford() {
	// Pairs of (CPU Utilization %, Request Latency ms)
	telemetryFeed := []mathops.Pair[float64, float64]{
		{X: 20.0, Y: 50.0},
		{X: 40.0, Y: 95.0},
		{X: 60.0, Y: 140.0},
		{X: 80.0, Y: 190.0},
		{X: 95.0, Y: 235.0},
	}

	bw := mathops.NewBivariateWelford()
	for _, obs := range telemetryFeed {
		bw.Update(obs.X, obs.Y)
	}

	snap := bw.Snapshot()
	fmt.Printf("Observations: %d | Mean CPU: %.1f%% | Mean Latency: %.1fms\n",
		snap.Count, snap.MeanX, snap.MeanY)
	fmt.Printf("Covariance: %.2f | Pearson Correlation (r): %.3f\n",
		snap.Covariance, snap.Correlation)

	// Output:
	// Observations: 5 | Mean CPU: 59.0% | Mean Latency: 142.0ms
	// Covariance: 2208.75 | Pearson Correlation (r): 0.998
}
