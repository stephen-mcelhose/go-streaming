package control_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/control"
)

func TestCUSUM(t *testing.T) {
	ctx := context.Background()

	type testCase struct {
		name              string
		config            control.CUSUMConfig
		inputs            []float64
		wantAlertIndices  []int64
		wantDirections    []control.CUSUMDirection
		assertSideEffects func(t *testing.T, alerts []control.CUSUMAlert)
	}

	testCases := []testCase{
		{
			name: "MUST not trigger alert when process remains in control near target",
			config: control.CUSUMConfig{
				Target:    100.0,
				Allowance: 5.0,
				Threshold: 20.0,
			},
			inputs:           []float64{101.0, 99.0, 102.0, 98.0, 100.0},
			wantAlertIndices: nil,
			wantDirections:   nil,
		},
		{
			name: "MUST trigger high-side alert when mean shifts upward continuously",
			config: control.CUSUMConfig{
				Target:       100.0,
				Allowance:    5.0,  // Target + Allowance = 105.0
				Threshold:    20.0, // Alert when HighSum >= 20.0
				ResetOnAlert: true,
			},
			// Sample 1: 115 (+10 -> S+=10)
			// Sample 2: 120 (+15 -> S+=25 >= 20 -> Alert at index 2)
			// Reset
			// Sample 3: 110 (+5 -> S+=5)
			// Sample 4: 125 (+20 -> S+=25 >= 20 -> Alert at index 4)
			inputs:           []float64{115.0, 120.0, 110.0, 125.0},
			wantAlertIndices: []int64{2, 4},
			wantDirections:   []control.CUSUMDirection{control.DirectionHigh, control.DirectionHigh},
		},
		{
			name: "MUST trigger low-side alert when mean shifts downward",
			config: control.CUSUMConfig{
				Target:       50.0,
				Allowance:    2.0,  // Target - Allowance = 48.0
				Threshold:    10.0, // Alert when LowSum <= -10.0
				ResetOnAlert: true,
			},
			// Sample 1: 40 (-8 -> S- = -8)
			// Sample 2: 42 (-6 -> S- = -14 <= -10 -> Alert at index 2)
			inputs:           []float64{40.0, 42.0},
			wantAlertIndices: []int64{2},
			wantDirections:   []control.CUSUMDirection{control.DirectionLow},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			inCh := make(chan float64, len(tc.inputs))
			for _, v := range tc.inputs {
				inCh <- v
			}
			close(inCh)

			outCh := make(chan control.CUSUMAlert, len(tc.inputs))
			err := control.CUSUM(ctx, tc.config, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh))
			require.NoError(t, err)
			close(outCh)

			var alerts []control.CUSUMAlert
			for a := range outCh {
				alerts = append(alerts, a)
			}

			require.Equal(t, len(tc.wantAlertIndices), len(alerts))
			for i, expIdx := range tc.wantAlertIndices {
				require.Equal(t, expIdx, alerts[i].SampleIndex)
				require.Equal(t, tc.wantDirections[i], alerts[i].Direction)
			}

			if tc.assertSideEffects != nil {
				tc.assertSideEffects(t, alerts)
			}
		})
	}
}

// ExampleCUSUM demonstrates detecting subtle upward latency degradation using Cumulative Sum control charts.
func ExampleCUSUM() {
	ctx := context.Background()

	// Ingress stream: Baseline response latency is 50ms.
	// Samples 1-3 are normal (~50ms).
	// Samples 4-6 experience an insidious upward mean shift to ~70ms.
	latencies := make(chan float64, 6)
	for _, l := range []float64{50.0, 52.0, 49.0, 68.0, 72.0, 71.0} {
		latencies <- l
	}
	close(latencies)

	alerts := make(chan control.CUSUMAlert, 6)

	// Configure CUSUM with Target = 50ms, Allowance K = 5ms, Threshold H = 25ms
	cfg := control.CUSUMConfig{
		Target:       50.0,
		Allowance:    5.0,  // Slack: only values above 55ms accumulate
		Threshold:    25.0, // Alert threshold
		ResetOnAlert: true,
	}

	_ = control.CUSUM(ctx, cfg, streaming.NewChanStream(latencies, nil), streaming.NewChanStream(nil, alerts))
	close(alerts)

	for a := range alerts {
		fmt.Printf("CUSUM Alert at sample #%d: Value=%.1fms (Cumulative Sum S+=%.1f reached threshold)\n",
			a.SampleIndex, a.Value, a.HighSum)
	}

	// Output:
	// CUSUM Alert at sample #5: Value=72.0ms (Cumulative Sum S+=30.0 reached threshold)
}
