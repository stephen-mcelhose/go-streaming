package examples_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
	"github.com/stephen-mcelhose/goexeclatex"
)

// LaTeXMetricObservation represents an incoming numeric measurement from a sensor, pricing feed, or telemetry stream.
type LaTeXMetricObservation struct {
	Timestamp int64
	Value     float64
}

// ScoredState represents the accumulated state output after filling LaTeX equation slots at each step.
type ScoredState struct {
	Timestamp   int64
	Input       float64
	SmoothedVal float64
}

// StreamingLaTeXScan executes a stateful recurrence equation across a stream using goexeclatex.
// For every incoming observation:
// 1. It binds current observation to slot "x".
// 2. It binds previous accumulated state to slot "s".
// 3. It binds constant parameters (like alpha or weights) to their respective slots.
// 4. It evaluates the LaTeX recurrence relation (e.g. `a \cdot x + (1 - a) \cdot s`) and carries the result forward.
func StreamingLaTeXScan(
	ctx context.Context,
	latexRecurrenceExpr string,
	params map[string]float64,
	initialState float64,
	in streaming.Receiver[LaTeXMetricObservation],
	out streaming.Sender[ScoredState],
) error {
	return transformations.Scan(ctx, initialState, func(ctx context.Context, prevState float64, obs LaTeXMetricObservation) (float64, error) {
		// Populate equation slots for this step
		slots := make(map[string]float64, len(params)+2)
		for k, v := range params {
			slots[k] = v
		}
		slots["x"] = obs.Value // Current input item slot
		slots["s"] = prevState // Previous accumulated state slot

		// Evaluate stateful LaTeX recurrence
		nextState, err := goexeclatex.Eval(latexRecurrenceExpr, slots)
		if err != nil {
			return 0, fmt.Errorf("evaluating latex recurrence %q: %w", latexRecurrenceExpr, err)
		}
		return nextState, nil
	}, in, &latexStateAdapter{out: out})
}

// latexStateAdapter maps internal scalar state into the downstream typed ScoredState stream.
type latexStateAdapter struct {
	out streaming.Sender[ScoredState]
}

func (a *latexStateAdapter) Send(ctx context.Context, val float64) error {
	return a.out.Send(ctx, ScoredState{
		SmoothedVal: val,
	})
}

func TestLaTeXStreaming(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST execute stateful LaTeX recurrence step-by-step filling slots", func(t *testing.T) {
		// Recurrence relation: S_t = a*x + (1-a)*s (Exponential smoothing)
		const recurrenceFormula = `a x + (1 - a) s`
		params := map[string]float64{"a": 0.5}

		inCh := make(chan LaTeXMetricObservation, 3)
		inCh <- LaTeXMetricObservation{Timestamp: 1000, Value: 10.0} // S_1 = 0.5(10) + 0.5(0) = 5.0
		inCh <- LaTeXMetricObservation{Timestamp: 1010, Value: 20.0} // S_2 = 0.5(20) + 0.5(5) = 12.5
		inCh <- LaTeXMetricObservation{Timestamp: 1020, Value: 30.0} // S_3 = 0.5(30) + 0.5(12.5) = 21.25
		close(inCh)

		outCh := make(chan ScoredState, 3)
		err := StreamingLaTeXScan(ctx, recurrenceFormula, params, 0.0,
			streaming.NewChanStream(inCh, nil),
			streaming.NewChanStream(nil, outCh),
		)
		require.NoError(t, err)
		close(outCh)

		var results []ScoredState
		for res := range outCh {
			results = append(results, res)
		}

		require.Equal(t, 3, len(results))
		require.InDelta(t, 5.0, results[0].SmoothedVal, 1e-6)
		require.InDelta(t, 12.5, results[1].SmoothedVal, 1e-6)
		require.InDelta(t, 21.25, results[2].SmoothedVal, 1e-6)
	})
}

// Example_latexStreaming demonstrates evaluating a stateful mathematical recurrence relation
// directly from a LaTeX formula: S_t = a \cdot x + (1 - a) \cdot s
// As each incoming metric arrives, it dynamically fills the current item slot ("x") and
// previous accumulated state slot ("s"), passing the result to the next iteration.
func Example_latexStreaming() {
	ctx := context.Background()

	// Ingress stream of sensor readings
	readings := make(chan LaTeXMetricObservation, 4)
	readings <- LaTeXMetricObservation{Timestamp: 100, Value: 100.0}
	readings <- LaTeXMetricObservation{Timestamp: 101, Value: 120.0}
	readings <- LaTeXMetricObservation{Timestamp: 102, Value: 110.0}
	readings <- LaTeXMetricObservation{Timestamp: 103, Value: 130.0}
	close(readings)

	// LaTeX recurrence formula: S_t = a * x + (1 - a) * s (Exponential Moving Average)
	const formula = `a x + (1 - a) s`
	params := map[string]float64{"a": 0.3}

	evaluatedCh := make(chan ScoredState, 4)

	_ = StreamingLaTeXScan(ctx, formula, params, 100.0,
		streaming.NewChanStream(readings, nil),
		streaming.NewChanStream(nil, evaluatedCh),
	)
	close(evaluatedCh)

	step := 1
	for res := range evaluatedCh {
		fmt.Printf("Step #%d: Filled slots -> Evaluated S_%d = %.2f\n",
			step, step, res.SmoothedVal)
		step++
	}

	// Output:
	// Step #1: Filled slots -> Evaluated S_1 = 100.00
	// Step #2: Filled slots -> Evaluated S_2 = 106.00
	// Step #3: Filled slots -> Evaluated S_3 = 107.20
	// Step #4: Filled slots -> Evaluated S_4 = 114.04
}
