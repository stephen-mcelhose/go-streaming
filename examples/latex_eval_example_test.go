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

// CoordinatePoint represents a 2D position (X, Y) emitted from a sensor or physics simulator.
type CoordinatePoint struct {
	ID string
	X  float64
	Y  float64
}

// EvaluatedPoint holds the original point and the evaluated LaTeX result.
type EvaluatedPoint struct {
	ID       string
	X        float64
	Y        float64
	Distance float64
}

// StreamingLaTeXMap applies a LaTeX mathematical expression to every point in a stream using goexeclatex.Eval.
func StreamingLaTeXMap(
	ctx context.Context,
	latexExpr string,
	in streaming.Receiver[CoordinatePoint],
	out streaming.Sender[EvaluatedPoint],
) error {
	return transformations.Map(ctx, func(ctx context.Context, pt CoordinatePoint) (EvaluatedPoint, error) {
		vars := map[string]float64{
			"x": pt.X,
			"y": pt.Y,
		}
		res, err := goexeclatex.Eval(latexExpr, vars)
		if err != nil {
			return EvaluatedPoint{}, fmt.Errorf("evaluating latex %q: %w", latexExpr, err)
		}
		return EvaluatedPoint{
			ID:       pt.ID,
			X:        pt.X,
			Y:        pt.Y,
			Distance: res,
		}, nil
	}, in, out)
}

func TestLaTeXStreaming(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST compute distance for all points using goexeclatex", func(t *testing.T) {
		const formula = `\sqrt{x^2 + y^2}`

		inCh := make(chan CoordinatePoint, 3)
		inCh <- CoordinatePoint{ID: "p1", X: 3.0, Y: 4.0}  // sqrt(9+16) = 5.0
		inCh <- CoordinatePoint{ID: "p2", X: 6.0, Y: 8.0}  // sqrt(36+64) = 10.0
		inCh <- CoordinatePoint{ID: "p3", X: 5.0, Y: 12.0} // sqrt(25+144) = 13.0
		close(inCh)

		outCh := make(chan EvaluatedPoint, 3)
		err := StreamingLaTeXMap(ctx, formula, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh))
		require.NoError(t, err)
		close(outCh)

		var results []EvaluatedPoint
		for res := range outCh {
			results = append(results, res)
		}

		require.Equal(t, 3, len(results))
		require.InDelta(t, 5.0, results[0].Distance, 1e-6)
		require.InDelta(t, 10.0, results[1].Distance, 1e-6)
		require.InDelta(t, 13.0, results[2].Distance, 1e-6)
	})
}

// Example_latexStreaming demonstrates evaluating LaTeX math formulas on streaming records using goexeclatex.
// In data science and physics pipelines, users can express transformations as mathematical equations
// (like \sqrt{x^2 + y^2}) and evaluate them dynamically across streaming data.
func Example_latexStreaming() {
	ctx := context.Background()

	// Ingress stream of 2D location points
	points := make(chan CoordinatePoint, 3)
	points <- CoordinatePoint{ID: "sensor_alpha", X: 3.0, Y: 4.0}
	points <- CoordinatePoint{ID: "sensor_beta", X: 1.0, Y: 1.0}
	points <- CoordinatePoint{ID: "sensor_gamma", X: 5.0, Y: 12.0}
	close(points)

	const formula = `\sqrt{x^2 + y^2}`
	evaluatedCh := make(chan EvaluatedPoint, 3)

	_ = StreamingLaTeXMap(ctx, formula, streaming.NewChanStream(points, nil), streaming.NewChanStream(nil, evaluatedCh))
	close(evaluatedCh)

	for res := range evaluatedCh {
		fmt.Printf("Point %s (%.1f, %.1f): Calculated Distance = %.2f\n",
			res.ID, res.X, res.Y, res.Distance)
	}

	// Output:
	// Point sensor_alpha (3.0, 4.0): Calculated Distance = 5.00
	// Point sensor_beta (1.0, 1.0): Calculated Distance = 1.41
	// Point sensor_gamma (5.0, 12.0): Calculated Distance = 13.00
}
