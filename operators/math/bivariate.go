package math

import (
	"context"
	"math"

	"github.com/stephen-mcelhose/go-streaming"
)

// Pair represents a paired observation of two numeric values from concurrent streams or datasets.
type Pair[X, Y OrderedNumber] struct {
	X X
	Y Y
}

// BivariateSnapshot contains point-in-time joint statistics for two numeric variables.
type BivariateSnapshot struct {
	Count       int64
	MeanX       float64
	MeanY       float64
	VarianceX   float64
	VarianceY   float64
	StdDevX     float64
	StdDevY     float64
	Covariance  float64
	Correlation float64
}

// BivariateWelford tracks online single-pass joint statistical moments (mean X, mean Y,
// variance X, variance Y, covariance, and Pearson correlation) using an extension of Welford's algorithm.
type BivariateWelford struct {
	count int64
	meanX float64
	meanY float64
	m2X   float64 // Sum of squared differences for X: sum((x_i - meanX)^2)
	m2Y   float64 // Sum of squared differences for Y: sum((y_i - meanY)^2)
	cXY   float64 // Co-moment: sum((x_i - meanX) * (y_i - meanY))
}

// NewBivariateWelford creates an empty BivariateWelford moment accumulator.
func NewBivariateWelford() *BivariateWelford {
	return &BivariateWelford{}
}

// Update incorporates a new paired observation (x, y) into the running joint moments.
func (b *BivariateWelford) Update(x, y float64) {
	b.count++
	if b.count == 1 {
		b.meanX = x
		b.meanY = y
		b.m2X = 0.0
		b.m2Y = 0.0
		b.cXY = 0.0
		return
	}

	deltaX := x - b.meanX
	deltaY := y - b.meanY

	b.meanX += deltaX / float64(b.count)
	b.meanY += deltaY / float64(b.count)

	b.m2X += deltaX * (x - b.meanX)
	b.m2Y += deltaY * (y - b.meanY)
	b.cXY += deltaX * (y - b.meanY)
}

// Count returns the total number of paired observations.
func (b *BivariateWelford) Count() int64 {
	return b.count
}

// MeanX returns the sample mean of variable X.
func (b *BivariateWelford) MeanX() float64 {
	return b.meanX
}

// MeanY returns the sample mean of variable Y.
func (b *BivariateWelford) MeanY() float64 {
	return b.meanY
}

// VarianceX returns the unbiased sample variance of variable X (denominator N-1).
func (b *BivariateWelford) VarianceX() float64 {
	if b.count < 2 {
		return 0.0
	}
	return b.m2X / float64(b.count-1)
}

// VarianceY returns the unbiased sample variance of variable Y (denominator N-1).
func (b *BivariateWelford) VarianceY() float64 {
	if b.count < 2 {
		return 0.0
	}
	return b.m2Y / float64(b.count-1)
}

// StdDevX returns the sample standard deviation of variable X.
func (b *BivariateWelford) StdDevX() float64 {
	return math.Sqrt(b.VarianceX())
}

// StdDevY returns the sample standard deviation of variable Y.
func (b *BivariateWelford) StdDevY() float64 {
	return math.Sqrt(b.VarianceY())
}

// Covariance returns the unbiased sample covariance between X and Y (denominator N-1).
func (b *BivariateWelford) Covariance() float64 {
	if b.count < 2 {
		return 0.0
	}
	return b.cXY / float64(b.count-1)
}

// PopulationCovariance returns the population covariance (denominator N).
func (b *BivariateWelford) PopulationCovariance() float64 {
	if b.count == 0 {
		return 0.0
	}
	return b.cXY / float64(b.count)
}

// Correlation returns the Pearson correlation coefficient r_XY in range [-1.0, 1.0].
// Returns 0 if variance of either variable is zero or count < 2.
func (b *BivariateWelford) Correlation() float64 {
	if b.count < 2 {
		return 0.0
	}
	denom := math.Sqrt(b.m2X * b.m2Y)
	if denom == 0 {
		return 0.0
	}
	return b.cXY / denom
}

// Snapshot exports the current state as a BivariateSnapshot.
func (b *BivariateWelford) Snapshot() BivariateSnapshot {
	return BivariateSnapshot{
		Count:       b.count,
		MeanX:       b.meanX,
		MeanY:       b.meanY,
		VarianceX:   b.VarianceX(),
		VarianceY:   b.VarianceY(),
		StdDevX:     b.StdDevX(),
		StdDevY:     b.StdDevY(),
		Covariance:  b.Covariance(),
		Correlation: b.Correlation(),
	}
}

// Covariance computes the sample covariance over a bounded stream of paired numbers.
func Covariance[X, Y OrderedNumber](
	ctx context.Context,
	in streaming.Receiver[Pair[X, Y]],
	out streaming.Sender[float64],
) error {
	bw := NewBivariateWelford()

	err := in.Receive(ctx, func(ctx context.Context, p Pair[X, Y]) error {
		bw.Update(float64(p.X), float64(p.Y))
		return nil
	})
	if err != nil {
		return err
	}

	return out.Send(ctx, bw.Covariance())
}

// RunningCovariance continuously emits updated BivariateSnapshots for every paired observation.
func RunningCovariance[X, Y OrderedNumber](
	ctx context.Context,
	in streaming.Receiver[Pair[X, Y]],
	out streaming.Sender[BivariateSnapshot],
) error {
	bw := NewBivariateWelford()

	return in.Receive(ctx, func(ctx context.Context, p Pair[X, Y]) error {
		bw.Update(float64(p.X), float64(p.Y))
		return out.Send(ctx, bw.Snapshot())
	})
}

// PearsonCorrelation computes the terminal Pearson correlation coefficient r in [-1, 1] over a bounded stream.
func PearsonCorrelation[X, Y OrderedNumber](
	ctx context.Context,
	in streaming.Receiver[Pair[X, Y]],
	out streaming.Sender[float64],
) error {
	bw := NewBivariateWelford()

	err := in.Receive(ctx, func(ctx context.Context, p Pair[X, Y]) error {
		bw.Update(float64(p.X), float64(p.Y))
		return nil
	})
	if err != nil {
		return err
	}

	return out.Send(ctx, bw.Correlation())
}
