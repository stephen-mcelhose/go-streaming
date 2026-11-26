package math

import (
	"context"
	"math"

	"github.com/stephen-mcelhose/go-streaming"
)

// MomentsSnapshot holds descriptive statistics including higher-order shape parameters.
type MomentsSnapshot struct {
	Count          int64
	Mean           float64
	Variance       float64
	StdDev         float64
	Skewness       float64
	Kurtosis       float64
	ExcessKurtosis float64
}

// Moments tracks single-pass online statistical moments up to 4th order (mean, variance,
// skewness, and kurtosis) using Philippe Pébay's recurrence formulas.
type Moments struct {
	count int64
	mean  float64
	m2    float64 // 2nd central moment sum: sum((x_i - mean)^2)
	m3    float64 // 3rd central moment sum: sum((x_i - mean)^3)
	m4    float64 // 4th central moment sum: sum((x_i - mean)^4)
}

// NewMoments creates an empty higher-order moment accumulator.
func NewMoments() *Moments {
	return &Moments{}
}

// Update incorporates a new sample into the running moments using Pébay's single-pass formulas.
func (m *Moments) Update(val float64) {
	nPrev := m.count
	m.count++
	n := float64(m.count)

	if m.count == 1 {
		m.mean = val
		m.m2 = 0.0
		m.m3 = 0.0
		m.m4 = 0.0
		return
	}

	delta := val - m.mean
	deltaN := delta / n
	deltaN2 := deltaN * deltaN
	term1 := delta * deltaN * float64(nPrev)

	m.mean += deltaN
	m.m4 += term1*deltaN2*(n*n-3.0*n+3.0) + 6.0*deltaN2*m.m2 - 4.0*deltaN*m.m3
	m.m3 += term1*deltaN*(n-2.0) - 3.0*deltaN*m.m2
	m.m2 += term1
}

// Count returns the number of observed samples.
func (m *Moments) Count() int64 {
	return m.count
}

// Mean returns the sample mean.
func (m *Moments) Mean() float64 {
	return m.mean
}

// Variance returns the sample variance (denominator N-1). Returns 0 if count < 2.
func (m *Moments) Variance() float64 {
	if m.count < 2 {
		return 0.0
	}
	return m.m2 / float64(m.count-1)
}

// StdDev returns the sample standard deviation.
func (m *Moments) StdDev() float64 {
	return math.Sqrt(m.Variance())
}

// Skewness returns the sample skewness (measure of distribution asymmetry).
// Positive values indicate a longer right tail; negative values indicate a longer left tail.
// Returns 0 if count < 3 or variance is 0.
func (m *Moments) Skewness() float64 {
	if m.count < 3 || m.m2 == 0 {
		return 0.0
	}
	n := float64(m.count)
	return (math.Sqrt(n) * m.m3) / math.Pow(m.m2, 1.5)
}

// Kurtosis returns the Pearson kurtosis (measure of tail fatness).
// Standard normal distribution has kurtosis = 3.0.
// Returns 0 if count < 4 or variance is 0.
func (m *Moments) Kurtosis() float64 {
	if m.count < 4 || m.m2 == 0 {
		return 0.0
	}
	n := float64(m.count)
	return (n * m.m4) / (m.m2 * m.m2)
}

// ExcessKurtosis returns kurtosis relative to normal distribution (Kurtosis - 3.0).
// Values > 0 indicate fat tails (leptokurtic); values < 0 indicate thin tails (platykurtic).
func (m *Moments) ExcessKurtosis() float64 {
	if m.count < 4 || m.m2 == 0 {
		return 0.0
	}
	return m.Kurtosis() - 3.0
}

// Snapshot exports the moments as a MomentsSnapshot.
func (m *Moments) Snapshot() MomentsSnapshot {
	return MomentsSnapshot{
		Count:          m.count,
		Mean:           m.mean,
		Variance:       m.Variance(),
		StdDev:         m.StdDev(),
		Skewness:       m.Skewness(),
		Kurtosis:       m.Kurtosis(),
		ExcessKurtosis: m.ExcessKurtosis(),
	}
}

// MomentsStats computes summary moments including skewness and kurtosis over a bounded stream.
func MomentsStats[T OrderedNumber](
	ctx context.Context,
	in streaming.Receiver[T],
	out streaming.Sender[MomentsSnapshot],
) error {
	m := NewMoments()

	err := in.Receive(ctx, func(ctx context.Context, item T) error {
		m.Update(float64(item))
		return nil
	})
	if err != nil {
		return err
	}

	return out.Send(ctx, m.Snapshot())
}

// RunningMoments continuously emits updated MomentsSnapshots for every incoming item on a stream.
func RunningMoments[T OrderedNumber](
	ctx context.Context,
	in streaming.Receiver[T],
	out streaming.Sender[MomentsSnapshot],
) error {
	m := NewMoments()

	return in.Receive(ctx, func(ctx context.Context, item T) error {
		m.Update(float64(item))
		return out.Send(ctx, m.Snapshot())
	})
}
