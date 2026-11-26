package mathops

import (
	"math"
)

// RealNumber defines constraints for types that support real-valued statistical moments.
type RealNumber interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

// Welford tracks online single-pass statistical moments (mean, variance, stddev, min, max, z-score)
// using B. P. Welford's numerically stable recurrence algorithm.
type Welford struct {
	count int
	mean  float64
	m2    float64 // Sum of squared differences: sum((x_i - mean)^2)
	min   float64
	max   float64
}

// NewWelford creates an empty Welford moment accumulator.
func NewWelford() *Welford {
	return &Welford{}
}

// Update incorporates a new sample into the running statistical moments.
func (w *Welford) Update(val float64) {
	w.count++
	if w.count == 1 {
		w.mean = val
		w.m2 = 0.0
		w.min = val
		w.max = val
		return
	}

	delta := val - w.mean
	w.mean += delta / float64(w.count)
	delta2 := val - w.mean
	w.m2 += delta * delta2

	if val < w.min {
		w.min = val
	}
	if val > w.max {
		w.max = val
	}
}

// Count returns the total number of samples observed.
func (w *Welford) Count() int {
	return w.count
}

// Mean returns the running sample mean. Returns 0 if count == 0.
func (w *Welford) Mean() float64 {
	return w.mean
}

// Variance returns the unbiased sample variance (denominator N-1). Returns 0 if count < 2.
func (w *Welford) Variance() float64 {
	if w.count < 2 {
		return 0.0
	}
	return w.m2 / float64(w.count-1)
}

// PopulationVariance returns the population variance (denominator N).
func (w *Welford) PopulationVariance() float64 {
	if w.count == 0 {
		return 0.0
	}
	return w.m2 / float64(w.count)
}

// StdDev returns the sample standard deviation (sqrt of sample variance).
func (w *Welford) StdDev() float64 {
	return math.Sqrt(w.Variance())
}

// Min returns the minimum sample observed. Returns 0 if count == 0.
func (w *Welford) Min() float64 {
	return w.min
}

// Max returns the maximum sample observed. Returns 0 if count == 0.
func (w *Welford) Max() float64 {
	return w.max
}

// ZScore computes the standard score Z = (x - mean) / stddev against current baseline.
// Returns 0 if standard deviation is zero or count < 2.
func (w *Welford) ZScore(val float64) float64 {
	sd := w.StdDev()
	if sd == 0 {
		return 0.0
	}
	return (val - w.mean) / sd
}

// Snapshot exports the current state as a StatsSnapshot.
func (w *Welford) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		Count:    w.count,
		Mean:     w.mean,
		Variance: w.Variance(),
		StdDev:   w.StdDev(),
		Min:      w.min,
		Max:      w.max,
	}
}
