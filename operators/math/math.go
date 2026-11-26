package math

import (
	"context"
	"math"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/pure"
	"golang.org/x/exp/constraints"
)

// Number represents any numeric type that supports addition and standard arithmetic (integers, floats, complex).
type Number interface {
	constraints.Integer | constraints.Float | constraints.Complex
}

// OrderedNumber represents numeric types that support total ordering (integers and floats).
type OrderedNumber interface {
	constraints.Integer | constraints.Float
}

// Sum accumulates elements of a bounded stream into their total scalar sum.
func Sum[T Number](ctx context.Context, in streaming.Receiver[T], out streaming.Sender[T]) error {
	return pure.Reduce(ctx, T(0), func(acc, item T) T {
		return acc + item
	}, in, out)
}

// RunningSum emits the cumulative sum after each incoming number on unbounded or bounded streams.
func RunningSum[T Number](ctx context.Context, in streaming.Receiver[T], out streaming.Sender[T]) error {
	return pure.Scan(ctx, T(0), func(acc, item T) T {
		return acc + item
	}, in, out)
}

// StatsSnapshot contains standard descriptive summary statistics for ordered numeric streams.
type StatsSnapshot struct {
	Count    int64
	Sum      float64
	Min      float64
	Max      float64
	Mean     float64
	Variance float64
	StdDev   float64
}

type welfordState struct {
	count int64
	sum   float64
	min   float64
	max   float64
	mean  float64
	m2    float64
}

// Stats accumulates an ordered bounded stream into descriptive summary statistics (Min, Max, Mean, Variance, StdDev).
// Implemented via Welford's algorithm for numerical stability.
func Stats[T OrderedNumber](ctx context.Context, in streaming.Receiver[T], out streaming.Sender[StatsSnapshot]) error {
	initial := welfordState{min: math.MaxFloat64, max: -math.MaxFloat64}

	return pure.Reduce(ctx, initial, func(s welfordState, val T) welfordState {
		fVal := float64(val)
		s.count++
		s.sum += fVal
		if fVal < s.min {
			s.min = fVal
		}
		if fVal > s.max {
			s.max = fVal
		}
		delta := fVal - s.mean
		s.mean += delta / float64(s.count)
		delta2 := fVal - s.mean
		s.m2 += delta * delta2
		return s
	}, in, &statsConverter{out: out})
}

type statsConverter struct {
	out streaming.Sender[StatsSnapshot]
}

func (sc *statsConverter) Send(ctx context.Context, s welfordState) error {
	if s.count == 0 {
		return sc.out.Send(ctx, StatsSnapshot{})
	}
	variance := 0.0
	if s.count > 1 {
		variance = s.m2 / float64(s.count-1)
	}
	return sc.out.Send(ctx, StatsSnapshot{
		Count:    s.count,
		Sum:      s.sum,
		Min:      s.min,
		Max:      s.max,
		Mean:     s.mean,
		Variance: variance,
		StdDev:   math.Sqrt(variance),
	})
}

type emaState struct {
	initialized bool
	value       float64
}

// EMA computes the Exponential Moving Average across a stream of numbers using smoothing factor alpha (0 < alpha <= 1).
func EMA(ctx context.Context, alpha float64, in streaming.Receiver[float64], out streaming.Sender[float64]) error {
	if alpha <= 0 || alpha > 1 {
		alpha = 0.2 // default smoothing
	}

	return pure.Scan(ctx, emaState{}, func(s emaState, item float64) emaState {
		if !s.initialized {
			return emaState{initialized: true, value: item}
		}
		return emaState{initialized: true, value: alpha*item + (1-alpha)*s.value}
	}, in, &emaSender{out: out})
}

type emaSender struct {
	out streaming.Sender[float64]
}

func (e *emaSender) Send(ctx context.Context, item emaState) error {
	if !item.initialized {
		return nil
	}
	return e.out.Send(ctx, item.value)
}
