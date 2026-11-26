package math

import (
	"context"

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

// Stats accumulates an ordered bounded stream into descriptive summary statistics (Min, Max, Mean, Variance, StdDev).
// Implemented via Welford's algorithm for numerical stability.
func Stats[T OrderedNumber](ctx context.Context, in streaming.Receiver[T], out streaming.Sender[StatsSnapshot]) error {
	w := NewWelford()

	err := in.Receive(ctx, func(ctx context.Context, val T) error {
		w.Update(float64(val))
		return nil
	})
	if err != nil {
		return err
	}

	return out.Send(ctx, w.Snapshot())
}

// RunningStats continuously calculates and emits updated summary statistics for each incoming element.
func RunningStats[T OrderedNumber](ctx context.Context, in streaming.Receiver[T], out streaming.Sender[StatsSnapshot]) error {
	w := NewWelford()

	return in.Receive(ctx, func(ctx context.Context, val T) error {
		w.Update(float64(val))
		return out.Send(ctx, w.Snapshot())
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
