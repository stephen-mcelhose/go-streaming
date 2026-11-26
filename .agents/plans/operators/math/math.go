package mathops

import (
	"context"
	"math"

	"github.com/stephen-mcelhose/go-streaming"
)

// StatsSnapshot represents a point-in-time calculation of distribution statistics.
type StatsSnapshot struct {
	Count    int
	Mean     float64
	Variance float64
	StdDev   float64
	Min      float64
	Max      float64
}

// Stats computes summary statistics across an entire bounded stream.
func Stats[T RealNumber](ctx context.Context, in streaming.Receiver[T], out streaming.Sender[StatsSnapshot]) error {
	w := NewWelford()

	err := in.Receive(ctx, func(ctx context.Context, item T) error {
		w.Update(float64(item))
		return nil
	})
	if err != nil {
		return err
	}

	return out.Send(ctx, w.Snapshot())
}

// RunningStats continuously calculates and emits updated summary statistics for each incoming element.
func RunningStats[T RealNumber](ctx context.Context, in streaming.Receiver[T], out streaming.Sender[StatsSnapshot]) error {
	w := NewWelford()

	return in.Receive(ctx, func(ctx context.Context, item T) error {
		w.Update(float64(item))
		return out.Send(ctx, w.Snapshot())
	})
}

// EMA calculates Exponential Moving Average with smoothing factor alpha (0 < alpha <= 1).
func EMA[T RealNumber](ctx context.Context, alpha float64, in streaming.Receiver[T], out streaming.Sender[float64]) error {
	var (
		currentEMA float64
		hasFirst   bool
	)

	return in.Receive(ctx, func(ctx context.Context, item T) error {
		val := float64(item)
		if !hasFirst {
			currentEMA = val
			hasFirst = true
		} else {
			currentEMA = alpha*val + (1.0-alpha)*currentEMA
		}
		return out.Send(ctx, currentEMA)
	})
}
