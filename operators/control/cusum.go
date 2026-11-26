package control

import (
	"context"

	"github.com/stephen-mcelhose/go-streaming"
	mathops "github.com/stephen-mcelhose/go-streaming/operators/math"
)

// CUSUMConfig configures the Cumulative Sum (CUSUM) change-point detector.
type CUSUMConfig struct {
	// Target is the expected in-control process mean (mu_0).
	Target float64
	// Allowance (K) is the slack parameter, often set to half the shift magnitude to detect (e.g. 0.5 * sigma).
	Allowance float64
	// Threshold (H) is the decision boundary (often 4.0 * sigma to 5.0 * sigma) that triggers a change alert.
	Threshold float64
	// ResetOnAlert indicates whether the cumulative sum should reset to zero after triggering an alert.
	ResetOnAlert bool
}

// CUSUMDirection indicates whether an alert was triggered by an upward or downward mean shift.
type CUSUMDirection int

const (
	// DirectionNone indicates no threshold breach.
	DirectionNone CUSUMDirection = iota
	// DirectionHigh indicates an upward shift (S+ > H).
	DirectionHigh
	// DirectionLow indicates a downward shift (S- < -H).
	DirectionLow
)

// CUSUMAlert represents a change-point detection notification.
type CUSUMAlert struct {
	SampleIndex int64
	Value       float64
	Direction   CUSUMDirection
	HighSum     float64
	LowSum      float64
}

// CUSUMState maintains the ongoing cumulative sum status.
type CUSUMState struct {
	Index   int64
	HighSum float64
	LowSum  float64
}

// CUSUM implements Page's Cumulative Sum control chart for early detection of mean shifts.
// It filters incoming observations and emits CUSUMAlert events whenever the high or low cumulative sum breaches Threshold.
func CUSUM[T mathops.OrderedNumber](
	ctx context.Context,
	cfg CUSUMConfig,
	in streaming.Receiver[T],
	out streaming.Sender[CUSUMAlert],
) error {
	var (
		index   int64
		highSum float64
		lowSum  float64
	)

	return in.Receive(ctx, func(ctx context.Context, item T) error {
		index++
		val := float64(item)

		// Upper CUSUM: tracks positive deviations beyond (Target + Allowance)
		highSum += val - (cfg.Target + cfg.Allowance)
		if highSum < 0 {
			highSum = 0
		}

		// Lower CUSUM: tracks negative deviations beyond (Target - Allowance)
		lowSum += val - (cfg.Target - cfg.Allowance)
		if lowSum > 0 {
			lowSum = 0
		}

		var (
			triggered bool
			dir       CUSUMDirection
		)

		if cfg.Threshold > 0 {
			if highSum >= cfg.Threshold {
				triggered = true
				dir = DirectionHigh
			} else if lowSum <= -cfg.Threshold {
				triggered = true
				dir = DirectionLow
			}
		}

		if triggered {
			alert := CUSUMAlert{
				SampleIndex: index,
				Value:       val,
				Direction:   dir,
				HighSum:     highSum,
				LowSum:      lowSum,
			}

			if cfg.ResetOnAlert {
				if dir == DirectionHigh {
					highSum = 0
				} else {
					lowSum = 0
				}
			}

			return out.Send(ctx, alert)
		}

		return nil
	})
}
