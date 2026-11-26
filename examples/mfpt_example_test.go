package examples_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	mathops "github.com/stephen-mcelhose/go-streaming/operators/math"
	"github.com/stephen-mcelhose/go-streaming/operators/pure"
)

// TimedReading represents a numeric measurement taken at a specific point in time.
type TimedReading struct {
	Timestamp time.Time
	Value     float64
}

// MFPTSnapshot contains running statistics on the time it takes for a process to reach a threshold.
//
// Academic Citations:
//   - Sidney Redner, "A Guide to First-Passage Processes", Cambridge University Press, 2001. https://doi.org/10.1017/CBO9780511606014
//   - S. Condamin, O. Bénichou, et al., "First-passage times in complex scale-invariant media", Nature, 450:77–80, 2007. https://doi.org/10.1038/nature06201
//   - Wikipedia: https://en.wikipedia.org/wiki/First-hitting-time_model
type MFPTSnapshot struct {
	PassageCount int           // Number of times the threshold was reached
	MeanDuration time.Duration // Running average time to reach threshold (MFPT)
	MinDuration  time.Duration // Fastest time recorded
	MaxDuration  time.Duration // Slowest time recorded
}

// mfptAccumulatorState tracks the start time of the current window and the Welford accumulator for passage times.
type mfptAccumulatorState struct {
	windowStart time.Time
	hasStart    bool
	welford     *mathops.Welford
	minDur      time.Duration
	maxDur      time.Duration
	snapshot    MFPTSnapshot
	hasCrossing bool
}

// RunningMFPT is a streaming operator that measures the Mean First Passage Time (MFPT).
// Each time the input value crosses the barrier condition:
// 1. It records the elapsed time (duration) since the start of the current cycle.
// 2. Updates the running average duration using Welford's algorithm.
// 3. Resets the cycle timer to start measuring the next passage time.
//
// Academic Citations:
//   - Sidney Redner, "A Guide to First-Passage Processes", Cambridge University Press, 2001. https://doi.org/10.1017/CBO9780511606014
//   - Wikipedia: https://en.wikipedia.org/wiki/First-hitting-time_model
func RunningMFPT(
	ctx context.Context,
	isBarrier func(val float64) bool,
	in streaming.Receiver[TimedReading],
	out streaming.Sender[MFPTSnapshot],
) error {
	initial := mfptAccumulatorState{
		welford: mathops.NewWelford(),
	}

	stateCh := make(chan mfptAccumulatorState, 10)

	// Step 1: Accumulate durations and detect barrier crossings using pure.Scan
	go func() {
		defer close(stateCh)
		_ = pure.Scan(ctx, initial, func(st mfptAccumulatorState, r TimedReading) mfptAccumulatorState {
			st.hasCrossing = false

			if !st.hasStart {
				st.windowStart = r.Timestamp
				st.hasStart = true
			}

			if isBarrier(r.Value) {
				duration := r.Timestamp.Sub(st.windowStart)
				durSeconds := duration.Seconds()

				st.welford.Update(durSeconds)
				count := st.welford.Count()

				if count == 1 || duration < st.minDur {
					st.minDur = duration
				}
				if count == 1 || duration > st.maxDur {
					st.maxDur = duration
				}

				meanSec := st.welford.Mean()
				st.snapshot = MFPTSnapshot{
					PassageCount: int(count),
					MeanDuration: time.Duration(meanSec * float64(time.Second)),
					MinDuration:  st.minDur,
					MaxDuration:  st.maxDur,
				}

				st.hasCrossing = true
				st.windowStart = r.Timestamp // Reset for next cycle
			}

			return st
		}, in, streaming.NewChanStream(nil, stateCh))
	}()

	// Step 2: Forward snapshots only when a threshold crossing occurred
	for st := range stateCh {
		if st.hasCrossing {
			if err := out.Send(ctx, st.snapshot); err != nil {
				return err
			}
		}
	}

	return nil
}

func TestMFPT(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST accurately compute mean first passage times across renewal cycles", func(t *testing.T) {
		baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

		inCh := make(chan TimedReading, 8)
		// Cycle 1: starts t=0, hits barrier at t=3s (duration = 3s)
		inCh <- TimedReading{Timestamp: baseTime, Value: 10.0}
		inCh <- TimedReading{Timestamp: baseTime.Add(1 * time.Second), Value: 20.0}
		inCh <- TimedReading{Timestamp: baseTime.Add(3 * time.Second), Value: 100.0} // Barrier crossed!

		// Cycle 2: starts t=3s, hits barrier at t=8s (duration = 5s)
		inCh <- TimedReading{Timestamp: baseTime.Add(4 * time.Second), Value: 15.0}
		inCh <- TimedReading{Timestamp: baseTime.Add(8 * time.Second), Value: 105.0} // Barrier crossed!
		close(inCh)

		outCh := make(chan MFPTSnapshot, 5)
		isBarrier := func(v float64) bool { return v >= 100.0 }

		err := RunningMFPT(ctx, isBarrier, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh))
		require.NoError(t, err)
		close(outCh)

		var snapshots []MFPTSnapshot
		for s := range outCh {
			snapshots = append(snapshots, s)
		}

		require.Equal(t, 2, len(snapshots))
		require.Equal(t, 1, snapshots[0].PassageCount)
		require.Equal(t, 3*time.Second, snapshots[0].MeanDuration)

		require.Equal(t, 2, snapshots[1].PassageCount)
		require.Equal(t, 4*time.Second, snapshots[1].MeanDuration) // (3s + 5s) / 2 = 4s
		require.Equal(t, 3*time.Second, snapshots[1].MinDuration)
		require.Equal(t, 5*time.Second, snapshots[1].MaxDuration)
	})
}

// Example_mfptRenewal demonstrates measuring how long a simulated system takes to accumulate a critical charge
// (reaching threshold = 100.0) before discharging and restarting.
func Example_mfptRenewal() {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Stream of sensor readings over time
	readings := make(chan TimedReading, 6)
	// Cycle 1: reaches 100.0 after 2 seconds
	readings <- TimedReading{Timestamp: baseTime, Value: 10.0}
	readings <- TimedReading{Timestamp: baseTime.Add(2 * time.Second), Value: 100.0}

	// Cycle 2: reaches 100.0 after 4 seconds (at t=6s)
	readings <- TimedReading{Timestamp: baseTime.Add(3 * time.Second), Value: 30.0}
	readings <- TimedReading{Timestamp: baseTime.Add(6 * time.Second), Value: 100.0}
	close(readings)

	snapshots := make(chan MFPTSnapshot, 5)
	isBarrier := func(v float64) bool { return v >= 100.0 }

	_ = RunningMFPT(ctx, isBarrier, streaming.NewChanStream(readings, nil), streaming.NewChanStream(nil, snapshots))
	close(snapshots)

	for snap := range snapshots {
		fmt.Printf("Crossing #%d: Mean Passage Time = %.1fs (Min: %.1fs, Max: %.1fs)\n",
			snap.PassageCount, snap.MeanDuration.Seconds(), snap.MinDuration.Seconds(), snap.MaxDuration.Seconds())
	}

	// Output:
	// Crossing #1: Mean Passage Time = 2.0s (Min: 2.0s, Max: 2.0s)
	// Crossing #2: Mean Passage Time = 3.0s (Min: 2.0s, Max: 4.0s)
}
