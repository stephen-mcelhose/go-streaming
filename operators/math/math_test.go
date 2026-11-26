package math_test

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	mathops "github.com/stephen-mcelhose/go-streaming/operators/math"
)

func TestSum(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST sum integer stream", func(t *testing.T) {
		inCh := make(chan int, 4)
		inCh <- 10
		inCh <- 20
		inCh <- 30
		inCh <- 40
		close(inCh)

		outCh := make(chan int, 1)
		err := mathops.Sum(ctx, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh))
		require.NoError(t, err)
		close(outCh)

		require.Equal(t, 100, <-outCh)
	})

	t.Run("MUST sum complex128 stream", func(t *testing.T) {
		inCh := make(chan complex128, 2)
		inCh <- 1 + 2i
		inCh <- 3 + 4i
		close(inCh)

		outCh := make(chan complex128, 1)
		err := mathops.Sum(ctx, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh))
		require.NoError(t, err)
		close(outCh)

		require.Equal(t, 4+6i, <-outCh)
	})
}

func TestRunningSum(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST emit running sum for floats", func(t *testing.T) {
		inCh := make(chan float64, 3)
		inCh <- 1.5
		inCh <- 2.5
		inCh <- 3.0
		close(inCh)

		outCh := make(chan float64, 3)
		err := mathops.RunningSum(ctx, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh))
		require.NoError(t, err)
		close(outCh)

		var running []float64
		for v := range outCh {
			running = append(running, v)
		}
		require.Equal(t, []float64{1.5, 4.0, 7.0}, running)
	})

	t.Run("MUST emit running sum for complex128", func(t *testing.T) {
		inCh := make(chan complex128, 2)
		inCh <- 1 + 1i
		inCh <- 2 + 3i
		close(inCh)

		outCh := make(chan complex128, 2)
		err := mathops.RunningSum(ctx, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, outCh))
		require.NoError(t, err)
		close(outCh)

		var running []complex128
		for v := range outCh {
			running = append(running, v)
		}
		require.Equal(t, []complex128{1 + 1i, 3 + 4i}, running)
	})
}

func TestStats(t *testing.T) {
	ctx := context.Background()

	inCh := make(chan float64, 5)
	inCh <- 10.0
	inCh <- 20.0
	inCh <- 30.0
	inCh <- 40.0
	inCh <- 50.0
	close(inCh)

	outCh := make(chan mathops.StatsSnapshot, 1)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	err := mathops.Stats(ctx, inStream, outStream)
	require.NoError(t, err)
	close(outCh)

	stats := <-outCh
	require.Equal(t, int64(5), stats.Count)
	require.Equal(t, 150.0, stats.Sum)
	require.Equal(t, 10.0, stats.Min)
	require.Equal(t, 50.0, stats.Max)
	require.Equal(t, 30.0, stats.Mean)
	require.Equal(t, 250.0, stats.Variance)
	require.InDelta(t, math.Sqrt(250.0), stats.StdDev, 0.0001)
}

func TestEMA(t *testing.T) {
	ctx := context.Background()

	inCh := make(chan float64, 3)
	inCh <- 10.0
	inCh <- 20.0
	inCh <- 30.0
	close(inCh)

	outCh := make(chan float64, 3)
	inStream := streaming.NewChanStream(inCh, nil)
	outStream := streaming.NewChanStream(nil, outCh)

	// alpha = 0.5 -> ema[0]=10, ema[1]=0.5*20 + 0.5*10 = 15, ema[2]=0.5*30 + 0.5*15 = 22.5
	err := mathops.EMA(ctx, 0.5, inStream, outStream)
	require.NoError(t, err)
	close(outCh)

	var emaValues []float64
	for v := range outCh {
		emaValues = append(emaValues, v)
	}
	require.Equal(t, []float64{10.0, 15.0, 22.5}, emaValues)
}
