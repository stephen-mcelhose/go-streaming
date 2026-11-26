package examples_test

import (
	"context"
	"fmt"
	"math/cmplx"

	"github.com/stephen-mcelhose/go-streaming"
	mathops "github.com/stephen-mcelhose/go-streaming/operators/math"
	"github.com/stephen-mcelhose/go-streaming/operators/pure"
)

// Example_mather demonstrates how a Mathematician or Signal Processing Engineer
// uses go-streaming with complex arithmetic, phase transformations, and clean functional operators.
func Example_mather() {
	ctx := context.Background()

	// Ingress stream of complex signal samples: z = x + iy
	signalCh := make(chan complex128, 3)
	signalCh <- 1.0 + 1.0i
	signalCh <- 0.0 + 2.0i
	signalCh <- 3.0 + 4.0i
	close(signalCh)

	// Step 1: Pure mathematical transformation computing phase angle (argument) for each complex number
	phaseCh := make(chan float64, 3)
	_ = pure.Map(ctx, func(z complex128) float64 {
		return cmplx.Phase(z) // Pure math without error tuples
	}, streaming.NewChanStream(signalCh, nil), streaming.NewChanStream(nil, phaseCh))
	close(phaseCh)

	// Step 2: Compute running sum of total complex vector energy
	vectorCh := make(chan complex128, 3)
	vectorCh <- 1.0 + 2.0i
	vectorCh <- 2.0 + 1.0i
	vectorCh <- 3.0 + 3.0i
	close(vectorCh)

	sumSink := make(chan complex128, 1)
	_ = mathops.Sum(ctx, streaming.NewChanStream(vectorCh, nil), streaming.NewChanStream(nil, sumSink))
	close(sumSink)

	totalVector := <-sumSink
	fmt.Printf("Total Vector Sum: %.1f + %.1fi\n", real(totalVector), imag(totalVector))

	// Output:
	// Total Vector Sum: 6.0 + 6.0i
}
