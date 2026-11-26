package examples_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	mathops "github.com/stephen-mcelhose/go-streaming/operators/math"
	"github.com/stephen-mcelhose/go-streaming/operators/pure"
	"github.com/stephen-mcelhose/go-streaming/operators/routing"
)

// KeyValue represents a generic key-value pair used for labeled metadata and secondary values.
type KeyValue[K, V any] struct {
	Key   K
	Value V
}

// Measurement represents a streaming observation answering a PRIMARY_QUESTION_CODE
// alongside categorical labels and auxiliary numeric secondary measurements.
// Syntax: [PRIMARY_QUESTION_CODE]{LABELS..., SECONDARY_VALUES...}
type Measurement[N mathops.OrderedNumber] struct {
	// Primary answers the PRIMARY_QUESTION_CODE with its primary numeric value.
	Primary KeyValue[string, N]

	// Labels are categorical key-value pairs (e.g. "TENANT=corp_a", "DEVICE_MODEL=x100", "ENV=prod").
	Labels []KeyValue[string, string]

	// Secondaries are auxiliary mathable numeric readings (e.g. "TEMPERATURE=37.2", "PULSE=72.0").
	Secondaries []KeyValue[string, N]
}

// HasLabelKey returns true if the specified label key exists on the measurement.
func (m Measurement[N]) HasLabelKey(key string) bool {
	for _, l := range m.Labels {
		if l.Key == key {
			return true
		}
	}
	return false
}

// GetLabel returns the value for a label key and whether it was found.
func (m Measurement[N]) GetLabel(key string) (string, bool) {
	for _, l := range m.Labels {
		if l.Key == key {
			return l.Value, true
		}
	}
	return "", false
}

// LabelMatcher evaluates whether a measurement's labels satisfy classification criteria.
type LabelMatcher interface {
	Matches(labels []KeyValue[string, string]) bool
}

// KeyExistsMatcher matches if the label key exists on the measurement, regardless of its value.
type KeyExistsMatcher struct {
	Key string
}

func (m KeyExistsMatcher) Matches(labels []KeyValue[string, string]) bool {
	for _, l := range labels {
		if l.Key == m.Key {
			return true
		}
	}
	return false
}

// ExactKeyValueMatcher matches if the label key exists AND equals the target value.
type ExactKeyValueMatcher struct {
	Key   string
	Value string
}

func (m ExactKeyValueMatcher) Matches(labels []KeyValue[string, string]) bool {
	for _, l := range labels {
		if l.Key == m.Key && l.Value == m.Value {
			return true
		}
	}
	return false
}

// CompositeSelector matches if all specified matchers pass (logical AND).
type CompositeSelector struct {
	Matchers []LabelMatcher
}

func (s CompositeSelector) Matches(labels []KeyValue[string, string]) bool {
	for _, m := range s.Matchers {
		if !m.Matches(labels) {
			return false
		}
	}
	return true
}

// ParseSelector parses human-readable selector strings.
// Rule formats:
// - "KEY": matches if KEY exists on the measurement (any value)
// - "KEY=VALUE": matches if KEY exists and equals VALUE
func ParseSelector(rules ...string) CompositeSelector {
	var matchers []LabelMatcher
	for _, rule := range rules {
		if strings.Contains(rule, "=") {
			parts := strings.SplitN(rule, "=", 2)
			matchers = append(matchers, ExactKeyValueMatcher{Key: parts[0], Value: parts[1]})
		} else {
			matchers = append(matchers, KeyExistsMatcher{Key: rule})
		}
	}
	return CompositeSelector{Matchers: matchers}
}

func TestMeasurementClassification(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST match measurements by key existence and exact key-value pairs", func(t *testing.T) {
		m1 := Measurement[float64]{
			Primary: KeyValue[string, float64]{Key: "BLOOD_PRESSURE_SYS", Value: 120.0},
			Labels: []KeyValue[string, string]{
				{Key: "CALIBRATED", Value: "true"},
				{Key: "ENV", Value: "prod"},
				{Key: "CLINIC_ID", Value: "c_101"},
			},
			Secondaries: []KeyValue[string, float64]{
				{Key: "PULSE", Value: 72.0},
			},
		}

		m2 := Measurement[float64]{
			Primary: KeyValue[string, float64]{Key: "BLOOD_PRESSURE_SYS", Value: 145.0},
			Labels: []KeyValue[string, string]{
				{Key: "ENV", Value: "dev"},
				{Key: "CLINIC_ID", Value: "c_101"},
			},
		}

		// Selector requires CALIBRATED to exist, and ENV to equal "prod"
		selector := ParseSelector("CALIBRATED", "ENV=prod")

		require.True(t, selector.Matches(m1.Labels), "m1 has CALIBRATED and ENV=prod")
		require.False(t, selector.Matches(m2.Labels), "m2 lacks CALIBRATED and ENV is dev")
	})

	t.Run("MUST filter and partition streaming measurements by primary question code and labels", func(t *testing.T) {
		inCh := make(chan Measurement[float64], 3)
		inCh <- Measurement[float64]{
			Primary: KeyValue[string, float64]{Key: "BP_SYS", Value: 120.0},
			Labels:  []KeyValue[string, string]{{Key: "STAGE", Value: "prod"}},
		}
		inCh <- Measurement[float64]{
			Primary: KeyValue[string, float64]{Key: "GLUCOSE", Value: 95.0},
			Labels:  []KeyValue[string, string]{{Key: "STAGE", Value: "prod"}},
		}
		inCh <- Measurement[float64]{
			Primary: KeyValue[string, float64]{Key: "BP_SYS", Value: 130.0},
			Labels:  []KeyValue[string, string]{{Key: "STAGE", Value: "dev"}},
		}
		close(inCh)

		// Filter only STAGE=prod measurements
		prodSelector := ParseSelector("STAGE=prod")
		filteredCh := make(chan Measurement[float64], 3)

		go func() {
			defer close(filteredCh)
			_ = pure.Filter(ctx, func(m Measurement[float64]) bool {
				return prodSelector.Matches(m.Labels)
			}, streaming.NewChanStream(inCh, nil), streaming.NewChanStream(nil, filteredCh))
		}()

		// Partition by primary question code
		bpCh := make(chan Measurement[float64], 2)
		glucoseCh := make(chan Measurement[float64], 2)

		routes := map[string]streaming.Sender[Measurement[float64]]{
			"BP_SYS":  streaming.NewChanStream(nil, bpCh),
			"GLUCOSE": streaming.NewChanStream(nil, glucoseCh),
		}

		_ = routing.Partition(ctx,
			func(m Measurement[float64]) (string, error) { return m.Primary.Key, nil },
			routes,
			nil,
			streaming.NewChanStream(filteredCh, nil),
		)

		close(bpCh)
		close(glucoseCh)

		var bpItems []Measurement[float64]
		for item := range bpCh {
			bpItems = append(bpItems, item)
		}

		require.Equal(t, 1, len(bpItems))
		require.Equal(t, "BP_SYS", bpItems[0].Primary.Key)
		require.Equal(t, 120.0, bpItems[0].Primary.Value)
	})
}

// Example_measurementStreaming demonstrates how to process a stream of Measurement items:
// 1. Ingests [PRIMARY_QUESTION_CODE]{LABELS..., SECONDARY_VALUES...} observations.
// 2. Classifies measurements using a label selector ("VERIFIED" key existence + "DEPARTMENT=cardiology").
// 3. Extracts primary values and computes single-pass Welford statistical summary moments on the classified stream.
func Example_measurementStreaming() {
	ctx := context.Background()

	// Ingress stream of clinical telemetry measurements
	measurements := make(chan Measurement[float64], 4)

	// Observation 1: Verified Cardiology BP reading
	measurements <- Measurement[float64]{
		Primary: KeyValue[string, float64]{Key: "BP_SYS", Value: 118.0},
		Labels: []KeyValue[string, string]{
			{Key: "VERIFIED", Value: "true"},
			{Key: "DEPARTMENT", Value: "cardiology"},
			{Key: "PATIENT_TIER", Value: "outpatient"},
		},
		Secondaries: []KeyValue[string, float64]{
			{Key: "PULSE", Value: 68.0},
			{Key: "TEMP_C", Value: 36.8},
		},
	}

	// Observation 2: Unverified reading (missing VERIFIED label) -> should be excluded
	measurements <- Measurement[float64]{
		Primary: KeyValue[string, float64]{Key: "BP_SYS", Value: 160.0},
		Labels: []KeyValue[string, string]{
			{Key: "DEPARTMENT", Value: "cardiology"},
		},
	}

	// Observation 3: Verified Cardiology BP reading
	measurements <- Measurement[float64]{
		Primary: KeyValue[string, float64]{Key: "BP_SYS", Value: 124.0},
		Labels: []KeyValue[string, string]{
			{Key: "VERIFIED", Value: "true"},
			{Key: "DEPARTMENT", Value: "cardiology"},
		},
		Secondaries: []KeyValue[string, float64]{
			{Key: "PULSE", Value: 74.0},
		},
	}

	// Observation 4: Verified Neurology reading (DEPARTMENT != cardiology) -> should be excluded
	measurements <- Measurement[float64]{
		Primary: KeyValue[string, float64]{Key: "BP_SYS", Value: 135.0},
		Labels: []KeyValue[string, string]{
			{Key: "VERIFIED", Value: "true"},
			{Key: "DEPARTMENT", Value: "neurology"},
		},
	}
	close(measurements)

	// Step 1: Filter stream using Label Selector
	// Classification rules:
	// - "VERIFIED": Label key must exist on the measurement (any value)
	// - "DEPARTMENT=cardiology": Label key must exist and match "cardiology"
	cardioSelector := ParseSelector("VERIFIED", "DEPARTMENT=cardiology")

	classifiedCh := make(chan Measurement[float64], 4)
	go func() {
		defer close(classifiedCh)
		_ = pure.Filter(ctx, func(m Measurement[float64]) bool {
			return cardioSelector.Matches(m.Labels)
		}, streaming.NewChanStream(measurements, nil), streaming.NewChanStream(nil, classifiedCh))
	}()

	// Step 2: Extract primary values and accumulate online Welford statistics
	w := mathops.NewWelford()
	for m := range classifiedCh {
		w.Update(m.Primary.Value)
		fmt.Printf("Classified [%s]: Primary=%.1f | Secondaries=%d\n",
			m.Primary.Key, m.Primary.Value, len(m.Secondaries))
	}

	stats := w.Snapshot()
	fmt.Printf("Cardiology Cohort Stats: Count=%d | Mean BP=%.1f | StdDev=%.1f\n",
		stats.Count, stats.Mean, stats.StdDev)

	// Output:
	// Classified [BP_SYS]: Primary=118.0 | Secondaries=2
	// Classified [BP_SYS]: Primary=124.0 | Secondaries=1
	// Cardiology Cohort Stats: Count=2 | Mean BP=121.0 | StdDev=4.2
}
