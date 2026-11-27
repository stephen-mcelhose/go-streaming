//go:build relate
// +build relate

package storage_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	mg "github.com/stephen-mcelhose/go-streaming/examples/measurement_graph"
	"github.com/stephen-mcelhose/go-streaming/examples/measurement_graph/storage"
)

// BenchmarkScalabilityMatrix explores latency and memory saturation boundaries across scale tiers:
// 10s (Unit), 1,000s (Batch), 100,000s (Stream Burst), 1,000,000s (Production Day).
// Matrix axes:
// - P: Primary Question Codes
// - C: Classes
// - I: Instances per Class
// - K: Calculations
// - M: Total Ingested Measurements
func BenchmarkScalabilityMatrix(b *testing.B) {
	tiers := []struct {
		name      string
		entities  int
		primaries int
		classes   int
		instances int
		calcs     int
		totalMeas int
	}{
		{name: "10s_Toy", entities: 5, primaries: 2, classes: 2, instances: 2, calcs: 1, totalMeas: 20},
		{name: "1000s_FieldBatch", entities: 100, primaries: 5, classes: 5, instances: 10, calcs: 5, totalMeas: 1000},
		{name: "100ks_StreamBurst", entities: 10000, primaries: 10, classes: 10, instances: 20, calcs: 10, totalMeas: 100000},
		{name: "1Ms_ProductionDay", entities: 50000, primaries: 20, classes: 20, instances: 50, calcs: 20, totalMeas: 1000000},
	}

	for _, tier := range tiers {
		b.Run(fmt.Sprintf("PrefixTrie/%s", tier.name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				store := storage.NewPrefixTrieGraphStore()
				runScaleHarness(b, store, tier.entities, tier.primaries, tier.classes, tier.instances, tier.calcs, tier.totalMeas)
				_ = store.Close()
			}
		})

		// RelateDB Native (run up to 1000s / 10ks in standard benchmark cycle to measure Cypher boundary)
		if tier.totalMeas <= 1000 {
			b.Run(fmt.Sprintf("RelateDB_Native/%s", tier.name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					store, err := storage.NewNativeRelateDBGraphStore(":memory:")
					if err != nil {
						b.Fatalf("failed to create NativeRelateDB: %v", err)
					}
					runScaleHarness(b, store, tier.entities, tier.primaries, tier.classes, tier.instances, tier.calcs, tier.totalMeas)
					_ = store.Close()
				}
			})
		}
	}
}

func runScaleHarness(
	b *testing.B,
	store storage.GraphStore,
	numEntities, numPrimaries, numClasses, numInstances, numCalcs, totalMeas int,
) {
	ctx := context.Background()
	engine := mg.NewEngine(store)

	// 1. Register Classifier Rules across Primaries -> Classes -> Instances
	for p := 0; p < numPrimaries; p++ {
		primaryCode := fmt.Sprintf("PRIMARY_Q_%d", p)
		for c := 0; c < numClasses; c++ {
			classID := fmt.Sprintf("CLASS_C_%d", c)
			for inst := 0; inst < numInstances; inst++ {
				instanceID := fmt.Sprintf("INST_%d_%d_%d", p, c, inst)
				engine.RegisterClassifierRule(mg.InstanceClassifier{
					PrimaryCode: primaryCode,
					ClassID:     classID,
					InstanceID:  instanceID,
					ClosedValues: map[string]string{
						"TAXA": "Corn",
						"TIER": fmt.Sprintf("T%d", inst%3),
					},
				})
			}
		}
	}

	// 2. Register Calculations requiring pairs of Classes
	for k := 0; k < numCalcs; k++ {
		c1 := fmt.Sprintf("CLASS_C_%d", k%numClasses)
		c2 := fmt.Sprintf("CLASS_C_%d", (k+1)%numClasses)
		calcCode := fmt.Sprintf("CALC_DERIVED_%d", k)
		outMetric := fmt.Sprintf("OUTPUT_M_%d", k)

		engine.RegisterCalculation(mg.StatefulCalculation{
			CalculationCode: calcCode,
			OutputMetric:    outMetric,
			RequiredSlots: []mg.ClassSelector{
				{SlotName: "V1", ClassID: c1},
				{SlotName: "V2", ClassID: c2},
			},
			Compute: func(ctx context.Context, entity mg.EntityIdentity, inputs map[string]mg.StoredMeasurement) (mg.StreamingMeasurement[float64], error) {
				v1 := inputs["V1"].Value
				v2 := inputs["V2"].Value
				return mg.StreamingMeasurement[float64]{
					Identity: entity,
					Primary:  mg.KeyValue[string, float64]{Key: outMetric, Value: v1 + v2},
					Labels:   []mg.KeyValue[string, string]{{Key: "TAXA", Value: "Corn"}, {Key: "TIER", Value: "T0"}},
				}, nil
			},
		})
	}

	// 3. Generate and Ingest Total Measurements
	rnd := rand.New(rand.NewSource(42))
	for m := 0; m < totalMeas; m++ {
		entityIdx := m % numEntities
		primaryIdx := m % numPrimaries
		tierIdx := rnd.Intn(3)

		meas := mg.StreamingMeasurement[float64]{
			Identity: mg.EntityIdentity{
				BusinessUnit: "Agronomy",
				EntityType:   "PLOT",
				EntityID:     fmt.Sprintf("plot_%d", entityIdx),
			},
			Primary: mg.KeyValue[string, float64]{
				Key:   fmt.Sprintf("PRIMARY_Q_%d", primaryIdx),
				Value: 100.0 + rnd.Float64()*50.0,
			},
			Labels: []mg.KeyValue[string, string]{
				{Key: "TAXA", Value: "Corn"},
				{Key: "TIER", Value: fmt.Sprintf("T%d", tierIdx)},
			},
			Timestamp: time.Now(),
		}

		_, err := engine.ClassifyAndPlace(ctx, meas)
		if err != nil {
			b.Fatalf("ClassifyAndPlace failed: %v", err)
		}
	}
}
