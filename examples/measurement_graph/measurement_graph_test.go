package measurement_graph_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	mg "github.com/stephen-mcelhose/go-streaming/examples/measurement_graph"
	"github.com/stephen-mcelhose/go-streaming/examples/measurement_graph/storage"
)

func TestMeasurementGraphClassification(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST classify and store measurements into entity node coordinates", func(t *testing.T) {
		relateStore, err := storage.NewRelateDBGraphStore(storage.RelateDBConfig{InMemory: true})
		require.NoError(t, err)
		defer relateStore.Close()

		engine := mg.NewEngine(relateStore)

		// Register schema instance rules mapping to Primary:Class:Instance:[Characteristics]
		engine.RegisterClassifierRule(mg.InstanceClassifier{
			PrimaryCode: "GRAIN_YIELD",
			ClassID:     "CLASS_GRAIN_YIELD",
			InstanceID:  "C4220-I2",
			ClosedValues: map[string]string{
				"TAXA":   "Corn",
				"METHOD": "Machine Harvest",
			},
			Characteristics: map[string]string{
				"SENSOR_TIER": "PRECISION_A",
			},
		})
		engine.RegisterClassifierRule(mg.InstanceClassifier{
			PrimaryCode: "MOISTURE",
			ClassID:     "CLASS_MOISTURE",
			InstanceID:  "C5343-I1",
			ClosedValues: map[string]string{
				"TAXA": "Corn",
			},
			OpenKeys: []string{"OPERATOR"},
		})

		// Incoming stream of observations
		inCh := make(chan mg.StreamingMeasurement[float64], 3)
		// Measurement 1: Matches C4220-I2
		inCh <- mg.StreamingMeasurement[float64]{
			Identity: mg.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_101"},
			Primary:  mg.KeyValue[string, float64]{Key: "GRAIN_YIELD", Value: 185.4},
			Labels: []mg.KeyValue[string, string]{
				{Key: "TAXA", Value: "Corn"},
				{Key: "METHOD", Value: "Machine Harvest"},
			},
			Timestamp: time.Now(),
		}
		// Measurement 2: Matches C5343-I1
		inCh <- mg.StreamingMeasurement[float64]{
			Identity: mg.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_101"},
			Primary:  mg.KeyValue[string, float64]{Key: "MOISTURE", Value: 14.2},
			Labels: []mg.KeyValue[string, string]{
				{Key: "TAXA", Value: "Corn"},
				{Key: "OPERATOR", Value: "tech_42"},
			},
			Timestamp: time.Now(),
		}
		// Measurement 3: Unmatched (TAXA=Soybean)
		inCh <- mg.StreamingMeasurement[float64]{
			Identity: mg.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_101"},
			Primary:  mg.KeyValue[string, float64]{Key: "GRAIN_YIELD", Value: 55.0},
			Labels: []mg.KeyValue[string, string]{
				{Key: "TAXA", Value: "Soybean"},
			},
			Timestamp: time.Now(),
		}
		close(inCh)

		unmatchedCh := make(chan mg.StreamingMeasurement[float64], 3)

		err = mg.Router(ctx, engine,
			streaming.NewChanStream(inCh, nil),
			nil,
			streaming.NewChanStream(nil, unmatchedCh),
		)
		require.NoError(t, err)
		close(unmatchedCh)

		var unmatched []mg.StreamingMeasurement[float64]
		for u := range unmatchedCh {
			unmatched = append(unmatched, u)
		}

		// Assertions (RFC 2119)
		require.Equal(t, 1, len(unmatched))
		count, err := engine.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, count)

		// Verify stored NodeCoordinates
		expectedNode1 := mg.NodeCoordinate{
			Entity:      mg.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_101"},
			PrimaryCode: "GRAIN_YIELD",
			ClassID:     "CLASS_GRAIN_YIELD",
			InstanceID:  "C4220-I2",
			Characteristics: map[string]string{
				"SENSOR_TIER": "PRECISION_A",
			},
		}
		stored1, found1, err := engine.GetMeasurement(ctx, expectedNode1)
		require.NoError(t, err)
		require.True(t, found1)
		require.InDelta(t, 185.4, stored1.Value, 1e-6)

		expectedNode2 := mg.NodeCoordinate{
			Entity:      mg.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_101"},
			PrimaryCode: "MOISTURE",
			ClassID:     "CLASS_MOISTURE",
			InstanceID:  "C5343-I1",
		}
		stored2, found2, err := engine.GetMeasurement(ctx, expectedNode2)
		require.NoError(t, err)
		require.True(t, found2)
		require.InDelta(t, 14.2, stored2.Value, 1e-6)
	})

	t.Run("MUST trigger calculation by resolving ClassSelectors across different instances", func(t *testing.T) {
		engine := mg.NewEngine(storage.NewPrefixTrieGraphStore())

		// Ingestion classifies combine harvester observation into CLASS_GRAIN_YIELD
		engine.RegisterClassifierRule(mg.InstanceClassifier{
			PrimaryCode: "GRAIN_YIELD",
			ClassID:     "CLASS_GRAIN_YIELD",
			InstanceID:  "COMBINE_MODEL_X",
			ClosedValues: map[string]string{
				"TAXA": "Corn",
			},
		})
		// Ingestion classifies lab moisture reading into CLASS_MOISTURE
		engine.RegisterClassifierRule(mg.InstanceClassifier{
			PrimaryCode: "MOISTURE",
			ClassID:     "CLASS_MOISTURE",
			InstanceID:  "LAB_OVEN_BENCH",
			ClosedValues: map[string]string{
				"TAXA": "Corn",
			},
		})

		// Register calculation: depends on CLASS_GRAIN_YIELD and CLASS_MOISTURE, completely decoupled from instance IDs
		engine.RegisterCalculation(mg.StatefulCalculation{
			CalculationCode: "STANDARDIZED_YIELD",
			OutputMetric:    "ADJ_YIELD",
			RequiredSlots: []mg.ClassSelector{
				{SlotName: "Yield", ClassID: "CLASS_GRAIN_YIELD"},
				{SlotName: "Moisture", ClassID: "CLASS_MOISTURE"},
			},
			Compute: func(ctx context.Context, entity mg.EntityIdentity, inputs map[string]mg.StoredMeasurement) (mg.StreamingMeasurement[float64], error) {
				y := inputs["Yield"].Value
				m := inputs["Moisture"].Value
				adj := y * (100.0 - m) / (100.0 - 15.5)
				return mg.StreamingMeasurement[float64]{
					Identity: entity,
					Primary:  mg.KeyValue[string, float64]{Key: "ADJ_YIELD", Value: adj},
				}, nil
			},
		})

		producerCh := make(chan mg.StreamingMeasurement[float64], 2)
		entity := mg.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_500"}

		// Observation 1: Ingests Grain Yield (calculation should NOT trigger yet)
		producerCh <- mg.StreamingMeasurement[float64]{
			Identity:  entity,
			Primary:   mg.KeyValue[string, float64]{Key: "GRAIN_YIELD", Value: 200.0},
			Labels:    []mg.KeyValue[string, string]{{Key: "TAXA", Value: "Corn"}},
			Timestamp: time.Now(),
		}
		// Observation 2: Ingests Moisture (calculation MUST now trigger via Class matching!)
		producerCh <- mg.StreamingMeasurement[float64]{
			Identity:  entity,
			Primary:   mg.KeyValue[string, float64]{Key: "MOISTURE", Value: 15.5},
			Labels:    []mg.KeyValue[string, string]{{Key: "TAXA", Value: "Corn"}},
			Timestamp: time.Now(),
		}
		close(producerCh)

		calcOutputCh := make(chan mg.StreamingMeasurement[float64], 5)

		err := mg.Router(ctx, engine,
			streaming.NewChanStream(producerCh, nil),
			streaming.NewChanStream(nil, calcOutputCh),
			nil,
		)
		require.NoError(t, err)
		close(calcOutputCh)

		var results []mg.StreamingMeasurement[float64]
		for r := range calcOutputCh {
			results = append(results, r)
		}

		require.Equal(t, 1, len(results))
		require.Equal(t, "ADJ_YIELD", results[0].Primary.Key)
		require.InDelta(t, 200.0, results[0].Primary.Value, 1e-6) // At 15.5% moisture, adjusted == raw
	})

	t.Run("MUST cascade calculation outputs forward into downstream calculations", func(t *testing.T) {
		engine := mg.NewEngine(storage.NewPrefixTrieGraphStore())

		// Rule 1: Ingests Grain Yield
		engine.RegisterClassifierRule(mg.InstanceClassifier{
			PrimaryCode:  "GRAIN_YIELD",
			ClassID:      "CLASS_GRAIN_YIELD",
			InstanceID:   "COMBINE_ROTARY",
			ClosedValues: map[string]string{"TAXA": "Corn"},
		})
		// Rule 2: Ingests Moisture
		engine.RegisterClassifierRule(mg.InstanceClassifier{
			PrimaryCode:  "MOISTURE",
			ClassID:      "CLASS_MOISTURE",
			InstanceID:   "PROBE_NIR",
			ClosedValues: map[string]string{"TAXA": "Corn"},
		})
		// Rule 3: Classifies derived Standardized Yield
		engine.RegisterClassifierRule(mg.InstanceClassifier{
			PrimaryCode:  "GRAIN_YIELD_STD15",
			ClassID:      "CLASS_STD_YIELD",
			InstanceID:   "CALC_ENGINE_DERIVED",
			ClosedValues: map[string]string{"TAXA": "Corn"},
		})
		// Rule 4: Ingests Target Baseline
		engine.RegisterClassifierRule(mg.InstanceClassifier{
			PrimaryCode:  "TARGET_YIELD",
			ClassID:      "CLASS_TARGET_YIELD",
			InstanceID:   "PLANNING_SPEC",
			ClosedValues: map[string]string{"TAXA": "Corn"},
		})

		// Tier 1 Calculation: Standardized Yield = Yield * (100 - Moisture) / (100 - 15.5)
		engine.RegisterCalculation(mg.StatefulCalculation{
			CalculationCode: "CALC_STD_YIELD",
			OutputMetric:    "GRAIN_YIELD_STD15",
			RequiredSlots: []mg.ClassSelector{
				{SlotName: "Yield", ClassID: "CLASS_GRAIN_YIELD"},
				{SlotName: "Moisture", ClassID: "CLASS_MOISTURE"},
			},
			Compute: func(ctx context.Context, entity mg.EntityIdentity, inputs map[string]mg.StoredMeasurement) (mg.StreamingMeasurement[float64], error) {
				y := inputs["Yield"].Value
				m := inputs["Moisture"].Value
				adj := y * (100.0 - m) / (100.0 - 15.5)
				return mg.StreamingMeasurement[float64]{
					Identity: entity,
					Primary:  mg.KeyValue[string, float64]{Key: "GRAIN_YIELD_STD15", Value: adj},
					Labels:   []mg.KeyValue[string, string]{{Key: "TAXA", Value: "Corn"}},
				}, nil
			},
		})

		// Tier 2 Calculation: Performance Index = (Standardized Yield / Target Yield) * 100
		engine.RegisterCalculation(mg.StatefulCalculation{
			CalculationCode: "CALC_PERF_INDEX",
			OutputMetric:    "PERFORMANCE_INDEX",
			RequiredSlots: []mg.ClassSelector{
				{SlotName: "StdYield", ClassID: "CLASS_STD_YIELD"},
				{SlotName: "Target", ClassID: "CLASS_TARGET_YIELD"},
			},
			Compute: func(ctx context.Context, entity mg.EntityIdentity, inputs map[string]mg.StoredMeasurement) (mg.StreamingMeasurement[float64], error) {
				stdY := inputs["StdYield"].Value
				target := inputs["Target"].Value
				idx := (stdY / target) * 100.0
				return mg.StreamingMeasurement[float64]{
					Identity: entity,
					Primary:  mg.KeyValue[string, float64]{Key: "PERFORMANCE_INDEX", Value: idx},
					Labels:   []mg.KeyValue[string, string]{{Key: "TAXA", Value: "Corn"}},
				}, nil
			},
		})

		entity := mg.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_cascade_999"}

		// Step 1: Pre-populate Target Yield
		_, err := engine.ClassifyAndPlace(ctx, mg.StreamingMeasurement[float64]{
			Identity:  entity,
			Primary:   mg.KeyValue[string, float64]{Key: "TARGET_YIELD", Value: 200.0},
			Labels:    []mg.KeyValue[string, string]{{Key: "TAXA", Value: "Corn"}},
			Timestamp: time.Now(),
		})
		require.NoError(t, err)

		// Step 2: Feed Grain Yield (200.0)
		_, err = engine.ClassifyAndPlace(ctx, mg.StreamingMeasurement[float64]{
			Identity:  entity,
			Primary:   mg.KeyValue[string, float64]{Key: "GRAIN_YIELD", Value: 200.0},
			Labels:    []mg.KeyValue[string, string]{{Key: "TAXA", Value: "Corn"}},
			Timestamp: time.Now(),
		})
		require.NoError(t, err)

		// Step 3: Feed Moisture (15.5%) -> Should trigger Tier 1 (StdYield=200.0), which naturally feeds back and triggers Tier 2 (PERF_INDEX=100.0)!
		res, err := engine.ClassifyAndPlace(ctx, mg.StreamingMeasurement[float64]{
			Identity:  entity,
			Primary:   mg.KeyValue[string, float64]{Key: "MOISTURE", Value: 15.5},
			Labels:    []mg.KeyValue[string, string]{{Key: "TAXA", Value: "Corn"}},
			Timestamp: time.Now(),
		})
		require.NoError(t, err)

		// Assert both Tier 1 and Tier 2 triggered in one cascade
		require.Len(t, res.Triggered, 2)
		require.Equal(t, "GRAIN_YIELD_STD15", res.Triggered[0].Primary.Key)
		require.InDelta(t, 200.0, res.Triggered[0].Primary.Value, 1e-6)

		require.Equal(t, "PERFORMANCE_INDEX", res.Triggered[1].Primary.Key)
		require.InDelta(t, 100.0, res.Triggered[1].Primary.Value, 1e-6)
	})
}

// Example_measurementTwoStreamPipeline demonstrates the complete two-stream reactive model:
//  1. Stream 1 (Producer): Emits incoming domain measurements from field IoT / lab analyzers.
//  2. Stream 2 (Router & Engine): Dynamically classifies measurements into Primary:Class:Instance hierarchy
//     and triggers multi-variable calculations based on declared ClassSelectors.
func Example_measurementTwoStreamPipeline() {
	ctx := context.Background()
	engine := mg.NewEngine(storage.NewPrefixTrieGraphStore())

	// 1. Setup Classification Hierarchy Rules: Primary -> Class -> Instance
	engine.RegisterClassifierRule(mg.InstanceClassifier{
		PrimaryCode: "GRAIN_YIELD",
		ClassID:     "CLASS_GRAIN_YIELD",
		InstanceID:  "COMBINE_HARVESTER_SERIES7",
		ClosedValues: map[string]string{
			"TAXA":   "Corn",
			"METHOD": "Combine",
		},
	})
	engine.RegisterClassifierRule(mg.InstanceClassifier{
		PrimaryCode: "MOISTURE",
		ClassID:     "CLASS_MOISTURE",
		InstanceID:  "MOISTURE_PROBE_NEAR_INFRARED",
		ClosedValues: map[string]string{
			"TAXA": "Corn",
		},
		OpenKeys: []string{"DEVICE_ID"},
	})

	// 2. Register Calculation requiring ClassSelectors [CLASS_GRAIN_YIELD, CLASS_MOISTURE]
	engine.RegisterCalculation(mg.StatefulCalculation{
		CalculationCode: "CALC_STD_YIELD",
		OutputMetric:    "GRAIN_YIELD_STD15",
		RequiredSlots: []mg.ClassSelector{
			{SlotName: "Yield", ClassID: "CLASS_GRAIN_YIELD"},
			{SlotName: "Moisture", ClassID: "CLASS_MOISTURE"},
		},
		Compute: func(ctx context.Context, entity mg.EntityIdentity, inputs map[string]mg.StoredMeasurement) (mg.StreamingMeasurement[float64], error) {
			rawYield := inputs["Yield"].Value
			moist := inputs["Moisture"].Value
			stdYield := rawYield * (100.0 - moist) / (100.0 - 15.5)

			return mg.StreamingMeasurement[float64]{
				Identity: entity,
				Primary:  mg.KeyValue[string, float64]{Key: "GRAIN_YIELD_STD15", Value: stdYield},
				Labels: []mg.KeyValue[string, string]{
					{Key: "STATUS", Value: "CALCULATED"},
					{Key: "BASE_MOISTURE", Value: "15.5%"},
				},
			}, nil
		},
	})

	// 3. Stream 1: Producer emits raw measurements over time for entity "plot_37711542"
	plotEntity := mg.EntityIdentity{
		BusinessUnit: "Agronomy",
		EntityType:   "PLOT",
		EntityID:     "plot_37711542",
	}

	producerCh := make(chan mg.StreamingMeasurement[float64], 2)

	producerCh <- mg.StreamingMeasurement[float64]{
		Identity: plotEntity,
		Primary:  mg.KeyValue[string, float64]{Key: "GRAIN_YIELD", Value: 185.4},
		Labels: []mg.KeyValue[string, string]{
			{Key: "TAXA", Value: "Corn"},
			{Key: "METHOD", Value: "Combine"},
		},
		Timestamp: time.Now(),
	}

	producerCh <- mg.StreamingMeasurement[float64]{
		Identity: plotEntity,
		Primary:  mg.KeyValue[string, float64]{Key: "MOISTURE", Value: 17.8},
		Labels: []mg.KeyValue[string, string]{
			{Key: "TAXA", Value: "Corn"},
			{Key: "DEVICE_ID", Value: "DICKEY_JOHN_GAC2500"},
		},
		Timestamp: time.Now(),
	}
	close(producerCh)

	// Output calculations receiver stream
	outCalcCh := make(chan mg.StreamingMeasurement[float64], 5)

	// Run Stream 2: Router and Engine
	_ = mg.Router(ctx, engine,
		streaming.NewChanStream(producerCh, nil),
		streaming.NewChanStream(nil, outCalcCh),
		nil,
	)
	close(outCalcCh)

	for calc := range outCalcCh {
		fmt.Printf("Emitted: %s [%s=%.2f]\n", calc.Identity.EntityID, calc.Primary.Key, calc.Primary.Value)
	}

	// Output:
	// Emitted: plot_37711542 [GRAIN_YIELD_STD15=180.35]
}
