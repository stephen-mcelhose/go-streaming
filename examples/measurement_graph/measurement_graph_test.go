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
func TestMeasurementClassifier_SpecificityScoring(t *testing.T) {
	type testCase struct {
		name string

		// setup
		rules []mg.InstanceClassifier

		// inputs
		measurement mg.StreamingMeasurement[float64]

		// expected outputs
		wantFound      bool
		wantInstanceID string
		wantClassID    string
	}

	testCases := []testCase{
		{
			name: "MUST prefer exact key-value match over presence wildcard regardless of rule registration order (Wildcard first)",
			rules: []mg.InstanceClassifier{
				{
					PrimaryCode: "GRAIN_YIELD",
					ClassID:     "CLASS_GRAIN_YIELD",
					InstanceID:  "COMBINE_GENERIC_PRESENCE",
					OpenKeys:    []string{"METHOD"},
				},
				{
					PrimaryCode:  "GRAIN_YIELD",
					ClassID:      "CLASS_GRAIN_YIELD",
					InstanceID:   "COMBINE_ROTARY_EXACT",
					ClosedValues: map[string]string{"METHOD": "Rotary"},
				},
			},
			measurement: mg.StreamingMeasurement[float64]{
				Identity: mg.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_101"},
				Primary:  mg.KeyValue[string, float64]{Key: "GRAIN_YIELD", Value: 185.4},
				Labels: []mg.KeyValue[string, string]{
					{Key: "METHOD", Value: "Rotary"},
				},
			},
			wantFound:      true,
			wantInstanceID: "COMBINE_ROTARY_EXACT",
			wantClassID:    "CLASS_GRAIN_YIELD",
		},
		{
			name: "MUST prefer exact key-value match over presence wildcard regardless of rule registration order (Exact first)",
			rules: []mg.InstanceClassifier{
				{
					PrimaryCode:  "GRAIN_YIELD",
					ClassID:      "CLASS_GRAIN_YIELD",
					InstanceID:   "COMBINE_ROTARY_EXACT",
					ClosedValues: map[string]string{"METHOD": "Rotary"},
				},
				{
					PrimaryCode: "GRAIN_YIELD",
					ClassID:     "CLASS_GRAIN_YIELD",
					InstanceID:  "COMBINE_GENERIC_PRESENCE",
					OpenKeys:    []string{"METHOD"},
				},
			},
			measurement: mg.StreamingMeasurement[float64]{
				Identity: mg.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_101"},
				Primary:  mg.KeyValue[string, float64]{Key: "GRAIN_YIELD", Value: 185.4},
				Labels: []mg.KeyValue[string, string]{
					{Key: "METHOD", Value: "Rotary"},
				},
			},
			wantFound:      true,
			wantInstanceID: "COMBINE_ROTARY_EXACT",
			wantClassID:    "CLASS_GRAIN_YIELD",
		},
		{
			name: "MUST fall back to presence wildcard when exact value does not match",
			rules: []mg.InstanceClassifier{
				{
					PrimaryCode:  "GRAIN_YIELD",
					ClassID:      "CLASS_GRAIN_YIELD",
					InstanceID:   "COMBINE_ROTARY_EXACT",
					ClosedValues: map[string]string{"METHOD": "Rotary"},
				},
				{
					PrimaryCode: "GRAIN_YIELD",
					ClassID:     "CLASS_GRAIN_YIELD",
					InstanceID:  "COMBINE_GENERIC_PRESENCE",
					OpenKeys:    []string{"METHOD"},
				},
			},
			measurement: mg.StreamingMeasurement[float64]{
				Identity: mg.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_101"},
				Primary:  mg.KeyValue[string, float64]{Key: "GRAIN_YIELD", Value: 185.4},
				Labels: []mg.KeyValue[string, string]{
					{Key: "METHOD", Value: "Conventional"},
				},
			},
			wantFound:      true,
			wantInstanceID: "COMBINE_GENERIC_PRESENCE",
			wantClassID:    "CLASS_GRAIN_YIELD",
		},
		{
			name: "MUST rank candidates by total exact match count on multi-label observations",
			rules: []mg.InstanceClassifier{
				{
					PrimaryCode:  "GRAIN_YIELD",
					ClassID:      "CLASS_GRAIN_YIELD",
					InstanceID:   "COMBINE_PARTIAL_EXACT",
					ClosedValues: map[string]string{"TAXA": "Corn"},
					OpenKeys:     []string{"METHOD"},
				},
				{
					PrimaryCode: "GRAIN_YIELD",
					ClassID:     "CLASS_GRAIN_YIELD",
					InstanceID:  "COMBINE_FULL_EXACT",
					ClosedValues: map[string]string{
						"TAXA":   "Corn",
						"METHOD": "Rotary",
					},
				},
			},
			measurement: mg.StreamingMeasurement[float64]{
				Identity: mg.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_101"},
				Primary:  mg.KeyValue[string, float64]{Key: "GRAIN_YIELD", Value: 185.4},
				Labels: []mg.KeyValue[string, string]{
					{Key: "TAXA", Value: "Corn"},
					{Key: "METHOD", Value: "Rotary"},
				},
			},
			wantFound:      true,
			wantInstanceID: "COMBINE_FULL_EXACT",
			wantClassID:    "CLASS_GRAIN_YIELD",
		},
		{
			name: "MUST return false when no rule matches primary code or required labels",
			rules: []mg.InstanceClassifier{
				{
					PrimaryCode:  "GRAIN_YIELD",
					ClassID:      "CLASS_GRAIN_YIELD",
					InstanceID:   "COMBINE_ROTARY_EXACT",
					ClosedValues: map[string]string{"METHOD": "Rotary"},
				},
			},
			measurement: mg.StreamingMeasurement[float64]{
				Identity: mg.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_101"},
				Primary:  mg.KeyValue[string, float64]{Key: "MOISTURE", Value: 14.2},
				Labels: []mg.KeyValue[string, string]{
					{Key: "METHOD", Value: "Rotary"},
				},
			},
			wantFound:      false,
			wantInstanceID: "",
			wantClassID:    "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			classifier := mg.NewClassifier()
			for _, r := range tc.rules {
				classifier.RegisterRule(r)
			}

			coord, found := classifier.Classify(tc.measurement)
			require.Equal(t, tc.wantFound, found)
			if tc.wantFound {
				require.NotNil(t, coord)
				require.Equal(t, tc.wantInstanceID, coord.InstanceID)
				require.Equal(t, tc.wantClassID, coord.ClassID)
			} else {
				require.Nil(t, coord)
			}
		})
	}
}

func TestReteConstraintEngine_Spec(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST enforce Alpha Negation (ForbiddenKeys) in classification", func(t *testing.T) {
		classifier := mg.NewClassifier()
		classifier.RegisterRule(mg.InstanceClassifier{
			PrimaryCode:   "BARN",
			ClassID:       "C10032",
			InstanceID:    "C10032-RAW",
			ForbiddenKeys: []string{"AGGT"},
			OpenKeys:      []string{"ANSM"},
		})

		// 1. Observation with forbidden AGGT MUST fail matching
		mWithAggt := mg.StreamingMeasurement[float64]{
			Identity: mg.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_1"},
			Primary:  mg.KeyValue[string, float64]{Key: "BARN", Value: 24.0},
			Labels: []mg.KeyValue[string, string]{
				{Key: "ANSM", Value: "User Collected"},
				{Key: "AGGT", Value: "882267e5-784a-459d"},
			},
		}
		coord, matched := classifier.Classify(mWithAggt)
		require.False(t, matched)
		require.Nil(t, coord)

		// 2. Observation without forbidden AGGT MUST match
		mWithoutAggt := mg.StreamingMeasurement[float64]{
			Identity: mg.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_1"},
			Primary:  mg.KeyValue[string, float64]{Key: "BARN", Value: 24.0},
			Labels: []mg.KeyValue[string, string]{
				{Key: "ANSM", Value: "User Collected"},
			},
		}
		coord, matched = classifier.Classify(mWithoutAggt)
		require.True(t, matched)
		require.NotNil(t, coord)
		require.Equal(t, "C10032-RAW", coord.InstanceID)
	})

	t.Run("MUST enforce Beta Negation (ForbiddenKeys) in calculation slot matching", func(t *testing.T) {
		engine := mg.NewEngine(storage.NewPrefixTrieGraphStore())

		engine.RegisterClassifierRule(mg.InstanceClassifier{
			PrimaryCode: "BARN",
			ClassID:     "C10032",
			InstanceID:  "C10032-I1",
			OpenKeys:    []string{"ANSM"},
		})
		engine.RegisterClassifierRule(mg.InstanceClassifier{
			PrimaryCode: "SC",
			ClassID:     "C5343",
			InstanceID:  "C5343-I1",
		})

		// Calculation forbids AGGT on BARN slot
		var calculationFired bool
		engine.RegisterCalculation(mg.StatefulCalculation{
			CalculationCode: "BARP_NO_AGGT",
			OutputMetric:    "BARP",
			RequiredSlots: []mg.ClassSelector{
				{
					SlotName:      "BARN",
					ClassID:       "C10032",
					ForbiddenKeys: []string{"AGGT"},
				},
				{
					SlotName: "SC",
					ClassID:  "C5343",
				},
			},
			Compute: func(ctx context.Context, entity mg.EntityIdentity, inputs map[string]mg.StoredMeasurement) (mg.StreamingMeasurement[float64], error) {
				calculationFired = true
				return mg.StreamingMeasurement[float64]{
					Identity: entity,
					Primary:  mg.KeyValue[string, float64]{Key: "BARP", Value: 10.0},
				}, nil
			},
		})

		entity := mg.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_neg_01"}

		// 1. Ingest SC
		_, err := engine.ClassifyAndPlace(ctx, mg.StreamingMeasurement[float64]{
			Identity: entity,
			Primary:  mg.KeyValue[string, float64]{Key: "SC", Value: 400.0},
		})
		require.NoError(t, err)

		// 2. Ingest BARN carrying AGGT -> Calculation MUST NOT fire
		_, err = engine.ClassifyAndPlace(ctx, mg.StreamingMeasurement[float64]{
			Identity: entity,
			Primary:  mg.KeyValue[string, float64]{Key: "BARN", Value: 24.0},
			Labels: []mg.KeyValue[string, string]{
				{Key: "ANSM", Value: "User Collected"},
				{Key: "AGGT", Value: "882267e5-784a-459d"},
			},
		})
		require.NoError(t, err)
		require.False(t, calculationFired, "Calculation MUST NOT fire when slot contains forbidden key AGGT")

		// 3. Ingest BARN without AGGT -> Calculation MUST fire
		_, err = engine.ClassifyAndPlace(ctx, mg.StreamingMeasurement[float64]{
			Identity: entity,
			Primary:  mg.KeyValue[string, float64]{Key: "BARN", Value: 24.0},
			Labels: []mg.KeyValue[string, string]{
				{Key: "ANSM", Value: "User Collected"},
			},
		})
		require.NoError(t, err)
		require.True(t, calculationFired, "Calculation MUST fire when candidate without forbidden key is placed")
	})

	t.Run("MUST unify dynamic variables (?var / @) across slots and evaluate valid permutations", func(t *testing.T) {
		engine := mg.NewEngine(storage.NewPrefixTrieGraphStore())

		engine.RegisterClassifierRule(mg.InstanceClassifier{
			PrimaryCode:  "SC",
			ClassID:      "C5343",
			InstanceID:   "C5343-EARLY",
			ClosedValues: map[string]string{"STAGE": "Early"},
		})
		engine.RegisterClassifierRule(mg.InstanceClassifier{
			PrimaryCode:  "SC",
			ClassID:      "C5343",
			InstanceID:   "C5343-LATE",
			ClosedValues: map[string]string{"STAGE": "Late"},
		})
		engine.RegisterClassifierRule(mg.InstanceClassifier{
			PrimaryCode:  "RTLC",
			ClassID:      "C6964",
			InstanceID:   "C6964-EARLY",
			ClosedValues: map[string]string{"STAGE": "Early"},
		})
		engine.RegisterClassifierRule(mg.InstanceClassifier{
			PrimaryCode:  "RTLC",
			ClassID:      "C6964",
			InstanceID:   "C6964-LATE",
			ClosedValues: map[string]string{"STAGE": "Late"},
		})

		var executedStages []string
		engine.RegisterCalculation(mg.StatefulCalculation{
			CalculationCode: "RTLP_DYNAMIC_STAGE",
			OutputMetric:    "RTLP",
			RequiredSlots: []mg.ClassSelector{
				{
					SlotName: "SC",
					ClassID:  "C5343",
					Constraints: map[string]string{
						"STAGE": "?stage",
					},
				},
				{
					SlotName: "RTLC",
					ClassID:  "C6964",
					Constraints: map[string]string{
						"STAGE": "?stage",
					},
				},
			},
			Compute: func(ctx context.Context, entity mg.EntityIdentity, inputs map[string]mg.StoredMeasurement) (mg.StreamingMeasurement[float64], error) {
				sc := inputs["SC"]
				rtlc := inputs["RTLC"]
				require.Equal(t, sc.Labels["STAGE"], rtlc.Labels["STAGE"], "Slots MUST share identical unified STAGE value")
				executedStages = append(executedStages, sc.Labels["STAGE"])

				return mg.StreamingMeasurement[float64]{
					Identity: entity,
					Primary:  mg.KeyValue[string, float64]{Key: "RTLP", Value: (rtlc.Value / sc.Value) * 100.0},
					Labels:   []mg.KeyValue[string, string]{{Key: "STAGE", Value: sc.Labels["STAGE"]}},
				}, nil
			},
		})

		entity := mg.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_unify_01"}

		// Place SC(STAGE=Early) and RTLC(STAGE=Late) -> Different stages, MUST NOT join
		_, _ = engine.ClassifyAndPlace(ctx, mg.StreamingMeasurement[float64]{
			Identity: entity,
			Primary:  mg.KeyValue[string, float64]{Key: "SC", Value: 400.0},
			Labels:   []mg.KeyValue[string, string]{{Key: "STAGE", Value: "Early"}},
		})
		_, _ = engine.ClassifyAndPlace(ctx, mg.StreamingMeasurement[float64]{
			Identity: entity,
			Primary:  mg.KeyValue[string, float64]{Key: "RTLC", Value: 20.0},
			Labels:   []mg.KeyValue[string, string]{{Key: "STAGE", Value: "Late"}},
		})
		require.Empty(t, executedStages, "Mismatched dynamic stages (Early vs Late) MUST NOT unify")

		// Place RTLC(STAGE=Early) -> Unifies with SC(Early) -> Executes for Early!
		_, _ = engine.ClassifyAndPlace(ctx, mg.StreamingMeasurement[float64]{
			Identity: entity,
			Primary:  mg.KeyValue[string, float64]{Key: "RTLC", Value: 40.0},
			Labels:   []mg.KeyValue[string, string]{{Key: "STAGE", Value: "Early"}},
		})
		require.Contains(t, executedStages, "Early")

		// Place SC(STAGE=Late) -> Unifies with RTLC(Late) -> Executes for Late!
		_, _ = engine.ClassifyAndPlace(ctx, mg.StreamingMeasurement[float64]{
			Identity: entity,
			Primary:  mg.KeyValue[string, float64]{Key: "SC", Value: 380.0},
			Labels:   []mg.KeyValue[string, string]{{Key: "STAGE", Value: "Late"}},
		})
		require.Contains(t, executedStages, "Late")
		require.Len(t, executedStages, 2)
	})

	t.Run("MUST declaratively bind slots to Secondary numeric values (SecondaryKey)", func(t *testing.T) {
		engine := mg.NewEngine(storage.NewPrefixTrieGraphStore())

		engine.RegisterClassifierRule(mg.InstanceClassifier{
			PrimaryCode: "SC",
			ClassID:     "C5343",
			InstanceID:  "C5343-I1",
		})
		engine.RegisterClassifierRule(mg.InstanceClassifier{
			PrimaryCode: "BARN",
			ClassID:     "C10032",
			InstanceID:  "C10032-I1",
		})

		var boundSecondaryValue float64
		engine.RegisterCalculation(mg.StatefulCalculation{
			CalculationCode: "BARP_SUBAREA_EXTRACTION",
			OutputMetric:    "BARP",
			RequiredSlots: []mg.ClassSelector{
				{
					SlotName:     "BARN_SUB",
					ClassID:      "C10032",
					SecondaryKey: "SUBAREA", // Extracts from Secondaries["SUBAREA"]
				},
				{
					SlotName: "SC_PRIMARY",
					ClassID:  "C5343", // Reads standard Value (Primary)
				},
			},
			Compute: func(ctx context.Context, entity mg.EntityIdentity, inputs map[string]mg.StoredMeasurement) (mg.StreamingMeasurement[float64], error) {
				barnObs := inputs["BARN_SUB"]
				scObs := inputs["SC_PRIMARY"]

				// Verify ValueForObservation helper
				subVal, ok := mg.ClassSelector{SecondaryKey: "SUBAREA"}.ValueForObservation(barnObs)
				require.True(t, ok)
				boundSecondaryValue = subVal

				return mg.StreamingMeasurement[float64]{
					Identity: entity,
					Primary:  mg.KeyValue[string, float64]{Key: "BARP", Value: (subVal / scObs.Value) * 100.0},
				}, nil
			},
		})

		entity := mg.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_sec_01"}

		// Place SC Primary = 400.0
		_, _ = engine.ClassifyAndPlace(ctx, mg.StreamingMeasurement[float64]{
			Identity: entity,
			Primary:  mg.KeyValue[string, float64]{Key: "SC", Value: 400.0},
		})

		// Place BARN Primary = 999.0, Secondary SUBAREA = 40.0
		res, err := engine.ClassifyAndPlace(ctx, mg.StreamingMeasurement[float64]{
			Identity: entity,
			Primary:  mg.KeyValue[string, float64]{Key: "BARN", Value: 999.0},
			Secondaries: []mg.KeyValue[string, float64]{
				{Key: "SUBAREA", Value: 40.0},
			},
		})
		require.NoError(t, err)
		require.Equal(t, 40.0, boundSecondaryValue)
		require.Len(t, res.Triggered, 1)
		require.InDelta(t, 10.0, res.Triggered[0].Primary.Value, 1e-6) // 40.0 / 400.0 * 100 = 10.0%
	})
}

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
