//go:build relate
// +build relate

package measurement_graph_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	mg "github.com/stephen-mcelhose/go-streaming/examples/measurement_graph"
	"github.com/stephen-mcelhose/go-streaming/examples/measurement_graph/storage"
)

func TestMeasurementGraphWithLiveRelateDB(t *testing.T) {
	ctx := context.Background()

	// Instantiate live C-FFI RelateDB embedded graph store
	relateStore, err := storage.NewNativeRelateDBGraphStore(":memory:")
	require.NoError(t, err)
	defer relateStore.Close()

	engine := mg.NewEngine(relateStore)

	// Register classification rules into Primary:Class:Instance hierarchy
	engine.RegisterClassifierRule(mg.InstanceClassifier{
		PrimaryCode: "GRAIN_YIELD",
		ClassID:     "CLASS_GRAIN_YIELD",
		InstanceID:  "COMBINE_ROTARY_4000",
		ClosedValues: map[string]string{
			"TAXA": "Corn",
		},
	})
	engine.RegisterClassifierRule(mg.InstanceClassifier{
		PrimaryCode: "MOISTURE",
		ClassID:     "CLASS_MOISTURE",
		InstanceID:  "MOISTURE_PROBE_NIR",
		ClosedValues: map[string]string{
			"TAXA": "Corn",
		},
	})

	// Register stateful multi-variable calculation: Depends on ClassSelectors, NOT instance IDs
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
			}, nil
		},
	})

	entity := mg.EntityIdentity{
		BusinessUnit: "Agronomy",
		EntityType:   "PLOT",
		EntityID:     "plot_live_relate_888",
	}

	producerCh := make(chan mg.StreamingMeasurement[float64], 2)
	producerCh <- mg.StreamingMeasurement[float64]{
		Identity:  entity,
		Primary:   mg.KeyValue[string, float64]{Key: "GRAIN_YIELD", Value: 185.4},
		Labels:    []mg.KeyValue[string, string]{{Key: "TAXA", Value: "Corn"}},
		Timestamp: time.Now(),
	}
	producerCh <- mg.StreamingMeasurement[float64]{
		Identity:  entity,
		Primary:   mg.KeyValue[string, float64]{Key: "MOISTURE", Value: 17.8},
		Labels:    []mg.KeyValue[string, string]{{Key: "TAXA", Value: "Corn"}},
		Timestamp: time.Now(),
	}
	close(producerCh)

	outCalcCh := make(chan mg.StreamingMeasurement[float64], 5)

	err = mg.Router(ctx, engine,
		streaming.NewChanStream(producerCh, nil),
		streaming.NewChanStream(nil, outCalcCh),
		nil,
	)
	require.NoError(t, err)
	close(outCalcCh)

	var results []mg.StreamingMeasurement[float64]
	for res := range outCalcCh {
		results = append(results, res)
	}

	// Assertions
	require.Len(t, results, 1)
	require.Equal(t, "GRAIN_YIELD_STD15", results[0].Primary.Key)
	require.InDelta(t, 180.3536, results[0].Primary.Value, 1e-3)

	// Verify graph persistence inside RelateDB
	count, err := relateStore.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	entityObservations, err := relateStore.GetByEntity(ctx, entity)
	require.NoError(t, err)
	require.Len(t, entityObservations, 2)
}
