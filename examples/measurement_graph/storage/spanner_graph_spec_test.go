package storage_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming/examples/measurement_graph/storage"
)

func TestSpannerGraphStore_Spec(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST insert and retrieve observation nodes via GQL ontology graph", func(t *testing.T) {
		store := storage.NewSpannerGraphStore(nil)
		defer store.Close()

		meas := storage.StoredMeasurement{
			Node: storage.NodeCoordinate{
				Entity:      storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_101"},
				PrimaryCode: "GRAIN_YIELD",
				ClassID:     "CLASS_GRAIN_YIELD",
				InstanceID:  "COMBINE_ROTARY",
			},
			Value:     185.4,
			Timestamp: time.Now(),
		}

		err := store.Put(ctx, meas)
		require.NoError(t, err)

		// 1. Get exact observation
		got, found, err := store.Get(ctx, meas.Node)
		require.NoError(t, err)
		require.True(t, found)
		require.InDelta(t, 185.4, got.Value, 1e-6)
		require.Equal(t, "plot_101", got.Node.Entity.EntityID)
		require.Equal(t, "COMBINE_ROTARY", got.Node.InstanceID)

		count, err := store.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("MUST retrieve observations by Entity and Class ontology traversal", func(t *testing.T) {
		store := storage.NewSpannerGraphStore(nil)
		defer store.Close()

		entity := storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_202"}

		// Yield observation
		err := store.Put(ctx, storage.StoredMeasurement{
			Node: storage.NodeCoordinate{
				Entity:      entity,
				PrimaryCode: "GRAIN_YIELD",
				ClassID:     "CLASS_GRAIN_YIELD",
				InstanceID:  "COMBINE_1",
			},
			Value:     190.0,
			Timestamp: time.Now(),
		})
		require.NoError(t, err)

		// Moisture observation
		err = store.Put(ctx, storage.StoredMeasurement{
			Node: storage.NodeCoordinate{
				Entity:      entity,
				PrimaryCode: "MOISTURE",
				ClassID:     "CLASS_MOISTURE",
				InstanceID:  "MOISTURE_PROBE_1",
			},
			Value:     14.5,
			Timestamp: time.Now(),
		})
		require.NoError(t, err)

		// 1. GetByEntity returns both observations
		byEntity, err := store.GetByEntity(ctx, entity)
		require.NoError(t, err)
		require.Len(t, byEntity, 2)

		// 2. GetByClass returns only matching class
		byYieldClass, err := store.GetByClass(ctx, entity, "CLASS_GRAIN_YIELD")
		require.NoError(t, err)
		require.Len(t, byYieldClass, 1)
		require.InDelta(t, 190.0, byYieldClass[0].Value, 1e-6)

		byMoistureClass, err := store.GetByClass(ctx, entity, "CLASS_MOISTURE")
		require.NoError(t, err)
		require.Len(t, byMoistureClass, 1)
		require.InDelta(t, 14.5, byMoistureClass[0].Value, 1e-6)
	})

	t.Run("MUST execute declarative fleet invalidation (DETACH DELETE) across all entities", func(t *testing.T) {
		store := storage.NewSpannerGraphStore(nil)
		defer store.Close()

		numEntities := 100
		for i := 0; i < numEntities; i++ {
			entity := storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: fmt.Sprintf("plot_%03d", i)}

			// Target sensor: COMBINE_FAULTY
			_ = store.Put(ctx, storage.StoredMeasurement{
				Node: storage.NodeCoordinate{
					Entity:      entity,
					PrimaryCode: "GRAIN_YIELD",
					ClassID:     "CLASS_YIELD",
					InstanceID:  "COMBINE_FAULTY",
				},
				Value: 180.0,
			})

			// Healthy sensor: COMBINE_HEALTHY
			_ = store.Put(ctx, storage.StoredMeasurement{
				Node: storage.NodeCoordinate{
					Entity:      entity,
					PrimaryCode: "GRAIN_YIELD",
					ClassID:     "CLASS_YIELD",
					InstanceID:  "COMBINE_HEALTHY",
				},
				Value: 195.0,
			})
		}

		totalCount, err := store.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 200, totalCount)

		// Declarative fleet revocation in Spanner Graph
		deletedCount, err := store.InvalidateFleet(ctx, "COMBINE_FAULTY")
		require.NoError(t, err)
		require.Equal(t, int64(100), deletedCount)

		finalCount, err := store.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 100, finalCount)

		// Verify COMBINE_HEALTHY remains
		allRemaining, err := store.All(ctx)
		require.NoError(t, err)
		require.Len(t, allRemaining, 100)
		for _, obs := range allRemaining {
			require.Equal(t, "COMBINE_HEALTHY", obs.Node.InstanceID)
		}
	})

	t.Run("MUST invalidate entity subtree in O(1) declarative GQL statement", func(t *testing.T) {
		store := storage.NewSpannerGraphStore(nil)
		defer store.Close()

		entity1 := storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_A"}
		entity2 := storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_B"}

		_ = store.Put(ctx, storage.StoredMeasurement{
			Node:  storage.NodeCoordinate{Entity: entity1, PrimaryCode: "YIELD", ClassID: "CLASS_YIELD", InstanceID: "I1"},
			Value: 100.0,
		})
		_ = store.Put(ctx, storage.StoredMeasurement{
			Node:  storage.NodeCoordinate{Entity: entity1, PrimaryCode: "MOISTURE", ClassID: "CLASS_MOISTURE", InstanceID: "I1"},
			Value: 15.0,
		})
		_ = store.Put(ctx, storage.StoredMeasurement{
			Node:  storage.NodeCoordinate{Entity: entity2, PrimaryCode: "YIELD", ClassID: "CLASS_YIELD", InstanceID: "I1"},
			Value: 200.0,
		})

		count, _ := store.Count(ctx)
		require.Equal(t, 3, count)

		// Invalidate entity 1
		err := store.InvalidateEntity(ctx, entity1)
		require.NoError(t, err)

		items1, _ := store.GetByEntity(ctx, entity1)
		require.Empty(t, items1)

		items2, _ := store.GetByEntity(ctx, entity2)
		require.Len(t, items2, 1)

		count, _ = store.Count(ctx)
		require.Equal(t, 1, count)
	})
}
