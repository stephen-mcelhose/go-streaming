package storage_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming/examples/measurement_graph/storage"
)

func TestCoordinatePartitioners_Spec(t *testing.T) {
	entity := storage.EntityIdentity{
		BusinessUnit: "Agronomy",
		EntityType:   "PLOT",
		EntityID:     "plot_part_100",
	}

	node := storage.NodeCoordinate{
		Entity:          entity,
		PrimaryCode:     "GRAIN_YIELD",
		ClassID:         "CLASS_GRAIN_YIELD",
		InstanceID:      "COMBINE_ROTARY",
		Characteristics: map[string]string{"PASS": "1", "SECTION": "North"},
	}

	t.Run("EntityFirstPartitioner MUST order entity coordinates above metric and class", func(t *testing.T) {
		segs := storage.EntityFirstPartitioner(node)
		require.Equal(t, []string{
			"Agronomy",
			"PLOT",
			"plot_part_100",
			"GRAIN_YIELD",
			"CLASS_GRAIN_YIELD",
			"COMBINE_ROTARY",
			"PASS=1",
			"SECTION=North",
		}, segs)
	})

	t.Run("PrimaryFirstPartitioner MUST order metric and class coordinates above entity", func(t *testing.T) {
		segs := storage.PrimaryFirstPartitioner(node)
		require.Equal(t, []string{
			"GRAIN_YIELD",
			"CLASS_GRAIN_YIELD",
			"COMBINE_ROTARY",
			"Agronomy",
			"PLOT",
			"plot_part_100",
			"PASS=1",
			"SECTION=North",
		}, segs)
	})

	t.Run("ClassFirstPartitioner MUST order class and metric above instance and entity", func(t *testing.T) {
		segs := storage.ClassFirstPartitioner(node)
		require.Equal(t, []string{
			"CLASS_GRAIN_YIELD",
			"GRAIN_YIELD",
			"COMBINE_ROTARY",
			"Agronomy",
			"PLOT",
			"plot_part_100",
			"PASS=1",
			"SECTION=North",
		}, segs)
	})
}

func TestRekey_Spec(t *testing.T) {
	ctx := context.Background()

	// Seed source entity-first store with 5 entities across 2 primary metrics
	srcStore := storage.NewPrefixTrieGraphStore()
	defer srcStore.Close()

	for e := 1; e <= 5; e++ {
		entity := storage.EntityIdentity{
			BusinessUnit: "Agronomy",
			EntityType:   "PLOT",
			EntityID:     fmt.Sprintf("plot_%d", e),
		}

		// Yield
		_ = srcStore.Put(ctx, storage.StoredMeasurement{
			Node: storage.NodeCoordinate{
				Entity:      entity,
				PrimaryCode: "GRAIN_YIELD",
				ClassID:     "CLASS_GRAIN_YIELD",
				InstanceID:  "COMBINE_1",
			},
			Value:     150.0 + float64(e),
			Timestamp: time.Now(),
		})

		// Moisture
		_ = srcStore.Put(ctx, storage.StoredMeasurement{
			Node: storage.NodeCoordinate{
				Entity:      entity,
				PrimaryCode: "GRAIN_MOISTURE",
				ClassID:     "CLASS_GRAIN_MOISTURE",
				InstanceID:  "MOISTURE_PROBE_1",
			},
			Value:     14.0 + float64(e)*0.1,
			Timestamp: time.Now(),
		})
	}

	srcCount, err := srcStore.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 10, srcCount)

	t.Run("Rekey MUST project all measurements into repartitioned store", func(t *testing.T) {
		primaryStore, err := storage.Rekey(ctx, storage.PrimaryFirstPartitioner, srcStore)
		require.NoError(t, err)
		defer primaryStore.Close()

		rekeyedCount, err := primaryStore.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 10, rekeyedCount)

		// Point lookup MUST work identically under rekeyed coordinate paths
		targetNode := storage.NodeCoordinate{
			Entity: storage.EntityIdentity{
				BusinessUnit: "Agronomy",
				EntityType:   "PLOT",
				EntityID:     "plot_3",
			},
			PrimaryCode: "GRAIN_YIELD",
			ClassID:     "CLASS_GRAIN_YIELD",
			InstanceID:  "COMBINE_1",
		}

		got, found, err := primaryStore.Get(ctx, targetNode)
		require.NoError(t, err)
		require.True(t, found)
		require.InDelta(t, 153.0, got.Value, 1e-6)
	})

	t.Run("Rekey MUST be fully reversible across multiple repartitioning cycles (lossless isomorphism)", func(t *testing.T) {
		// Cycle 1: EntityFirst (srcStore) -> PrimaryFirst
		primaryStore, err := storage.Rekey(ctx, storage.PrimaryFirstPartitioner, srcStore)
		require.NoError(t, err)
		defer primaryStore.Close()

		// Cycle 2: PrimaryFirst -> ClassFirst
		classStore, err := storage.Rekey(ctx, storage.ClassFirstPartitioner, primaryStore)
		require.NoError(t, err)
		defer classStore.Close()

		// Cycle 3: ClassFirst -> EntityFirst (roundtrip back to original layout)
		restoredStore, err := storage.Rekey(ctx, storage.EntityFirstPartitioner, classStore)
		require.NoError(t, err)
		defer restoredStore.Close()

		// 1. Count must be preserved exactly
		restoredCount, err := restoredStore.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 10, restoredCount)

		// 2. All records must be bit-for-bit identical to srcStore
		srcAll, err := srcStore.All(ctx)
		require.NoError(t, err)

		for _, srcMeas := range srcAll {
			got, found, err := restoredStore.Get(ctx, srcMeas.Node)
			require.NoError(t, err)
			require.True(t, found, "restored store MUST contain coordinate %s", srcMeas.Node.String())
			require.InDelta(t, srcMeas.Value, got.Value, 1e-6)
			require.Equal(t, srcMeas.Node.Entity, got.Node.Entity)
			require.Equal(t, srcMeas.Node.PrimaryCode, got.Node.PrimaryCode)
			require.Equal(t, srcMeas.Node.ClassID, got.Node.ClassID)
			require.Equal(t, srcMeas.Node.InstanceID, got.Node.InstanceID)
		}
	})

	t.Run("InvalidatePrefix MUST prune entire metric fleet across all entities in O(1)", func(t *testing.T) {
		primaryStore, err := storage.Rekey(ctx, storage.PrimaryFirstPartitioner, srcStore)
		require.NoError(t, err)
		defer primaryStore.Close()

		// Invalidate all GRAIN_YIELD across ALL 5 entities in 1 operation
		err = primaryStore.InvalidatePrefix(ctx, "GRAIN_YIELD")
		require.NoError(t, err)

		// Remaining count must be 5 (all GRAIN_MOISTURE preserved)
		afterCount, err := primaryStore.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 5, afterCount)

		// Querying any plot for GRAIN_YIELD must return not found
		for e := 1; e <= 5; e++ {
			node := storage.NodeCoordinate{
				Entity: storage.EntityIdentity{
					BusinessUnit: "Agronomy",
					EntityType:   "PLOT",
					EntityID:     fmt.Sprintf("plot_%d", e),
				},
				PrimaryCode: "GRAIN_YIELD",
				ClassID:     "CLASS_GRAIN_YIELD",
				InstanceID:  "COMBINE_1",
			}
			_, found, err := primaryStore.Get(ctx, node)
			require.NoError(t, err)
			require.False(t, found, "plot_%d GRAIN_YIELD MUST be pruned", e)

			// GRAIN_MOISTURE must still exist
			moistureNode := storage.NodeCoordinate{
				Entity: storage.EntityIdentity{
					BusinessUnit: "Agronomy",
					EntityType:   "PLOT",
					EntityID:     fmt.Sprintf("plot_%d", e),
				},
				PrimaryCode: "GRAIN_MOISTURE",
				ClassID:     "CLASS_GRAIN_MOISTURE",
				InstanceID:  "MOISTURE_PROBE_1",
			}
			got, found, err := primaryStore.Get(ctx, moistureNode)
			require.NoError(t, err)
			require.True(t, found, "plot_%d GRAIN_MOISTURE MUST still exist", e)
			require.InDelta(t, 14.0+float64(e)*0.1, got.Value, 1e-6)
		}
	})

	t.Run("InvalidatePrefix MUST prune specific Class or Instance fleet in O(1)", func(t *testing.T) {
		primaryStore, err := storage.Rekey(ctx, storage.PrimaryFirstPartitioner, srcStore)
		require.NoError(t, err)
		defer primaryStore.Close()

		// Prune [Primary=GRAIN_YIELD, Class=CLASS_GRAIN_YIELD, Instance=COMBINE_1]
		err = primaryStore.InvalidatePrefix(ctx, "GRAIN_YIELD", "CLASS_GRAIN_YIELD", "COMBINE_1")
		require.NoError(t, err)

		afterCount, err := primaryStore.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 5, afterCount)
	})
}
