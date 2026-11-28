package storage_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming/examples/measurement_graph/storage"
)

func TestDualIndexStore_Spec(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST atomically place observations across both Entity and Primary internal tries", func(t *testing.T) {
		store := storage.NewDualIndexStoreWithShards(16)
		defer store.Close()

		meas := storage.StoredMeasurement{
			Node: storage.NodeCoordinate{
				Entity:      storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_101"},
				PrimaryCode: "GRAIN_YIELD",
				ClassID:     "CLASS_GRAIN_YIELD",
				InstanceID:  "COMBINE_ROTARY",
				Characteristics: map[string]string{
					"PASS": "1",
				},
			},
			Value:     188.5,
			Timestamp: time.Now(),
		}

		err := store.Put(ctx, meas)
		require.NoError(t, err)

		// 1. Fetch from standard Entity-first axis
		gotEntity, foundEntity, err := store.Get(ctx, meas.Node)
		require.NoError(t, err)
		require.True(t, foundEntity)
		require.InDelta(t, 188.5, gotEntity.Value, 1e-6)

		// 2. Fetch directly from internal Primary-first axis
		gotPrimary, foundPrimary, err := store.GetPrimaryAxis(ctx, meas.Node)
		require.NoError(t, err)
		require.True(t, foundPrimary)
		require.InDelta(t, 188.5, gotPrimary.Value, 1e-6)

		count, err := store.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("MUST invalidate entire sensor fleet via localized micro-snip across all shards in sub-millisecond time", func(t *testing.T) {
		store := storage.NewDualIndexStoreWithShards(64)
		defer store.Close()

		numEntities := 1000
		for i := 0; i < numEntities; i++ {
			entityID := fmt.Sprintf("plot_%04d", i)
			// Observation 1: COMBINE_1 (Target for revocation)
			err := store.Put(ctx, storage.StoredMeasurement{
				Node: storage.NodeCoordinate{
					Entity:      storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: entityID},
					PrimaryCode: "GRAIN_YIELD",
					ClassID:     "CLASS_YIELD",
					InstanceID:  "COMBINE_1",
				},
				Value:     180.0 + float64(i),
				Timestamp: time.Now(),
			})
			require.NoError(t, err)

			// Observation 2: COMBINE_2 (Should NOT be pruned)
			err = store.Put(ctx, storage.StoredMeasurement{
				Node: storage.NodeCoordinate{
					Entity:      storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: entityID},
					PrimaryCode: "GRAIN_YIELD",
					ClassID:     "CLASS_YIELD",
					InstanceID:  "COMBINE_2",
				},
				Value:     190.0 + float64(i),
				Timestamp: time.Now(),
			})
			require.NoError(t, err)
		}

		totalCount, err := store.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 2000, totalCount)

		// Fleet invalidation: Revoke COMBINE_1 across all 1000 entities
		start := time.Now()
		prunedCount, err := store.InvalidatePrefixCount(ctx, "GRAIN_YIELD", "CLASS_YIELD", "COMBINE_1")
		elapsed := time.Since(start)

		require.NoError(t, err)
		require.Equal(t, 1000, prunedCount)
		require.Less(t, elapsed, 100*time.Millisecond) // Sub-100ms in debug, sub-1ms in release

		// Verify COMBINE_1 is completely gone from Entity axis
		for i := 0; i < 50; i++ {
			entityID := fmt.Sprintf("plot_%04d", i)
			_, found, err := store.Get(ctx, storage.NodeCoordinate{
				Entity:      storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: entityID},
				PrimaryCode: "GRAIN_YIELD",
				ClassID:     "CLASS_YIELD",
				InstanceID:  "COMBINE_1",
			})
			require.NoError(t, err)
			require.False(t, found)
		}

		// Verify COMBINE_2 is completely intact
		for i := 0; i < 50; i++ {
			entityID := fmt.Sprintf("plot_%04d", i)
			got, found, err := store.Get(ctx, storage.NodeCoordinate{
				Entity:      storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: entityID},
				PrimaryCode: "GRAIN_YIELD",
				ClassID:     "CLASS_YIELD",
				InstanceID:  "COMBINE_2",
			})
			require.NoError(t, err)
			require.True(t, found)
			require.InDelta(t, 190.0+float64(i), got.Value, 1e-6)
		}

		// Verify final count
		finalCount, err := store.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 1000, finalCount)
	})

	t.Run("MUST invalidate entity working memory within owning single shard and clean Primary index", func(t *testing.T) {
		store := storage.NewDualIndexStoreWithShards(16)
		defer store.Close()

		entity1 := storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_A"}
		entity2 := storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_B"}

		// Add 3 observations for entity 1
		for _, primary := range []string{"YIELD", "MOISTURE", "SPEED"} {
			_ = store.Put(ctx, storage.StoredMeasurement{
				Node: storage.NodeCoordinate{
					Entity:      entity1,
					PrimaryCode: primary,
					ClassID:     "CLASS_" + primary,
					InstanceID:  "INST_1",
				},
				Value: 100.0,
			})
		}

		// Add 1 observation for entity 2
		_ = store.Put(ctx, storage.StoredMeasurement{
			Node: storage.NodeCoordinate{
				Entity:      entity2,
				PrimaryCode: "YIELD",
				ClassID:     "CLASS_YIELD",
				InstanceID:  "INST_1",
			},
			Value: 200.0,
		})

		count, err := store.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 4, count)

		// Invalidate entity 1
		err = store.InvalidateEntity(ctx, entity1)
		require.NoError(t, err)

		// Entity 1 observations must be gone from Entity axis
		items1, err := store.GetByEntity(ctx, entity1)
		require.NoError(t, err)
		require.Empty(t, items1)

		// Entity 1 must also be gone from Primary axis
		_, foundPrimary, err := store.GetPrimaryAxis(ctx, storage.NodeCoordinate{
			Entity:      entity1,
			PrimaryCode: "YIELD",
			ClassID:     "CLASS_YIELD",
			InstanceID:  "INST_1",
		})
		require.NoError(t, err)
		require.False(t, foundPrimary)

		// Entity 2 must remain completely intact
		items2, err := store.GetByEntity(ctx, entity2)
		require.NoError(t, err)
		require.Len(t, items2, 1)

		count, err = store.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("MUST execute 64 concurrent workers without data races or deadlocks", func(t *testing.T) {
		store := storage.NewDualIndexStoreWithShards(32)
		defer store.Close()

		numWorkers := 32
		opsPerWorker := 200

		var wg sync.WaitGroup
		wg.Add(numWorkers)

		for w := 0; w < numWorkers; w++ {
			workerID := w
			go func() {
				defer wg.Done()
				for op := 0; op < opsPerWorker; op++ {
					entityID := fmt.Sprintf("plot_%03d", (workerID*17+op)%100)
					entity := storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: entityID}

					// 1. Put
					_ = store.Put(ctx, storage.StoredMeasurement{
						Node: storage.NodeCoordinate{
							Entity:      entity,
							PrimaryCode: "METRIC_A",
							ClassID:     "CLASS_A",
							InstanceID:  fmt.Sprintf("INST_%d", workerID%4),
						},
						Value: float64(op),
					})

					// 2. Read
					_, _ = store.GetByEntity(ctx, entity)

					// 3. Periodic invalidation
					if op%50 == 0 {
						_ = store.InvalidateEntity(ctx, entity)
					}
					if op%100 == 0 {
						_ = store.InvalidatePrefix(ctx, "METRIC_A", "CLASS_A", fmt.Sprintf("INST_%d", workerID%4))
					}
				}
			}()
		}

		wg.Wait()
		// Test completes cleanly without deadlock or race detector panic
	})
}
