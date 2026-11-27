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

func TestClusterRouter_Spec(t *testing.T) {
	ctx := context.Background()

	// Helper to build a 4-node cluster with 16 total shards (4 shards/node)
	setupCluster := func(t *testing.T, totalShards, numNodes int) (*storage.ClusterRouter, []*storage.LocalClusterNode) {
		shardsPerNode := totalShards / numNodes
		var nodes []*storage.LocalClusterNode
		var clusterNodes []storage.ClusterNode

		for n := 0; n < numNodes; n++ {
			var assigned []int
			for s := 0; s < shardsPerNode; s++ {
				assigned = append(assigned, n*shardsPerNode+s)
			}
			node := storage.NewLocalClusterNode(fmt.Sprintf("node_%02d", n), totalShards, assigned)
			nodes = append(nodes, node)
			clusterNodes = append(clusterNodes, node)
		}

		router, err := storage.NewClusterRouter(totalShards, clusterNodes)
		require.NoError(t, err)
		return router, nodes
	}

	t.Run("MUST route entity observations strictly to owning cluster node in O(1) without touching other nodes", func(t *testing.T) {
		router, nodes := setupCluster(t, 16, 4)
		defer router.Close()

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

		err := router.Put(ctx, meas)
		require.NoError(t, err)

		// 1. Fetch from top-level router
		got, found, err := router.Get(ctx, meas.Node)
		require.NoError(t, err)
		require.True(t, found)
		require.InDelta(t, 185.4, got.Value, 1e-6)

		// 2. Verify that exactly ONE node holds the data, and other 3 nodes have count 0
		nodesWithData := 0
		for _, n := range nodes {
			c, err := n.Count(ctx)
			require.NoError(t, err)
			if c > 0 {
				nodesWithData++
				require.Equal(t, 1, c)
			}
		}
		require.Equal(t, 1, nodesWithData, "Observation MUST reside on exactly one owning node")
	})

	t.Run("MUST execute parallel scatter-gather fleet invalidations across all nodes", func(t *testing.T) {
		router, _ := setupCluster(t, 16, 4)
		defer router.Close()

		numEntities := 400
		for i := 0; i < numEntities; i++ {
			entityID := fmt.Sprintf("plot_%04d", i)
			// Target observation: COMBINE_1
			err := router.Put(ctx, storage.StoredMeasurement{
				Node: storage.NodeCoordinate{
					Entity:      storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: entityID},
					PrimaryCode: "GRAIN_YIELD",
					ClassID:     "CLASS_YIELD",
					InstanceID:  "COMBINE_1",
				},
				Value: 180.0,
			})
			require.NoError(t, err)

			// Non-target observation: COMBINE_2
			err = router.Put(ctx, storage.StoredMeasurement{
				Node: storage.NodeCoordinate{
					Entity:      storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: entityID},
					PrimaryCode: "GRAIN_YIELD",
					ClassID:     "CLASS_YIELD",
					InstanceID:  "COMBINE_2",
				},
				Value: 195.0,
			})
			require.NoError(t, err)
		}

		totalCount, err := router.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 800, totalCount)

		// Scatter-gather fleet invalidation
		err = router.InvalidatePrefix(ctx, "GRAIN_YIELD", "CLASS_YIELD", "COMBINE_1")
		require.NoError(t, err)

		// Verify target is pruned cluster-wide
		for i := 0; i < 50; i++ {
			entityID := fmt.Sprintf("plot_%04d", i)
			_, found, err := router.Get(ctx, storage.NodeCoordinate{
				Entity:      storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: entityID},
				PrimaryCode: "GRAIN_YIELD",
				ClassID:     "CLASS_YIELD",
				InstanceID:  "COMBINE_1",
			})
			require.NoError(t, err)
			require.False(t, found)
		}

		// Verify non-target is intact
		finalCount, err := router.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 400, finalCount)
	})

	t.Run("MUST invalidate entity strictly within owning node", func(t *testing.T) {
		router, _ := setupCluster(t, 16, 4)
		defer router.Close()

		entity1 := storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_A"}
		entity2 := storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_B"}

		_ = router.Put(ctx, storage.StoredMeasurement{
			Node: storage.NodeCoordinate{
				Entity:      entity1,
				PrimaryCode: "YIELD",
				ClassID:     "CLASS_YIELD",
				InstanceID:  "INST_1",
			},
			Value: 100.0,
		})
		_ = router.Put(ctx, storage.StoredMeasurement{
			Node: storage.NodeCoordinate{
				Entity:      entity2,
				PrimaryCode: "YIELD",
				ClassID:     "CLASS_YIELD",
				InstanceID:  "INST_1",
			},
			Value: 200.0,
		})

		err := router.InvalidateEntity(ctx, entity1)
		require.NoError(t, err)

		items1, err := router.GetByEntity(ctx, entity1)
		require.NoError(t, err)
		require.Empty(t, items1)

		items2, err := router.GetByEntity(ctx, entity2)
		require.NoError(t, err)
		require.Len(t, items2, 1)
	})

	t.Run("MUST support multi-node concurrent ingestion and fleet pruning under race detector", func(t *testing.T) {
		router, _ := setupCluster(t, 32, 8)
		defer router.Close()

		numWorkers := 24
		opsPerWorker := 100

		var wg sync.WaitGroup
		wg.Add(numWorkers)

		for w := 0; w < numWorkers; w++ {
			workerID := w
			go func() {
				defer wg.Done()
				for op := 0; op < opsPerWorker; op++ {
					entityID := fmt.Sprintf("plot_%03d", (workerID*13+op)%60)
					entity := storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: entityID}

					_ = router.Put(ctx, storage.StoredMeasurement{
						Node: storage.NodeCoordinate{
							Entity:      entity,
							PrimaryCode: "METRIC_A",
							ClassID:     "CLASS_A",
							InstanceID:  fmt.Sprintf("INST_%d", workerID%4),
						},
						Value: float64(op),
					})

					_, _ = router.GetByEntity(ctx, entity)

					if op%40 == 0 {
						_ = router.InvalidateEntity(ctx, entity)
					}
					if op%80 == 0 {
						_ = router.InvalidatePrefix(ctx, "METRIC_A", "CLASS_A", fmt.Sprintf("INST_%d", workerID%4))
					}
				}
			}()
		}

		wg.Wait()
	})
}
