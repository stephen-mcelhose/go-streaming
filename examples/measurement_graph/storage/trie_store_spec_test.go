package storage_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming/examples/measurement_graph/storage"
)

func TestPrefixTrie_ShardingConfig_Spec(t *testing.T) {
	ctx := context.Background()

	type testCase struct {
		name        string
		inputShards int
		wantShards  int
	}

	testCases := []testCase{
		{
			name:        "MUST default to 256 shards when using NewPrefixTrieGraphStore",
			inputShards: 0,
			wantShards:  storage.DefaultTrieShards,
		},
		{
			name:        "MUST round non-power-of-2 up to next power of 2",
			inputShards: 50,
			wantShards:  64,
		},
		{
			name:        "MUST fallback to 1 shard when requested shards is 1 or negative",
			inputShards: 1,
			wantShards:  1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var store *storage.PrefixTrieGraphStore
			if tc.inputShards == 0 {
				store = storage.NewPrefixTrieGraphStore()
			} else {
				store = storage.NewPrefixTrieGraphStoreWithShards(tc.inputShards)
			}
			defer store.Close()

			require.Equal(t, tc.wantShards, store.ShardCount())

			// Verify basic operation on configured store
			entity := storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_cfg_1"}
			node := storage.NodeCoordinate{Entity: entity, PrimaryCode: "GRAIN_YIELD", ClassID: "CLASS_YIELD", InstanceID: "I1"}
			err := store.Put(ctx, storage.StoredMeasurement{Node: node, Value: 100.0, Timestamp: time.Now()})
			require.NoError(t, err)

			got, found, err := store.Get(ctx, node)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, 100.0, got.Value)
		})
	}
}

func TestPrefixTrie_ConcurrentDisjointEntities_Spec(t *testing.T) {
	ctx := context.Background()
	store := storage.NewPrefixTrieGraphStoreWithShards(256)
	defer store.Close()

	const numGoroutines = 64
	const writesPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	startGate := make(chan struct{})

	for g := 0; g < numGoroutines; g++ {
		go func(routineID int) {
			defer wg.Done()
			<-startGate

			entity := storage.EntityIdentity{
				BusinessUnit: "Agronomy",
				EntityType:   "PLOT",
				EntityID:     fmt.Sprintf("plot_concurrent_%d", routineID),
			}

			for i := 0; i < writesPerGoroutine; i++ {
				node := storage.NodeCoordinate{
					Entity:      entity,
					PrimaryCode: fmt.Sprintf("METRIC_%d", i),
					ClassID:     "CLASS_M",
					InstanceID:  "INST_1",
				}
				err := store.Put(ctx, storage.StoredMeasurement{
					Node:      node,
					Value:     float64(i),
					Timestamp: time.Now(),
				})
				if err != nil {
					t.Errorf("routine %d write %d failed: %v", routineID, i, err)
					return
				}
			}
		}(g)
	}

	close(startGate)
	wg.Wait()

	count, err := store.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, numGoroutines*writesPerGoroutine, count)
}

func TestPrefixTrie_StripedInvalidation_Spec(t *testing.T) {
	ctx := context.Background()
	store := storage.NewPrefixTrieGraphStoreWithShards(256)
	defer store.Close()

	entityA := storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_worker_A"}
	entityB := storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_victim_B"}

	// Populate Entity B
	for i := 0; i < 50; i++ {
		nodeB := storage.NodeCoordinate{
			Entity:      entityB,
			PrimaryCode: fmt.Sprintf("METRIC_B_%d", i),
			ClassID:     "CLASS_B",
			InstanceID:  "I1",
		}
		err := store.Put(ctx, storage.StoredMeasurement{Node: nodeB, Value: float64(i), Timestamp: time.Now()})
		require.NoError(t, err)
	}

	stopWritingA := make(chan struct{})
	var aWriteCount atomic.Int64
	var wg sync.WaitGroup

	// Concurrently write to Entity A in tight loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		var i int
		for {
			select {
			case <-stopWritingA:
				return
			default:
				nodeA := storage.NodeCoordinate{
					Entity:      entityA,
					PrimaryCode: fmt.Sprintf("METRIC_A_%d", i%20),
					ClassID:     "CLASS_A",
					InstanceID:  "I1",
				}
				if err := store.Put(ctx, storage.StoredMeasurement{Node: nodeA, Value: float64(i), Timestamp: time.Now()}); err == nil {
					aWriteCount.Add(1)
				}
				i++
			}
		}
	}()

	// Invalidate Entity B while Entity A is aggressively writing
	time.Sleep(10 * time.Millisecond)
	err := store.InvalidateEntity(ctx, entityB)
	require.NoError(t, err)

	close(stopWritingA)
	wg.Wait()

	// Entity B must be completely gone
	bObs, err := store.GetByEntity(ctx, entityB)
	require.NoError(t, err)
	require.Empty(t, bObs)

	// Entity A must be healthy and readable
	aObs, err := store.GetByEntity(ctx, entityA)
	require.NoError(t, err)
	require.NotEmpty(t, aObs)
}

func TestPrefixTrie_GlobalCount_Spec(t *testing.T) {
	ctx := context.Background()
	store := storage.NewPrefixTrieGraphStoreWithShards(64)
	defer store.Close()

	const entities = 20
	const metricsPerEntity = 10

	// 1. Concurrent inserts across entities
	var wg sync.WaitGroup
	for e := 0; e < entities; e++ {
		wg.Add(1)
		go func(entityIdx int) {
			defer wg.Done()
			entity := storage.EntityIdentity{
				BusinessUnit: "Agronomy",
				EntityType:   "PLOT",
				EntityID:     fmt.Sprintf("plot_cnt_%d", entityIdx),
			}
			for m := 0; m < metricsPerEntity; m++ {
				node := storage.NodeCoordinate{
					Entity:      entity,
					PrimaryCode: fmt.Sprintf("M_%d", m),
					ClassID:     "CLASS_M",
					InstanceID:  "I1",
				}
				_ = store.Put(ctx, storage.StoredMeasurement{Node: node, Value: 1.0, Timestamp: time.Now()})
			}
		}(e)
	}
	wg.Wait()

	count, err := store.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, entities*metricsPerEntity, count)

	// 2. Invalidate half of the entities concurrently
	for e := 0; e < entities/2; e++ {
		wg.Add(1)
		go func(entityIdx int) {
			defer wg.Done()
			entity := storage.EntityIdentity{
				BusinessUnit: "Agronomy",
				EntityType:   "PLOT",
				EntityID:     fmt.Sprintf("plot_cnt_%d", entityIdx),
			}
			_ = store.InvalidateEntity(ctx, entity)
		}(e)
	}
	wg.Wait()

	countAfter, err := store.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, (entities/2)*metricsPerEntity, countAfter)
}
