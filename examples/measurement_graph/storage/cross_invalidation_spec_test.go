package storage_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming/examples/measurement_graph/storage"
)

func TestPrefixTrie_DetachPrefix_Spec(t *testing.T) {
	ctx := context.Background()

	type testCase struct {
		name              string
		setupStore        func() *storage.PrefixTrieGraphStore
		prefix            []string
		wantDetachedCount int
		wantRemainCount   int
		wantErr           error
		assertSideEffects func(t *testing.T, source *storage.PrefixTrieGraphStore, detached *storage.PrefixTrieGraphStore)
	}

	testCases := []testCase{
		{
			name: "MUST return empty store and zero count when prefix is not found",
			setupStore: func() *storage.PrefixTrieGraphStore {
				store := storage.NewPrefixTrieGraphStore()
				node := storage.NodeCoordinate{
					Entity:      storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_1"},
					PrimaryCode: "GRAIN_YIELD",
					ClassID:     "CLASS_YIELD",
					InstanceID:  "I1",
				}
				_ = store.Put(ctx, storage.StoredMeasurement{Node: node, Value: 100.0, Timestamp: time.Now()})
				return store
			},
			prefix:            []string{"Cardiology"},
			wantDetachedCount: 0,
			wantRemainCount:   1,
			wantErr:           nil,
			assertSideEffects: func(t *testing.T, source *storage.PrefixTrieGraphStore, detached *storage.PrefixTrieGraphStore) {
				require.NotNil(t, detached)
				count, err := detached.Count(ctx)
				require.NoError(t, err)
				require.Equal(t, 0, count)
			},
		},
		{
			name: "MUST detach matching subtree in O(1) and return valid standalone PrefixTrieGraphStore",
			setupStore: func() *storage.PrefixTrieGraphStore {
				store := storage.NewPrefixTrieGraphStore()
				for i := 1; i <= 10; i++ {
					node := storage.NodeCoordinate{
						Entity:      storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: fmt.Sprintf("plot_%d", i)},
						PrimaryCode: "GRAIN_YIELD",
						ClassID:     "CLASS_YIELD",
						InstanceID:  "I1",
					}
					_ = store.Put(ctx, storage.StoredMeasurement{Node: node, Value: float64(i), Timestamp: time.Now()})
				}
				// Add an unrelated BU
				otherNode := storage.NodeCoordinate{
					Entity:      storage.EntityIdentity{BusinessUnit: "Cardiology", EntityType: "PATIENT", EntityID: "pat_1"},
					PrimaryCode: "HEART_RATE",
					ClassID:     "CLASS_HR",
					InstanceID:  "I2",
				}
				_ = store.Put(ctx, storage.StoredMeasurement{Node: otherNode, Value: 72.0, Timestamp: time.Now()})
				return store
			},
			prefix:            []string{"Agronomy"},
			wantDetachedCount: 10,
			wantRemainCount:   1,
			wantErr:           nil,
			assertSideEffects: func(t *testing.T, source *storage.PrefixTrieGraphStore, detached *storage.PrefixTrieGraphStore) {
				// 1. Source must not contain Agronomy plots anymore
				for i := 1; i <= 10; i++ {
					node := storage.NodeCoordinate{
						Entity:      storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: fmt.Sprintf("plot_%d", i)},
						PrimaryCode: "GRAIN_YIELD",
						ClassID:     "CLASS_YIELD",
						InstanceID:  "I1",
					}
					_, found, err := source.Get(ctx, node)
					require.NoError(t, err)
					require.False(t, found, "source MUST NOT contain detached node %s", node.String())
				}

				// 2. Detached mini-store must contain all 10 Agronomy plots
				for i := 1; i <= 10; i++ {
					node := storage.NodeCoordinate{
						Entity:      storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: fmt.Sprintf("plot_%d", i)},
						PrimaryCode: "GRAIN_YIELD",
						ClassID:     "CLASS_YIELD",
						InstanceID:  "I1",
					}
					got, found, err := detached.Get(ctx, node)
					require.NoError(t, err)
					require.True(t, found, "detached store MUST contain node %s", node.String())
					require.Equal(t, float64(i), got.Value)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.setupStore()
			defer store.Close()

			detached, err := store.DetachPrefix(ctx, tc.prefix...)
			if tc.wantErr != nil {
				require.Error(t, err)
				require.Equal(t, tc.wantErr.Error(), err.Error())
				return
			}
			require.NoError(t, err)
			defer detached.Close()

			dCount, err := detached.Count(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.wantDetachedCount, dCount)

			rCount, err := store.Count(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.wantRemainCount, rCount)

			if tc.assertSideEffects != nil {
				tc.assertSideEffects(t, store, detached)
			}
		})
	}
}

func TestPrefixTrie_PruneMatching_Spec(t *testing.T) {
	ctx := context.Background()

	type testCase struct {
		name              string
		setupTargetStore  func() *storage.PrefixTrieGraphStore
		setupOtherStore   func() storage.GraphStore
		wantPrunedCount   int
		wantRemainCount   int
		wantErr           error
		assertSideEffects func(t *testing.T, target *storage.PrefixTrieGraphStore)
	}

	testCases := []testCase{
		{
			name: "MUST remove all coordinates present in other store and leave non-matching untouched",
			setupTargetStore: func() *storage.PrefixTrieGraphStore {
				store := storage.NewPrefixTrieGraphStore()
				for i := 1; i <= 10; i++ {
					node := storage.NodeCoordinate{
						Entity:      storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: fmt.Sprintf("plot_%d", i)},
						PrimaryCode: "GRAIN_YIELD",
						ClassID:     "CLASS_YIELD",
						InstanceID:  "I1",
					}
					_ = store.Put(ctx, storage.StoredMeasurement{Node: node, Value: float64(i), Timestamp: time.Now()})
				}
				return store
			},
			setupOtherStore: func() storage.GraphStore {
				// Store with plots 1, 2, 3
				other := storage.NewPrefixTrieGraphStore()
				for i := 1; i <= 3; i++ {
					node := storage.NodeCoordinate{
						Entity:      storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: fmt.Sprintf("plot_%d", i)},
						PrimaryCode: "GRAIN_YIELD",
						ClassID:     "CLASS_YIELD",
						InstanceID:  "I1",
					}
					_ = other.Put(ctx, storage.StoredMeasurement{Node: node, Value: float64(i), Timestamp: time.Now()})
				}
				return other
			},
			wantPrunedCount: 3,
			wantRemainCount: 7,
			wantErr:         nil,
			assertSideEffects: func(t *testing.T, target *storage.PrefixTrieGraphStore) {
				// Plots 1..3 must be deleted
				for i := 1; i <= 3; i++ {
					node := storage.NodeCoordinate{
						Entity:      storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: fmt.Sprintf("plot_%d", i)},
						PrimaryCode: "GRAIN_YIELD",
						ClassID:     "CLASS_YIELD",
						InstanceID:  "I1",
					}
					_, found, err := target.Get(ctx, node)
					require.NoError(t, err)
					require.False(t, found, "plot_%d MUST be pruned", i)
				}
				// Plots 4..10 must remain
				for i := 4; i <= 10; i++ {
					node := storage.NodeCoordinate{
						Entity:      storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: fmt.Sprintf("plot_%d", i)},
						PrimaryCode: "GRAIN_YIELD",
						ClassID:     "CLASS_YIELD",
						InstanceID:  "I1",
					}
					got, found, err := target.Get(ctx, node)
					require.NoError(t, err)
					require.True(t, found, "plot_%d MUST remain", i)
					require.Equal(t, float64(i), got.Value)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			target := tc.setupTargetStore()
			defer target.Close()

			other := tc.setupOtherStore()
			defer other.Close()

			prunedCount, err := target.PruneMatching(ctx, other)
			if tc.wantErr != nil {
				require.Error(t, err)
				require.Equal(t, tc.wantErr.Error(), err.Error())
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantPrunedCount, prunedCount)

			remainCount, err := target.Count(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.wantRemainCount, remainCount)

			if tc.assertSideEffects != nil {
				tc.assertSideEffects(t, target)
			}
		})
	}
}

func TestPrefixTrie_CrossTrieInvalidation_Integration_Spec(t *testing.T) {
	ctx := context.Background()

	// 1. Setup dual stores:
	// - entityStore: EntityFirst [BU, Type, ID, Primary, Class, Instance]
	// - primaryStore: PrimaryFirst [Primary, Class, Instance, BU, Type, ID]
	entityStore := storage.NewPrefixTrieGraphStoreWithPartitioner(storage.EntityFirstPartitioner, storage.DefaultTrieShards)
	defer entityStore.Close()

	primaryStore := storage.NewPrefixTrieGraphStoreWithPartitioner(storage.PrimaryFirstPartitioner, storage.DefaultTrieShards)
	defer primaryStore.Close()

	const numEntities = 50

	// Populate 50 entities with 2 metrics each:
	// Metric 1: GRAIN_YIELD on COMBINE_1 (Faulty fleet to be revoked)
	// Metric 2: GRAIN_MOISTURE on PROBE_1 (Healthy sensor)
	for e := 1; e <= numEntities; e++ {
		ent := storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: fmt.Sprintf("plot_%d", e)}

		yieldMeas := storage.StoredMeasurement{
			Node: storage.NodeCoordinate{
				Entity:      ent,
				PrimaryCode: "GRAIN_YIELD",
				ClassID:     "CLASS_GRAIN_YIELD",
				InstanceID:  "COMBINE_1",
			},
			Value:     160.0 + float64(e),
			Timestamp: time.Now(),
		}
		_ = entityStore.Put(ctx, yieldMeas)
		_ = primaryStore.Put(ctx, yieldMeas)

		moistureMeas := storage.StoredMeasurement{
			Node: storage.NodeCoordinate{
				Entity:      ent,
				PrimaryCode: "GRAIN_MOISTURE",
				ClassID:     "CLASS_GRAIN_MOISTURE",
				InstanceID:  "PROBE_1",
			},
			Value:     14.5,
			Timestamp: time.Now(),
		}
		_ = entityStore.Put(ctx, moistureMeas)
		_ = primaryStore.Put(ctx, moistureMeas)
	}

	totalFacts, err := primaryStore.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 100, totalFacts)

	// --- CROSS-TRIE INVALIDATION WORKFLOW ---

	// Step 1: Detach the faulty sensor fleet [Primary=GRAIN_YIELD, Class=CLASS_GRAIN_YIELD, Instance=COMBINE_1] in O(1)
	detachedMiniStore, err := primaryStore.DetachPrefix(ctx, "GRAIN_YIELD", "CLASS_GRAIN_YIELD", "COMBINE_1")
	require.NoError(t, err)
	defer detachedMiniStore.Close()

	detachedCount, err := detachedMiniStore.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 50, detachedCount)

	primaryRemainCount, err := primaryStore.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 50, primaryRemainCount, "primaryStore count must reflect O(1) subtree clip")

	// Step 2: Mini-Rekey detached store to EntityFirst layout (only touches k=50 nodes)
	rekeyedMiniStore, err := storage.Rekey(ctx, storage.EntityFirstPartitioner, detachedMiniStore)
	require.NoError(t, err)
	defer rekeyedMiniStore.Close()

	// Step 3: Targeted Prune on Entity Store
	prunedCount, err := entityStore.PruneMatching(ctx, rekeyedMiniStore)
	require.NoError(t, err)
	require.Equal(t, 50, prunedCount)

	entityRemainCount, err := entityStore.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 50, entityRemainCount)

	// Step 4: Verification of Integrity
	for e := 1; e <= numEntities; e++ {
		ent := storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: fmt.Sprintf("plot_%d", e)}
		yieldNode := storage.NodeCoordinate{
			Entity:      ent,
			PrimaryCode: "GRAIN_YIELD",
			ClassID:     "CLASS_GRAIN_YIELD",
			InstanceID:  "COMBINE_1",
		}
		moistureNode := storage.NodeCoordinate{
			Entity:      ent,
			PrimaryCode: "GRAIN_MOISTURE",
			ClassID:     "CLASS_GRAIN_MOISTURE",
			InstanceID:  "PROBE_1",
		}

		// Yield must be completely pruned in BOTH stores
		_, found, err := entityStore.Get(ctx, yieldNode)
		require.NoError(t, err)
		require.False(t, found, "yield node on entityStore MUST be pruned")

		_, found, err = primaryStore.Get(ctx, yieldNode)
		require.NoError(t, err)
		require.False(t, found, "yield node on primaryStore MUST be pruned")

		// Moisture must remain untouched in BOTH stores
		got, found, err := entityStore.Get(ctx, moistureNode)
		require.NoError(t, err)
		require.True(t, found, "moisture node on entityStore MUST remain")
		require.Equal(t, 14.5, got.Value)

		got, found, err = primaryStore.Get(ctx, moistureNode)
		require.NoError(t, err)
		require.True(t, found, "moisture node on primaryStore MUST remain")
		require.Equal(t, 14.5, got.Value)
	}
}
