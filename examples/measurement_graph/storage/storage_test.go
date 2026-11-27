package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming/examples/measurement_graph/storage"
)

func TestStorageImplementations(t *testing.T) {
	ctx := context.Background()

	stores := map[string]func() storage.GraphStore{
		"MemoryGraphStore": func() storage.GraphStore {
			return storage.NewMemoryGraphStore()
		},
		"PrefixTrieGraphStore": func() storage.GraphStore {
			return storage.NewPrefixTrieGraphStore()
		},
		"RelateDBGraphStore": func() storage.GraphStore {
			s, err := storage.NewRelateDBGraphStore(storage.RelateDBConfig{InMemory: true})
			if err != nil {
				t.Fatalf("failed to create RelateDBGraphStore: %v", err)
			}
			return s
		},
	}

	for name, createStore := range stores {
		t.Run(name, func(t *testing.T) {
			store := createStore()
			defer store.Close()

			entity1 := storage.EntityIdentity{
				BusinessUnit: "Agronomy",
				EntityType:   "PLOT",
				EntityID:     "plot_101",
			}
			node1 := storage.NodeCoordinate{
				Entity:          entity1,
				PrimaryCode:     "GRAIN_YIELD",
				ClassID:         "CLASS_GRAIN_YIELD",
				InstanceID:      "C4220-I2",
				Characteristics: map[string]string{"DEVICE": "COMBINE_A"},
			}
			meas1 := storage.StoredMeasurement{
				Node:      node1,
				Value:     185.4,
				Timestamp: time.Now(),
			}

			// MUST Put and Get
			err := store.Put(ctx, meas1)
			require.NoError(t, err)

			got, found, err := store.Get(ctx, node1)
			require.NoError(t, err)
			require.True(t, found)
			require.InDelta(t, 185.4, got.Value, 1e-6)

			// MUST Count
			count, err := store.Count(ctx)
			require.NoError(t, err)
			require.Equal(t, 1, count)

			// MUST GetByEntity
			entityNodes, err := store.GetByEntity(ctx, entity1)
			require.NoError(t, err)
			require.Len(t, entityNodes, 1)

			// MUST GetByClass
			classNodes, err := store.GetByClass(ctx, entity1, "CLASS_GRAIN_YIELD")
			require.NoError(t, err)
			require.Len(t, classNodes, 1)

			// MUST Retract
			err = store.Retract(ctx, node1)
			require.NoError(t, err)

			_, foundAfterRetract, err := store.Get(ctx, node1)
			require.NoError(t, err)
			require.False(t, foundAfterRetract)

			countAfterRetract, err := store.Count(ctx)
			require.NoError(t, err)
			require.Equal(t, 0, countAfterRetract)

			// Put again and test InvalidateEntity
			err = store.Put(ctx, meas1)
			require.NoError(t, err)

			err = store.InvalidateEntity(ctx, entity1)
			require.NoError(t, err)

			entityNodesAfterInvalidate, err := store.GetByEntity(ctx, entity1)
			require.NoError(t, err)
			require.Empty(t, entityNodesAfterInvalidate)

			countAfterInvalidate, err := store.Count(ctx)
			require.NoError(t, err)
			require.Equal(t, 0, countAfterInvalidate)

			// Close and verify operations
			require.NoError(t, store.Close())
		})
	}
}
