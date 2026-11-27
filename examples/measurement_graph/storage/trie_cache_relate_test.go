//go:build relate
// +build relate

package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming/examples/measurement_graph/storage"
)

func TestTrieCache_LayeredWithRelateDB(t *testing.T) {
	ctx := context.Background()

	relateDB, err := storage.NewNativeRelateDBGraphStore(":memory:")
	require.NoError(t, err)
	defer relateDB.Close()

	cache := storage.NewTrieCache(relateDB)
	defer cache.Close()

	entity := storage.EntityIdentity{
		BusinessUnit: "Agronomy",
		EntityType:   "PLOT",
		EntityID:     "plot_relate_cache_99",
	}

	node1 := storage.NodeCoordinate{
		Entity:      entity,
		PrimaryCode: "GRAIN_YIELD",
		ClassID:     "CLASS_GRAIN_YIELD",
		InstanceID:  "COMBINE_ROTARY",
	}

	meas1 := storage.StoredMeasurement{
		Node:      node1,
		Value:     195.4,
		Timestamp: time.Now(),
	}

	// 1. Put into TrieCache -> writes L1 Trie and propagates Cypher MERGE to RelateDB
	err = cache.Put(ctx, meas1)
	require.NoError(t, err)

	// Verify L1 hit
	l1Got, l1Found, err := cache.Get(ctx, node1)
	require.NoError(t, err)
	require.True(t, l1Found)
	require.InDelta(t, 195.4, l1Got.Value, 1e-6)

	// Verify RelateDB L2 store contains the persisted graph node
	relateGot, relateFound, err := relateDB.Get(ctx, node1)
	require.NoError(t, err)
	require.True(t, relateFound)
	require.InDelta(t, 195.4, relateGot.Value, 1e-6)

	// 2. Query GetByClass through TrieCache
	classObs, err := cache.GetByClass(ctx, entity, "CLASS_GRAIN_YIELD")
	require.NoError(t, err)
	require.Len(t, classObs, 1)
	require.InDelta(t, 195.4, classObs[0].Value, 1e-6)

	// 3. Invalidate entity -> clears both hot L1 Trie and RelateDB graph
	err = cache.InvalidateEntity(ctx, entity)
	require.NoError(t, err)

	count, err := cache.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	relateCount, err := relateDB.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, relateCount)
}
