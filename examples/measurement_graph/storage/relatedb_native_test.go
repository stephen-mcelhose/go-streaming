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

func TestNativeRelateDBGraphStore(t *testing.T) {
	ctx := context.Background()

	store, err := storage.NewNativeRelateDBGraphStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	entity1 := storage.EntityIdentity{
		BusinessUnit: "Agronomy",
		EntityType:   "PLOT",
		EntityID:     "plot_777",
	}

	node1 := storage.NodeCoordinate{
		Entity:      entity1,
		PrimaryCode: "GRAIN_YIELD",
		ClassID:     "CLASS_GRAIN_YIELD",
		InstanceID:  "COMBINE_X",
	}

	node2 := storage.NodeCoordinate{
		Entity:      entity1,
		PrimaryCode: "MOISTURE",
		ClassID:     "CLASS_MOISTURE",
		InstanceID:  "OVEN_LAB",
	}

	// 1. MUST Put Observation nodes into live RelateDB graph hierarchy
	err = store.Put(ctx, storage.StoredMeasurement{
		Node:      node1,
		Value:     192.5,
		Timestamp: time.Now(),
	})
	require.NoError(t, err)

	err = store.Put(ctx, storage.StoredMeasurement{
		Node:      node2,
		Value:     15.2,
		Timestamp: time.Now(),
	})
	require.NoError(t, err)

	// 2. MUST Count total observation nodes via Cypher
	count, err := store.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	// 3. MUST Get specific node by coordinate
	got, found, err := store.Get(ctx, node1)
	require.NoError(t, err)
	require.True(t, found)
	require.InDelta(t, 192.5, got.Value, 1e-6)

	// 4. MUST GetByEntity traversing (Entity)-[:OBSERVED]->(Observation)
	entityObservations, err := store.GetByEntity(ctx, entity1)
	require.NoError(t, err)
	require.Len(t, entityObservations, 2)

	// 5. MUST GetByClass traversing (Entity)-[:OBSERVED]->(Observation)-[:INSTANCE_OF]->(:Instance)-[:MEMBER_OF]->(Class)
	yieldClassObservations, err := store.GetByClass(ctx, entity1, "CLASS_GRAIN_YIELD")
	require.NoError(t, err)
	require.Len(t, yieldClassObservations, 1)
	require.InDelta(t, 192.5, yieldClassObservations[0].Value, 1e-6)

	// 6. MUST Retract specific node
	err = store.Retract(ctx, node1)
	require.NoError(t, err)

	_, foundAfterRetract, err := store.Get(ctx, node1)
	require.NoError(t, err)
	require.False(t, foundAfterRetract)

	countAfterRetract, err := store.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, countAfterRetract)

	// 7. MUST InvalidateEntity
	err = store.InvalidateEntity(ctx, entity1)
	require.NoError(t, err)

	countAfterInvalidate, err := store.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, countAfterInvalidate)
}
