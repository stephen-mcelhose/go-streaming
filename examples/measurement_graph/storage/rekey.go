package storage

import (
	"context"
	"fmt"
	"sort"
)

// CoordinatePartitioner maps a NodeCoordinate into ordered trie path segments.
type CoordinatePartitioner func(coord NodeCoordinate) []string

// EntityFirstPartitioner (Default): [BU, EntityType, EntityID, PrimaryCode, ClassID, InstanceID, ...Characteristics]
func EntityFirstPartitioner(c NodeCoordinate) []string {
	return c.Segments()
}

// PrimaryFirstPartitioner: [PrimaryCode, ClassID, InstanceID, BU, EntityType, EntityID, ...Characteristics]
func PrimaryFirstPartitioner(c NodeCoordinate) []string {
	segs := []string{
		c.PrimaryCode,
		c.ClassID,
		c.InstanceID,
		c.Entity.BusinessUnit,
		c.Entity.EntityType,
		c.Entity.EntityID,
	}
	if len(c.Characteristics) > 0 {
		var keys []string
		for k := range c.Characteristics {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			segs = append(segs, fmt.Sprintf("%s=%s", k, c.Characteristics[k]))
		}
	}
	return segs
}

// ClassFirstPartitioner: [ClassID, PrimaryCode, InstanceID, BU, EntityType, EntityID, ...Characteristics]
func ClassFirstPartitioner(c NodeCoordinate) []string {
	segs := []string{
		c.ClassID,
		c.PrimaryCode,
		c.InstanceID,
		c.Entity.BusinessUnit,
		c.Entity.EntityType,
		c.Entity.EntityID,
	}
	if len(c.Characteristics) > 0 {
		var keys []string
		for k := range c.Characteristics {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			segs = append(segs, fmt.Sprintf("%s=%s", k, c.Characteristics[k]))
		}
	}
	return segs
}

// Rekey constructs a new TrieCache partitioned by the given CoordinatePartitioner,
// populating it with all measurements extracted from the source store.
func Rekey(ctx context.Context, partitioner CoordinatePartitioner, src GraphStore) (*TrieCache, error) {
	if src == nil {
		return nil, fmt.Errorf("rekey: src GraphStore cannot be nil")
	}

	rekeyedTrie := NewPrefixTrieGraphStoreWithPartitioner(partitioner, DefaultTrieShards)

	allObs, err := src.All(ctx)
	if err != nil {
		return nil, fmt.Errorf("rekey: failed to read source store: %w", err)
	}

	for _, obs := range allObs {
		if err := rekeyedTrie.Put(ctx, obs); err != nil {
			return nil, fmt.Errorf("rekey: failed to place observation into rekeyed store: %w", err)
		}
	}

	return &TrieCache{
		PrefixTrieGraphStore: rekeyedTrie,
		l2:                   nil,
	}, nil
}
