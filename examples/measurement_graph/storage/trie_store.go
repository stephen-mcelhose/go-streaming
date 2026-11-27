package storage

import (
	"context"
)

// PrefixTrieGraphStore adapts the generic PrefixTrie[string, StoredMeasurement] to the GraphStore contract.
type PrefixTrieGraphStore struct {
	trie        *PrefixTrie[string, StoredMeasurement]
	partitioner CoordinatePartitioner
}

// NewPrefixTrieGraphStore constructs an empty prefix-trie graph store with EntityFirstPartitioner and DefaultShards.
func NewPrefixTrieGraphStore() *PrefixTrieGraphStore {
	return NewPrefixTrieGraphStoreWithPartitioner(EntityFirstPartitioner, DefaultShards)
}

// NewPrefixTrieGraphStoreWithShards constructs a prefix-trie with EntityFirstPartitioner and N shards.
func NewPrefixTrieGraphStoreWithShards(shards int) *PrefixTrieGraphStore {
	return NewPrefixTrieGraphStoreWithPartitioner(EntityFirstPartitioner, shards)
}

// NewPrefixTrieGraphStoreWithPartitioner constructs a prefix-trie with a custom CoordinatePartitioner and N shards.
func NewPrefixTrieGraphStoreWithPartitioner(partitioner CoordinatePartitioner, shards int) *PrefixTrieGraphStore {
	if partitioner == nil {
		partitioner = EntityFirstPartitioner
	}
	return &PrefixTrieGraphStore{
		trie:        NewGenericPrefixTrieWithShards[string, StoredMeasurement](shards),
		partitioner: partitioner,
	}
}

// ShardCount returns the number of lock shards configured.
func (t *PrefixTrieGraphStore) ShardCount() int {
	return t.trie.ShardCount()
}

// Put places an observation measurement into the prefix trie.
func (t *PrefixTrieGraphStore) Put(ctx context.Context, m StoredMeasurement) error {
	path := t.partitioner(m.Node)
	t.trie.Put(path, m)
	return nil
}

// Get fetches an exact measurement by its NodeCoordinate path segments.
func (t *PrefixTrieGraphStore) Get(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error) {
	path := t.partitioner(node)
	val, ok := t.trie.Get(path)
	return val, ok, nil
}

// GetByEntity collects all stored measurements matching an entity identity.
func (t *PrefixTrieGraphStore) GetByEntity(ctx context.Context, entity EntityIdentity) ([]StoredMeasurement, error) {
	var results []StoredMeasurement
	all := t.trie.CollectAll()
	for _, m := range all {
		if m.Node.Entity == entity {
			results = append(results, m)
		}
	}
	return results, nil
}

// GetByClass collects all stored measurements under an entity matching ClassID.
func (t *PrefixTrieGraphStore) GetByClass(ctx context.Context, entity EntityIdentity, classID string) ([]StoredMeasurement, error) {
	var results []StoredMeasurement
	all := t.trie.CollectAll()
	for _, m := range all {
		if m.Node.Entity == entity && m.Node.ClassID == classID {
			results = append(results, m)
		}
	}
	return results, nil
}

// InvalidatePrefix prunes any subtree matching a prefix path of arbitrary depth in O(1).
func (t *PrefixTrieGraphStore) InvalidatePrefix(ctx context.Context, prefix ...string) error {
	t.trie.InvalidatePrefix(prefix...)
	return nil
}

// InvalidateEntity prunes the subtree for an entity.
func (t *PrefixTrieGraphStore) InvalidateEntity(ctx context.Context, entity EntityIdentity) error {
	segs := []string{entity.BusinessUnit, entity.EntityType, entity.EntityID}
	return t.InvalidatePrefix(ctx, segs...)
}

// Retract removes a specific observation node coordinate.
func (t *PrefixTrieGraphStore) Retract(ctx context.Context, node NodeCoordinate) error {
	path := t.partitioner(node)
	t.trie.Delete(path)
	return nil
}

// DetachPrefix removes the subtree at prefix in O(1) and returns it as a standalone PrefixTrieGraphStore.
func (t *PrefixTrieGraphStore) DetachPrefix(ctx context.Context, prefix ...string) (*PrefixTrieGraphStore, error) {
	detachedGeneric := t.trie.DetachPrefix(prefix...)
	return &PrefixTrieGraphStore{
		trie:        detachedGeneric,
		partitioner: t.partitioner,
	}, nil
}

// PruneMatching removes all coordinates present in otherStore from the receiver store.
func (t *PrefixTrieGraphStore) PruneMatching(ctx context.Context, otherStore GraphStore) (int, error) {
	if otherStore == nil {
		return 0, nil
	}

	allOther, err := otherStore.All(ctx)
	if err != nil {
		return 0, err
	}

	prunedCount := 0
	for _, obs := range allOther {
		path := t.partitioner(obs.Node)
		if _, found := t.trie.Get(path); found {
			if t.trie.Delete(path) {
				prunedCount++
			}
		}
	}

	return prunedCount, nil
}

// All retrieves all stored measurements across the trie.
func (t *PrefixTrieGraphStore) All(ctx context.Context) ([]StoredMeasurement, error) {
	return t.trie.CollectAll(), nil
}

// Count returns the total number of placed measurements in the trie.
func (t *PrefixTrieGraphStore) Count(ctx context.Context) (int, error) {
	return t.trie.Count(), nil
}

// Close resets the trie and clears counters across all shards.
func (t *PrefixTrieGraphStore) Close() error {
	if t != nil && t.trie != nil {
		t.trie.Close()
	}
	return nil
}
