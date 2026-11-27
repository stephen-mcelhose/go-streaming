package storage

import (
	"context"
	"hash/fnv"
	"sync"
)

// DualIndexShard encapsulates two un-synchronized prefix-tries protected by a single read-write mutex.
// TrieByEntity is ordered entity-first (BU -> EntityType -> EntityID -> Primary -> Class -> Instance).
// TrieByPrimary is ordered primary-first (Primary -> Class -> Instance -> BU -> EntityType -> EntityID).
type DualIndexShard struct {
	mu            sync.RWMutex
	trieByEntity  *PrefixTrieGraphStore
	trieByPrimary *PrefixTrieGraphStore
}

// newDualIndexShard constructs an affinity shard wrapping two 1-shard PrefixTrieGraphStores.
func newDualIndexShard() *DualIndexShard {
	return &DualIndexShard{
		trieByEntity:  NewPrefixTrieGraphStoreWithPartitioner(EntityFirstPartitioner, 1),
		trieByPrimary: NewPrefixTrieGraphStoreWithPartitioner(PrimaryFirstPartitioner, 1),
	}
}

// Put writes an observation atomically to both internal tries under the shard lock.
func (s *DualIndexShard) Put(ctx context.Context, m StoredMeasurement) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.trieByEntity.Put(ctx, m); err != nil {
		return err
	}
	return s.trieByPrimary.Put(ctx, m)
}

// Get fetches a measurement from the entity-first trie.
func (s *DualIndexShard) Get(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.trieByEntity.Get(ctx, node)
}

// GetByEntity fetches all measurements matching an entity identity.
func (s *DualIndexShard) GetByEntity(ctx context.Context, entity EntityIdentity) ([]StoredMeasurement, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.trieByEntity.GetByEntity(ctx, entity)
}

// GetByClass fetches all measurements for an entity matching a class ID.
func (s *DualIndexShard) GetByClass(ctx context.Context, entity EntityIdentity, classID string) ([]StoredMeasurement, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.trieByEntity.GetByClass(ctx, entity, classID)
}

// InvalidateEntity prunes an entity's working memory subtree from TrieByEntity in O(1)
// and retracts those items from TrieByPrimary under the single shard lock.
func (s *DualIndexShard) InvalidateEntity(ctx context.Context, entity EntityIdentity) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Detach entity subtree from TrieByEntity in O(1)
	segs := []string{entity.BusinessUnit, entity.EntityType, entity.EntityID}
	detached, err := s.trieByEntity.DetachPrefix(ctx, segs...)
	if err != nil {
		return err
	}

	// 2. Retract those matching coordinates from TrieByPrimary
	_, err = s.trieByPrimary.PruneMatching(ctx, detached)
	return err
}

// Retract removes a specific observation node from both internal tries.
func (s *DualIndexShard) Retract(ctx context.Context, node NodeCoordinate) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_ = s.trieByEntity.Retract(ctx, node)
	_ = s.trieByPrimary.Retract(ctx, node)
	return nil
}

// invalidatePrefixFleet performs localized micro-snip on TrieByPrimary and prunes TrieByEntity.
func (s *DualIndexShard) invalidatePrefixFleet(ctx context.Context, prefix ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Micro-snip O(1) from TrieByPrimary
	detached, err := s.trieByPrimary.DetachPrefix(ctx, prefix...)
	if err != nil {
		return 0, err
	}

	// 2. Point retraction O(k_i * depth) from TrieByEntity
	return s.trieByEntity.PruneMatching(ctx, detached)
}

// count returns the total measurement count in TrieByEntity.
func (s *DualIndexShard) count(ctx context.Context) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.trieByEntity.Count(ctx)
}

// all returns all measurements in TrieByEntity.
func (s *DualIndexShard) all(ctx context.Context) ([]StoredMeasurement, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.trieByEntity.All(ctx)
}

// close clears both internal tries.
func (s *DualIndexShard) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.trieByEntity.Close()
	_ = s.trieByPrimary.Close()
	return nil
}

// DualIndexStore is a stateless affinity-sharded GraphStore coordinator implementing ADR-0004.
// Point and Entity operations route directly in O(1) to ShardIdx = fnv32a(EntityIdentity) & (M - 1).
// Cross-cutting fleet operations fan out across all M shards executing localized micro-snips.
type DualIndexStore struct {
	shards    []*DualIndexShard
	numShards int
	mask      int // bitmask for fast power-of-2 routing, or -1 if non-power-of-2
}

// NewDualIndexStore creates a DualIndexStore with default 256 affinity shards.
func NewDualIndexStore() *DualIndexStore {
	return NewDualIndexStoreWithShards(DefaultShards)
}

// NewDualIndexStoreWithShards creates a DualIndexStore with M affinity shards.
func NewDualIndexStoreWithShards(shards int) *DualIndexStore {
	if shards <= 1 {
		shards = 1
	} else if (shards & (shards - 1)) != 0 {
		// Round up to next power of 2
		v := shards - 1
		v |= v >> 1
		v |= v >> 2
		v |= v >> 4
		v |= v >> 8
		v |= v >> 16
		shards = v + 1
	}

	shardList := make([]*DualIndexShard, shards)
	for i := range shardList {
		shardList[i] = newDualIndexShard()
	}

	return &DualIndexStore{
		shards:    shardList,
		numShards: shards,
		mask:      shards - 1,
	}
}

// ShardCount returns the number of affinity shards configured.
func (d *DualIndexStore) ShardCount() int {
	return d.numShards
}

// shardForEntity computes the affinity shard for an EntityIdentity via FNV-1a.
func (d *DualIndexStore) shardForEntity(entity EntityIdentity) *DualIndexShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(entity.BusinessUnit))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(entity.EntityType))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(entity.EntityID))

	var idx int
	if d.mask >= 0 {
		idx = int(h.Sum32()) & d.mask
	} else {
		idx = int(h.Sum32()) % d.numShards
	}
	return d.shards[idx]
}

// Put places an observation into its affinity shard.
func (d *DualIndexStore) Put(ctx context.Context, m StoredMeasurement) error {
	return d.shardForEntity(m.Node.Entity).Put(ctx, m)
}

// Get fetches an exact measurement by NodeCoordinate from its affinity shard.
func (d *DualIndexStore) Get(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error) {
	return d.shardForEntity(node.Entity).Get(ctx, node)
}

// GetByEntity collects all measurements for an entity from its affinity shard.
func (d *DualIndexStore) GetByEntity(ctx context.Context, entity EntityIdentity) ([]StoredMeasurement, error) {
	return d.shardForEntity(entity).GetByEntity(ctx, entity)
}

// GetByClass collects all measurements for an entity matching classID from its affinity shard.
func (d *DualIndexStore) GetByClass(ctx context.Context, entity EntityIdentity, classID string) ([]StoredMeasurement, error) {
	return d.shardForEntity(entity).GetByClass(ctx, entity, classID)
}

// InvalidateEntity prunes an entity's working memory strictly within its affinity shard.
func (d *DualIndexStore) InvalidateEntity(ctx context.Context, entity EntityIdentity) error {
	return d.shardForEntity(entity).InvalidateEntity(ctx, entity)
}

// Retract removes an observation coordinate from its affinity shard.
func (d *DualIndexStore) Retract(ctx context.Context, node NodeCoordinate) error {
	return d.shardForEntity(node.Entity).Retract(ctx, node)
}

// InvalidatePrefix fans out across all M shards, executing an O(1) micro-snip on TrieByPrimary
// and targeted O(k_i * depth) retraction on TrieByEntity within each shard.
func (d *DualIndexStore) InvalidatePrefix(ctx context.Context, prefix ...string) error {
	for _, shard := range d.shards {
		if _, err := shard.invalidatePrefixFleet(ctx, prefix...); err != nil {
			return err
		}
	}
	return nil
}

// InvalidatePrefixCount fans out across all M shards and returns the total pruned count.
func (d *DualIndexStore) InvalidatePrefixCount(ctx context.Context, prefix ...string) (int, error) {
	totalPruned := 0
	for _, shard := range d.shards {
		pruned, err := shard.invalidatePrefixFleet(ctx, prefix...)
		if err != nil {
			return totalPruned, err
		}
		totalPruned += pruned
	}
	return totalPruned, nil
}

// All collects all stored measurements across all affinity shards.
func (d *DualIndexStore) All(ctx context.Context) ([]StoredMeasurement, error) {
	var all []StoredMeasurement
	for _, shard := range d.shards {
		items, err := shard.all(ctx)
		if err != nil {
			return nil, err
		}
		all = append(all, items...)
	}
	return all, nil
}

// Count returns the aggregate count across all affinity shards.
func (d *DualIndexStore) Count(ctx context.Context) (int, error) {
	total := 0
	for _, shard := range d.shards {
		c, err := shard.count(ctx)
		if err != nil {
			return 0, err
		}
		total += c
	}
	return total, nil
}

// Close resets and cleans up all affinity shards.
func (d *DualIndexStore) Close() error {
	for _, shard := range d.shards {
		if err := shard.close(); err != nil {
			return err
		}
	}
	return nil
}

// GetPrimaryAxis fetches a measurement directly from TrieByPrimary across shards (diagnostic/audit).
func (d *DualIndexStore) GetPrimaryAxis(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error) {
	shard := d.shardForEntity(node.Entity)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	return shard.trieByPrimary.Get(ctx, node)
}
