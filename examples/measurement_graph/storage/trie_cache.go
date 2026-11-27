package storage

import (
	"context"
)

// TrieCache decorates any L2 backing GraphStore (e.g. RelateDB or external KV)
// with an Inverted PrefixTrie L1 cache for sub-microsecond reads and $O(1)$ entity pruning.
type TrieCache struct {
	*PrefixTrieGraphStore
	l2 GraphStore
}

// NewTrieCache constructs a tiered TrieCache over an L2 backing GraphStore.
func NewTrieCache(l2 GraphStore) *TrieCache {
	return &TrieCache{
		PrefixTrieGraphStore: NewPrefixTrieGraphStore(),
		l2:                   l2,
	}
}

// Put writes synchronously to L1 Trie and propagates to L2 backing store if present.
func (c *TrieCache) Put(ctx context.Context, m StoredMeasurement) error {
	if err := c.PrefixTrieGraphStore.Put(ctx, m); err != nil {
		return err
	}
	if c.l2 != nil {
		return c.l2.Put(ctx, m)
	}
	return nil
}

// Get queries L1 Trie, falling back to L2 backing store on miss and caching in L1.
func (c *TrieCache) Get(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error) {
	if m, found, err := c.PrefixTrieGraphStore.Get(ctx, node); found || err != nil || c.l2 == nil {
		return m, found, err
	}
	m, found, err := c.l2.Get(ctx, node)
	if found && err == nil {
		_ = c.PrefixTrieGraphStore.Put(ctx, m)
	}
	return m, found, err
}

// GetByEntity queries L1 Trie, falling back to L2 and populating L1 on miss.
func (c *TrieCache) GetByEntity(ctx context.Context, entity EntityIdentity) ([]StoredMeasurement, error) {
	if res, err := c.PrefixTrieGraphStore.GetByEntity(ctx, entity); len(res) > 0 || err != nil || c.l2 == nil {
		return res, err
	}
	res, err := c.l2.GetByEntity(ctx, entity)
	if err == nil {
		for _, m := range res {
			_ = c.PrefixTrieGraphStore.Put(ctx, m)
		}
	}
	return res, err
}

// GetByClass queries L1 Trie, falling back to L2 and populating L1 on miss.
func (c *TrieCache) GetByClass(ctx context.Context, entity EntityIdentity, classID string) ([]StoredMeasurement, error) {
	if res, err := c.PrefixTrieGraphStore.GetByClass(ctx, entity, classID); len(res) > 0 || err != nil || c.l2 == nil {
		return res, err
	}
	res, err := c.l2.GetByClass(ctx, entity, classID)
	if err == nil {
		for _, m := range res {
			_ = c.PrefixTrieGraphStore.Put(ctx, m)
		}
	}
	return res, err
}

// All returns all stored measurements from L1 or falls back to L2.
func (c *TrieCache) All(ctx context.Context) ([]StoredMeasurement, error) {
	if res, err := c.PrefixTrieGraphStore.All(ctx); len(res) > 0 || err != nil || c.l2 == nil {
		return res, err
	}
	res, err := c.l2.All(ctx)
	if err == nil {
		for _, m := range res {
			_ = c.PrefixTrieGraphStore.Put(ctx, m)
		}
	}
	return res, err
}

// InvalidateEntity prunes entity working memory from L1 in O(1) and invalidates L2.
func (c *TrieCache) InvalidateEntity(ctx context.Context, entity EntityIdentity) error {
	_ = c.PrefixTrieGraphStore.InvalidateEntity(ctx, entity)
	if c.l2 != nil {
		return c.l2.InvalidateEntity(ctx, entity)
	}
	return nil
}

// Retract removes an observation coordinate from L1 and L2.
func (c *TrieCache) Retract(ctx context.Context, node NodeCoordinate) error {
	_ = c.PrefixTrieGraphStore.Retract(ctx, node)
	if c.l2 != nil {
		return c.l2.Retract(ctx, node)
	}
	return nil
}

// Close cleans up L1 and closes L2 backing store.
func (c *TrieCache) Close() error {
	_ = c.PrefixTrieGraphStore.Close()
	if c.l2 != nil {
		return c.l2.Close()
	}
	return nil
}
