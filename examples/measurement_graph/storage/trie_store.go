package storage

import (
	"context"
	"sync"
	"sync/atomic"
)

// DefaultTrieShards is the default number of entity lock shards.
const DefaultTrieShards = 256

// trieNode represents a node in the prefix trie.
type trieNode struct {
	mu       sync.RWMutex
	children map[string]*trieNode
	value    *StoredMeasurement
}

func newTrieNode() *trieNode {
	return &trieNode{
		children: make(map[string]*trieNode),
	}
}

// PrefixTrieGraphStore implements GraphStore using an inverted prefix-trie with striped locking.
type PrefixTrieGraphStore struct {
	partitioner CoordinatePartitioner
	shards      []sync.RWMutex
	mask        uint32
	root        *trieNode
	count       atomic.Int64
}

// NewPrefixTrieGraphStore constructs an empty prefix-trie graph store with EntityFirstPartitioner and DefaultTrieShards.
func NewPrefixTrieGraphStore() *PrefixTrieGraphStore {
	return NewPrefixTrieGraphStoreWithPartitioner(EntityFirstPartitioner, DefaultTrieShards)
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
	actualShards := 1
	if shards > 1 {
		actualShards = 1
		for actualShards < shards {
			actualShards <<= 1
		}
	}

	return &PrefixTrieGraphStore{
		partitioner: partitioner,
		shards:      make([]sync.RWMutex, actualShards),
		mask:        uint32(actualShards - 1),
		root:        newTrieNode(),
	}
}

// ShardCount returns the number of lock shards configured.
func (t *PrefixTrieGraphStore) ShardCount() int {
	return len(t.shards)
}

func (t *PrefixTrieGraphStore) shardForSegments(segs []string) *sync.RWMutex {
	if len(t.shards) == 1 {
		return &t.shards[0]
	}
	var rootSegs []string
	if len(segs) >= 3 {
		rootSegs = segs[:3]
	} else {
		rootSegs = segs
	}
	h := fnv32(rootSegs...)
	return &t.shards[h&t.mask]
}

// fnv32 hashes strings into a 32-bit uint using non-allocating FNV-1a.
func fnv32(parts ...string) uint32 {
	const offset32 = 2166136261
	const prime32 = 16777619
	hash := uint32(offset32)
	for _, p := range parts {
		for i := 0; i < len(p); i++ {
			hash ^= uint32(p[i])
			hash *= prime32
		}
		hash ^= uint32(':')
		hash *= prime32
	}
	return hash
}

func (t *PrefixTrieGraphStore) getOrCreateChild(parent *trieNode, seg string) *trieNode {
	parent.mu.RLock()
	child, exists := parent.children[seg]
	parent.mu.RUnlock()
	if exists {
		return child
	}

	parent.mu.Lock()
	defer parent.mu.Unlock()
	child, exists = parent.children[seg]
	if !exists {
		child = newTrieNode()
		parent.children[seg] = child
	}
	return child
}

func (t *PrefixTrieGraphStore) getChild(parent *trieNode, seg string) *trieNode {
	parent.mu.RLock()
	defer parent.mu.RUnlock()
	return parent.children[seg]
}

// Put places an observation measurement into the prefix trie.
func (t *PrefixTrieGraphStore) Put(ctx context.Context, m StoredMeasurement) error {
	segs := t.partitioner(m.Node)
	shard := t.shardForSegments(segs)
	shard.Lock()
	defer shard.Unlock()

	curr := t.root
	for _, seg := range segs {
		curr = t.getOrCreateChild(curr, seg)
	}

	curr.mu.Lock()
	if curr.value == nil {
		t.count.Add(1)
	}
	mCopy := m
	curr.value = &mCopy
	curr.mu.Unlock()

	return nil
}

// Get fetches an exact measurement by its NodeCoordinate path segments.
func (t *PrefixTrieGraphStore) Get(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error) {
	segs := t.partitioner(node)
	shard := t.shardForSegments(segs)
	shard.RLock()
	defer shard.RUnlock()

	curr := t.root
	for _, seg := range segs {
		curr = t.getChild(curr, seg)
		if curr == nil {
			return StoredMeasurement{}, false, nil
		}
	}

	curr.mu.RLock()
	defer curr.mu.RUnlock()
	if curr.value == nil {
		return StoredMeasurement{}, false, nil
	}
	return *curr.value, true, nil
}

// GetByEntity collects all stored measurements matching an entity identity.
func (t *PrefixTrieGraphStore) GetByEntity(ctx context.Context, entity EntityIdentity) ([]StoredMeasurement, error) {
	var results []StoredMeasurement
	all, err := t.All(ctx)
	if err != nil {
		return nil, err
	}
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
	all, err := t.All(ctx)
	if err != nil {
		return nil, err
	}
	for _, m := range all {
		if m.Node.Entity == entity && m.Node.ClassID == classID {
			results = append(results, m)
		}
	}
	return results, nil
}

// DetachPrefix removes the subtree at the specified prefix path from the trie in O(1)
// and returns it as an independent, fully-functional *PrefixTrieGraphStore*.
func (t *PrefixTrieGraphStore) DetachPrefix(ctx context.Context, prefix ...string) (*PrefixTrieGraphStore, error) {
	detachedStore := NewPrefixTrieGraphStoreWithPartitioner(t.partitioner, DefaultTrieShards)
	if len(prefix) == 0 {
		return detachedStore, nil
	}

	shard := t.shardForSegments(prefix)
	shard.Lock()
	defer shard.Unlock()

	var targetNode *trieNode

	if len(prefix) == 1 {
		t.root.mu.Lock()
		node, exists := t.root.children[prefix[0]]
		if exists {
			delete(t.root.children, prefix[0])
			targetNode = node
		}
		t.root.mu.Unlock()
	} else {
		curr := t.root
		for i := 0; i < len(prefix)-1; i++ {
			curr = t.getChild(curr, prefix[i])
			if curr == nil {
				return detachedStore, nil
			}
		}

		lastSeg := prefix[len(prefix)-1]
		curr.mu.Lock()
		node, exists := curr.children[lastSeg]
		if exists {
			delete(curr.children, lastSeg)
			targetNode = node
		}
		curr.mu.Unlock()
	}

	if targetNode == nil {
		return detachedStore, nil
	}

	subCount := t.countSubtreeValues(targetNode)
	t.count.Add(-int64(subCount))

	// Wire detached node tree into detachedStore matching path prefix
	detachedCurr := detachedStore.root
	for _, seg := range prefix[:len(prefix)-1] {
		detachedCurr = detachedStore.getOrCreateChild(detachedCurr, seg)
	}
	detachedCurr.mu.Lock()
	detachedCurr.children[prefix[len(prefix)-1]] = targetNode
	detachedCurr.mu.Unlock()

	detachedStore.count.Store(int64(subCount))
	return detachedStore, nil
}

// PruneMatching removes all coordinates present in otherStore from the receiver store.
// Returns the count of removed measurements.
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
		// Check if it exists before retracting to count accurately
		_, found, err := t.Get(ctx, obs.Node)
		if err == nil && found {
			if err := t.Retract(ctx, obs.Node); err == nil {
				prunedCount++
			}
		}
	}

	return prunedCount, nil
}

// InvalidatePrefix prunes any subtree matching a prefix path of arbitrary depth in O(1).
func (t *PrefixTrieGraphStore) InvalidatePrefix(ctx context.Context, prefix ...string) error {
	if len(prefix) == 0 {
		return nil
	}

	shard := t.shardForSegments(prefix)
	shard.Lock()
	defer shard.Unlock()

	if len(prefix) == 1 {
		t.root.mu.Lock()
		targetNode, exists := t.root.children[prefix[0]]
		if !exists {
			t.root.mu.Unlock()
			return nil
		}
		delete(t.root.children, prefix[0])
		t.root.mu.Unlock()

		subCount := t.countSubtreeValues(targetNode)
		t.count.Add(-int64(subCount))
		return nil
	}

	curr := t.root
	for i := 0; i < len(prefix)-1; i++ {
		curr = t.getChild(curr, prefix[i])
		if curr == nil {
			return nil
		}
	}

	lastSeg := prefix[len(prefix)-1]
	curr.mu.Lock()
	targetNode, exists := curr.children[lastSeg]
	if !exists {
		curr.mu.Unlock()
		return nil
	}
	delete(curr.children, lastSeg)
	curr.mu.Unlock()

	subCount := t.countSubtreeValues(targetNode)
	t.count.Add(-int64(subCount))
	return nil
}

// InvalidateEntity prunes the subtree for an entity.
func (t *PrefixTrieGraphStore) InvalidateEntity(ctx context.Context, entity EntityIdentity) error {
	segs := []string{entity.BusinessUnit, entity.EntityType, entity.EntityID}
	return t.InvalidatePrefix(ctx, segs...)
}

// Retract removes a specific observation node coordinate.
func (t *PrefixTrieGraphStore) Retract(ctx context.Context, node NodeCoordinate) error {
	segs := t.partitioner(node)
	shard := t.shardForSegments(segs)
	shard.Lock()
	defer shard.Unlock()

	stack := []*trieNode{t.root}
	curr := t.root

	for _, seg := range segs {
		curr = t.getChild(curr, seg)
		if curr == nil {
			return nil
		}
		stack = append(stack, curr)
	}

	curr.mu.Lock()
	if curr.value != nil {
		curr.value = nil
		t.count.Add(-1)
	}
	curr.mu.Unlock()

	// Clean up empty leaf nodes up the trie stack towards root
	for i := len(segs) - 1; i >= 0; i-- {
		parent := stack[i]
		seg := segs[i]

		parent.mu.Lock()
		child := parent.children[seg]
		if child != nil {
			child.mu.RLock()
			isEmpty := len(child.children) == 0 && child.value == nil
			child.mu.RUnlock()
			if isEmpty {
				delete(parent.children, seg)
			}
		}
		parent.mu.Unlock()
	}

	return nil
}

// All retrieves all stored measurements across the trie.
func (t *PrefixTrieGraphStore) All(ctx context.Context) ([]StoredMeasurement, error) {
	for i := range t.shards {
		t.shards[i].RLock()
		defer t.shards[i].RUnlock()
	}

	var results []StoredMeasurement
	t.collectValues(t.root, &results)
	return results, nil
}

// Count returns the total number of placed measurements in the trie.
func (t *PrefixTrieGraphStore) Count(ctx context.Context) (int, error) {
	return int(t.count.Load()), nil
}

// Close resets the trie and clears counters across all shards.
func (t *PrefixTrieGraphStore) Close() error {
	for i := range t.shards {
		t.shards[i].Lock()
		defer t.shards[i].Unlock()
	}

	t.root.mu.Lock()
	t.root = newTrieNode()
	t.count.Store(0)
	return nil
}

func (t *PrefixTrieGraphStore) collectValues(curr *trieNode, out *[]StoredMeasurement) {
	if curr == nil {
		return
	}
	curr.mu.RLock()
	if curr.value != nil {
		*out = append(*out, *curr.value)
	}
	children := make([]*trieNode, 0, len(curr.children))
	for _, child := range curr.children {
		children = append(children, child)
	}
	curr.mu.RUnlock()

	for _, child := range children {
		t.collectValues(child, out)
	}
}

func (t *PrefixTrieGraphStore) countSubtreeValues(curr *trieNode) int {
	if curr == nil {
		return 0
	}
	curr.mu.RLock()
	c := 0
	if curr.value != nil {
		c++
	}
	children := make([]*trieNode, 0, len(curr.children))
	for _, child := range curr.children {
		children = append(children, child)
	}
	curr.mu.RUnlock()

	for _, child := range children {
		c += t.countSubtreeValues(child)
	}
	return c
}
