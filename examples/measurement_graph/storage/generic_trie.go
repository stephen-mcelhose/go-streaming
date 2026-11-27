package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// DefaultShards is the default number of striped lock shards for the generic trie.
const DefaultShards = 256

// DefaultTrieShards aliases DefaultShards for backward compatibility.
const DefaultTrieShards = DefaultShards

type trieNode[K comparable, V any] struct {
	mu       sync.RWMutex
	children map[K]*trieNode[K, V]
	value    *V
}

func newGenericTrieNode[K comparable, V any]() *trieNode[K, V] {
	return &trieNode[K, V]{
		children: make(map[K]*trieNode[K, V]),
	}
}

// PrefixTrie is a domain-agnostic, thread-safe, striped prefix trie
// parameterized over key segment type K and stored value type V.
type PrefixTrie[K comparable, V any] struct {
	shards []sync.RWMutex
	mask   uint32
	root   *trieNode[K, V]
	count  atomic.Int64
}

// NewGenericPrefixTrie constructs a generic PrefixTrie with DefaultShards.
func NewGenericPrefixTrie[K comparable, V any]() *PrefixTrie[K, V] {
	return NewGenericPrefixTrieWithShards[K, V](DefaultShards)
}

// NewGenericPrefixTrieWithShards constructs a generic PrefixTrie with N shards (normalized to power of 2).
func NewGenericPrefixTrieWithShards[K comparable, V any](shards int) *PrefixTrie[K, V] {
	actualShards := 1
	if shards > 1 {
		actualShards = 1
		for actualShards < shards {
			actualShards <<= 1
		}
	}

	return &PrefixTrie[K, V]{
		shards: make([]sync.RWMutex, actualShards),
		mask:   uint32(actualShards - 1),
		root:   newGenericTrieNode[K, V](),
	}
}

// ShardCount returns the number of lock shards configured.
func (t *PrefixTrie[K, V]) ShardCount() int {
	return len(t.shards)
}

func (t *PrefixTrie[K, V]) shardForPath(path []K) *sync.RWMutex {
	if len(t.shards) == 1 {
		return &t.shards[0]
	}
	var rootSegs []K
	if len(path) >= 3 {
		rootSegs = path[:3]
	} else {
		rootSegs = path
	}

	h := fnvGeneric(rootSegs...)
	return &t.shards[h&t.mask]
}

func fnvGeneric[K comparable](parts ...K) uint32 {
	const offset32 = 2166136261
	const prime32 = 16777619
	hash := uint32(offset32)
	for _, p := range parts {
		s := fmt.Sprint(p)
		for i := 0; i < len(s); i++ {
			hash ^= uint32(s[i])
			hash *= prime32
		}
		hash ^= uint32(':')
		hash *= prime32
	}
	return hash
}

func (t *PrefixTrie[K, V]) getOrCreateChild(parent *trieNode[K, V], seg K) *trieNode[K, V] {
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
		child = newGenericTrieNode[K, V]()
		parent.children[seg] = child
	}
	return child
}

func (t *PrefixTrie[K, V]) getChild(parent *trieNode[K, V], seg K) *trieNode[K, V] {
	parent.mu.RLock()
	defer parent.mu.RUnlock()
	return parent.children[seg]
}

// Put places a value at the exact path.
func (t *PrefixTrie[K, V]) Put(path []K, value V) {
	shard := t.shardForPath(path)
	shard.Lock()
	defer shard.Unlock()

	curr := t.root
	for _, seg := range path {
		curr = t.getOrCreateChild(curr, seg)
	}

	curr.mu.Lock()
	if curr.value == nil {
		t.count.Add(1)
	}
	vCopy := value
	curr.value = &vCopy
	curr.mu.Unlock()
}

// Get fetches the value at the exact path.
func (t *PrefixTrie[K, V]) Get(path []K) (V, bool) {
	shard := t.shardForPath(path)
	shard.RLock()
	defer shard.RUnlock()

	curr := t.root
	for _, seg := range path {
		curr = t.getChild(curr, seg)
		if curr == nil {
			var zero V
			return zero, false
		}
	}

	curr.mu.RLock()
	defer curr.mu.RUnlock()
	if curr.value == nil {
		var zero V
		return zero, false
	}
	return *curr.value, true
}

// CollectPrefix collects all values under the prefix path.
func (t *PrefixTrie[K, V]) CollectPrefix(prefix ...K) []V {
	shard := t.shardForPath(prefix)
	shard.RLock()
	defer shard.RUnlock()

	curr := t.root
	for _, seg := range prefix {
		curr = t.getChild(curr, seg)
		if curr == nil {
			return nil
		}
	}

	var results []V
	t.collectValues(curr, &results)
	return results
}

// CollectAll returns all stored values in the trie.
func (t *PrefixTrie[K, V]) CollectAll() []V {
	for i := range t.shards {
		t.shards[i].RLock()
		defer t.shards[i].RUnlock()
	}

	var results []V
	t.collectValues(t.root, &results)
	return results
}

// InvalidatePrefix prunes the subtree matching prefix in O(1) pointer operations.
func (t *PrefixTrie[K, V]) InvalidatePrefix(prefix ...K) int {
	if len(prefix) == 0 {
		return 0
	}

	shard := t.shardForPath(prefix)
	shard.Lock()
	defer shard.Unlock()

	if len(prefix) == 1 {
		t.root.mu.Lock()
		targetNode, exists := t.root.children[prefix[0]]
		if !exists {
			t.root.mu.Unlock()
			return 0
		}
		delete(t.root.children, prefix[0])
		t.root.mu.Unlock()

		subCount := t.countSubtreeValues(targetNode)
		t.count.Add(-int64(subCount))
		return subCount
	}

	curr := t.root
	for i := 0; i < len(prefix)-1; i++ {
		curr = t.getChild(curr, prefix[i])
		if curr == nil {
			return 0
		}
	}

	lastSeg := prefix[len(prefix)-1]
	curr.mu.Lock()
	targetNode, exists := curr.children[lastSeg]
	if !exists {
		curr.mu.Unlock()
		return 0
	}
	delete(curr.children, lastSeg)
	curr.mu.Unlock()

	subCount := t.countSubtreeValues(targetNode)
	t.count.Add(-int64(subCount))
	return subCount
}

// DetachPrefix removes the subtree at prefix in O(1) and returns it as a standalone PrefixTrie.
func (t *PrefixTrie[K, V]) DetachPrefix(prefix ...K) *PrefixTrie[K, V] {
	detachedStore := NewGenericPrefixTrieWithShards[K, V](DefaultShards)
	if len(prefix) == 0 {
		return detachedStore
	}

	shard := t.shardForPath(prefix)
	shard.Lock()
	var targetNode *trieNode[K, V]

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
				shard.Unlock()
				return detachedStore
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
	shard.Unlock()

	if targetNode == nil {
		return detachedStore
	}

	subCount := t.countSubtreeValues(targetNode)
	t.count.Add(-int64(subCount))

	detachedCurr := detachedStore.root
	for _, seg := range prefix[:len(prefix)-1] {
		detachedCurr = detachedStore.getOrCreateChild(detachedCurr, seg)
	}
	detachedCurr.mu.Lock()
	detachedCurr.children[prefix[len(prefix)-1]] = targetNode
	detachedCurr.mu.Unlock()

	detachedStore.count.Store(int64(subCount))
	return detachedStore
}

// Delete removes a specific leaf path from the trie.
func (t *PrefixTrie[K, V]) Delete(path []K) bool {
	shard := t.shardForPath(path)
	shard.Lock()
	defer shard.Unlock()

	stack := []*trieNode[K, V]{t.root}
	curr := t.root

	for _, seg := range path {
		curr = t.getChild(curr, seg)
		if curr == nil {
			return false
		}
		stack = append(stack, curr)
	}

	curr.mu.Lock()
	if curr.value == nil {
		curr.mu.Unlock()
		return false
	}
	curr.value = nil
	t.count.Add(-1)
	curr.mu.Unlock()

	for i := len(path) - 1; i >= 0; i-- {
		parent := stack[i]
		seg := path[i]

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

	return true
}

// Count returns the active count of items.
func (t *PrefixTrie[K, V]) Count() int {
	return int(t.count.Load())
}

// Close resets the trie.
func (t *PrefixTrie[K, V]) Close() {
	if t == nil || t.root == nil {
		return
	}
	t.root = newGenericTrieNode[K, V]()
	t.count.Store(0)
}

func (t *PrefixTrie[K, V]) collectValues(curr *trieNode[K, V], out *[]V) {
	if curr == nil {
		return
	}
	curr.mu.RLock()
	if curr.value != nil {
		*out = append(*out, *curr.value)
	}
	children := make([]*trieNode[K, V], 0, len(curr.children))
	for _, child := range curr.children {
		children = append(children, child)
	}
	curr.mu.RUnlock()

	for _, child := range children {
		t.collectValues(child, out)
	}
}

func (t *PrefixTrie[K, V]) countSubtreeValues(curr *trieNode[K, V]) int {
	if curr == nil {
		return 0
	}
	curr.mu.RLock()
	c := 0
	if curr.value != nil {
		c++
	}
	children := make([]*trieNode[K, V], 0, len(curr.children))
	for _, child := range curr.children {
		children = append(children, child)
	}
	curr.mu.RUnlock()

	for _, child := range children {
		c += t.countSubtreeValues(child)
	}
	return c
}
