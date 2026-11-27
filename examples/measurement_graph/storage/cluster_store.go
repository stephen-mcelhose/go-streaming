package storage

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
)

// ClusterNode represents an isolated physical or logical worker node hosting a subset of shards.
// It implements the standard GraphStore interface. In a distributed deployment, this can be
// backed by a gRPC client transport, an in-process daemon, or an IPC socket.
type ClusterNode interface {
	GraphStore

	// NodeID returns the unique identifier for this physical / logical worker node.
	NodeID() string

	// AssignedShards returns the list of shard indices owned by this node.
	AssignedShards() []int

	// InvalidatePrefix prunes matching subtrees across all local shards on this node.
	InvalidatePrefix(ctx context.Context, prefix ...string) error
}

// LocalClusterNode is an in-process implementation of ClusterNode hosting one or more DualIndexShard instances.
type LocalClusterNode struct {
	nodeID      string
	totalShards int
	shards      map[int]*DualIndexShard
	shardList   []int
	mu          sync.RWMutex
}

// NewLocalClusterNode creates a ClusterNode hosting the specified shard indices out of totalShards.
func NewLocalClusterNode(nodeID string, totalShards int, shardIndices []int) *LocalClusterNode {
	if totalShards <= 0 {
		totalShards = DefaultShards
	}
	shards := make(map[int]*DualIndexShard, len(shardIndices))
	for _, idx := range shardIndices {
		shards[idx] = newDualIndexShard()
	}
	return &LocalClusterNode{
		nodeID:      nodeID,
		totalShards: totalShards,
		shards:      shards,
		shardList:   shardIndices,
	}
}

// NodeID returns the node's unique identifier.
func (n *LocalClusterNode) NodeID() string {
	return n.nodeID
}

// AssignedShards returns the slice of shard indices hosted on this node.
func (n *LocalClusterNode) AssignedShards() []int {
	return n.shardList
}

// shardForEntity finds the local shard for an entity if hosted here.
func (n *LocalClusterNode) shardForEntity(entity EntityIdentity) (*DualIndexShard, bool) {
	h := fnv.New32a()
	_, _ = h.Write([]byte(entity.BusinessUnit))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(entity.EntityType))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(entity.EntityID))

	shardIdx := int(h.Sum32()) % n.totalShards
	shard, exists := n.shards[shardIdx]
	return shard, exists
}

// Put places an observation into the node's local shard.
func (n *LocalClusterNode) Put(ctx context.Context, m StoredMeasurement) error {
	shard, exists := n.shardForEntity(m.Node.Entity)
	if !exists {
		return fmt.Errorf("node %s does not host shard for entity %s", n.nodeID, m.Node.Entity.String())
	}
	return shard.Put(ctx, m)
}

// Get fetches an exact measurement.
func (n *LocalClusterNode) Get(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error) {
	shard, exists := n.shardForEntity(node.Entity)
	if !exists {
		return StoredMeasurement{}, false, nil
	}
	return shard.Get(ctx, node)
}

// GetByEntity collects all measurements for an entity.
func (n *LocalClusterNode) GetByEntity(ctx context.Context, entity EntityIdentity) ([]StoredMeasurement, error) {
	shard, exists := n.shardForEntity(entity)
	if !exists {
		return nil, nil
	}
	return shard.GetByEntity(ctx, entity)
}

// GetByClass collects all measurements for an entity matching classID.
func (n *LocalClusterNode) GetByClass(ctx context.Context, entity EntityIdentity, classID string) ([]StoredMeasurement, error) {
	shard, exists := n.shardForEntity(entity)
	if !exists {
		return nil, nil
	}
	return shard.GetByClass(ctx, entity, classID)
}

// InvalidateEntity prunes an entity's working memory.
func (n *LocalClusterNode) InvalidateEntity(ctx context.Context, entity EntityIdentity) error {
	shard, exists := n.shardForEntity(entity)
	if !exists {
		return nil
	}
	return shard.InvalidateEntity(ctx, entity)
}

// Retract removes an observation coordinate.
func (n *LocalClusterNode) Retract(ctx context.Context, node NodeCoordinate) error {
	shard, exists := n.shardForEntity(node.Entity)
	if !exists {
		return nil
	}
	return shard.Retract(ctx, node)
}

// InvalidatePrefix prunes matching subtrees across all local shards on this node.
func (n *LocalClusterNode) InvalidatePrefix(ctx context.Context, prefix ...string) error {
	for _, shard := range n.shards {
		if _, err := shard.invalidatePrefixFleet(ctx, prefix...); err != nil {
			return err
		}
	}
	return nil
}

// All collects all stored measurements across this node's shards.
func (n *LocalClusterNode) All(ctx context.Context) ([]StoredMeasurement, error) {
	var results []StoredMeasurement
	for _, shard := range n.shards {
		items, err := shard.all(ctx)
		if err != nil {
			return nil, err
		}
		results = append(results, items...)
	}
	return results, nil
}

// Count returns the aggregate count on this node.
func (n *LocalClusterNode) Count(ctx context.Context) (int, error) {
	total := 0
	for _, shard := range n.shards {
		c, err := shard.count(ctx)
		if err != nil {
			return 0, err
		}
		total += c
	}
	return total, nil
}

// Close resets and cleans up all local shards.
func (n *LocalClusterNode) Close() error {
	for _, shard := range n.shards {
		if err := shard.close(); err != nil {
			return err
		}
	}
	return nil
}

// ClusterRouter coordinates a cluster of independent worker nodes using Entity Affinity Sharding.
// Entity-scoped operations route in O(1) to the exact owning worker node over network/IPC.
// Fleet invalidations execute parallel scatter-gather across all nodes.
type ClusterRouter struct {
	totalShards int
	shardToNode []ClusterNode
	nodes       []ClusterNode
	mu          sync.RWMutex
}

// NewClusterRouter constructs a ClusterRouter with totalShards partitioned across nodes.
func NewClusterRouter(totalShards int, nodes []ClusterNode) (*ClusterRouter, error) {
	if totalShards <= 0 {
		totalShards = DefaultShards
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("cluster router requires at least one cluster node")
	}

	shardToNode := make([]ClusterNode, totalShards)
	assignedCount := 0

	// Map each shard to its designated node
	for _, node := range nodes {
		for _, shardIdx := range node.AssignedShards() {
			if shardIdx < 0 || shardIdx >= totalShards {
				return nil, fmt.Errorf("node %s assigned invalid shard index %d (total: %d)", node.NodeID(), shardIdx, totalShards)
			}
			if shardToNode[shardIdx] != nil {
				return nil, fmt.Errorf("shard index %d multiply assigned (to %s and %s)", shardIdx, shardToNode[shardIdx].NodeID(), node.NodeID())
			}
			shardToNode[shardIdx] = node
			assignedCount++
		}
	}

	if assignedCount != totalShards {
		return nil, fmt.Errorf("incomplete shard assignment: %d of %d shards assigned across nodes", assignedCount, totalShards)
	}

	return &ClusterRouter{
		totalShards: totalShards,
		shardToNode: shardToNode,
		nodes:       nodes,
	}, nil
}

// nodeForEntity calculates the target ClusterNode for an EntityIdentity via FNV-1a.
func (r *ClusterRouter) nodeForEntity(entity EntityIdentity) ClusterNode {
	h := fnv.New32a()
	_, _ = h.Write([]byte(entity.BusinessUnit))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(entity.EntityType))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(entity.EntityID))

	shardIdx := int(h.Sum32()) % r.totalShards
	return r.shardToNode[shardIdx]
}

// Put routes observation to the owning worker node in O(1).
func (r *ClusterRouter) Put(ctx context.Context, m StoredMeasurement) error {
	return r.nodeForEntity(m.Node.Entity).Put(ctx, m)
}

// Get fetches observation from the owning worker node in O(1).
func (r *ClusterRouter) Get(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error) {
	return r.nodeForEntity(node.Entity).Get(ctx, node)
}

// GetByEntity fetches working memory from the owning worker node in O(1).
func (r *ClusterRouter) GetByEntity(ctx context.Context, entity EntityIdentity) ([]StoredMeasurement, error) {
	return r.nodeForEntity(entity).GetByEntity(ctx, entity)
}

// GetByClass fetches class slot facts from the owning worker node in O(1).
func (r *ClusterRouter) GetByClass(ctx context.Context, entity EntityIdentity, classID string) ([]StoredMeasurement, error) {
	return r.nodeForEntity(entity).GetByClass(ctx, entity, classID)
}

// InvalidateEntity prunes working memory on the owning worker node in O(1).
func (r *ClusterRouter) InvalidateEntity(ctx context.Context, entity EntityIdentity) error {
	return r.nodeForEntity(entity).InvalidateEntity(ctx, entity)
}

// Retract removes a specific coordinate on the owning worker node in O(1).
func (r *ClusterRouter) Retract(ctx context.Context, node NodeCoordinate) error {
	return r.nodeForEntity(node.Entity).Retract(ctx, node)
}

// InvalidatePrefix executes parallel scatter-gather invalidation across all worker nodes.
func (r *ClusterRouter) InvalidatePrefix(ctx context.Context, prefix ...string) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(r.nodes))

	for _, node := range r.nodes {
		wg.Add(1)
		go func(n ClusterNode) {
			defer wg.Done()
			if err := n.InvalidatePrefix(ctx, prefix...); err != nil {
				errCh <- err
			}
		}(node)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

// All collects observations across all worker nodes via scatter-gather.
func (r *ClusterRouter) All(ctx context.Context) ([]StoredMeasurement, error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var all []StoredMeasurement
	errCh := make(chan error, len(r.nodes))

	for _, node := range r.nodes {
		wg.Add(1)
		go func(n ClusterNode) {
			defer wg.Done()
			items, err := n.All(ctx)
			if err != nil {
				errCh <- err
				return
			}
			mu.Lock()
			all = append(all, items...)
			mu.Unlock()
		}(node)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return nil, err
		}
	}
	return all, nil
}

// Count aggregates count across all worker nodes via scatter-gather.
func (r *ClusterRouter) Count(ctx context.Context) (int, error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	total := 0
	errCh := make(chan error, len(r.nodes))

	for _, node := range r.nodes {
		wg.Add(1)
		go func(n ClusterNode) {
			defer wg.Done()
			c, err := n.Count(ctx)
			if err != nil {
				errCh <- err
				return
			}
			mu.Lock()
			total += c
			mu.Unlock()
		}(node)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

// Close gracefully closes all worker nodes.
func (r *ClusterRouter) Close() error {
	for _, node := range r.nodes {
		if err := node.Close(); err != nil {
			return err
		}
	}
	return nil
}
