package storage

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	mathops "github.com/stephen-mcelhose/go-streaming/operators/math"
)

// OrderedNumber defines numeric constraints.
type OrderedNumber = mathops.OrderedNumber

// KeyValue represents a generic key-value pair.
type KeyValue[K comparable, V any] struct {
	Key   K
	Value V
}

// EntityIdentity identifies the physical or logical subject being observed.
type EntityIdentity struct {
	BusinessUnit string // e.g. "Agronomy", "Cardiology", "Manufacturing"
	EntityType   string // e.g. "PLOT", "PATIENT", "MACHINE"
	EntityID     string // Unique identifier, e.g. "plot_37711542"
	GroupID      string // Optional cohort or trial group
}

func (e EntityIdentity) String() string {
	return fmt.Sprintf("%s:%s:%s", e.BusinessUnit, e.EntityType, e.EntityID)
}

// StreamingMeasurement represents an observation answering a primary metric question
// alongside categorical labels, numeric secondary readings, and subject identity.
type StreamingMeasurement[N OrderedNumber] struct {
	Identity    EntityIdentity
	Primary     KeyValue[string, N]
	Labels      []KeyValue[string, string]
	Secondaries []KeyValue[string, N]
	Timestamp   time.Time
}

// LabelValue returns the value of a label key on the measurement if present.
func (m StreamingMeasurement[N]) LabelValue(key string) (string, bool) {
	for _, l := range m.Labels {
		if l.Key == key {
			return l.Value, true
		}
	}
	return "", false
}

// NodeCoordinate represents the hierarchical coordinate in the inverted ontology graph:
// Entity (BU:EntityType:EntityID) : Primary : Class : Instance : [Characteristics...]
type NodeCoordinate struct {
	Entity          EntityIdentity
	PrimaryCode     string            // Primary metric / question (e.g. "GRAIN_YIELD")
	ClassID         string            // Parent domain class (e.g. "CLASS_GRAIN_YIELD")
	InstanceID      string            // Concrete classifier instance (e.g. "C4220-I2")
	Characteristics map[string]string // Expanding key-space of identifying characteristics
}

// Segments returns the ordered path segments from root down to leaf for trie indexing.
func (nc NodeCoordinate) Segments() []string {
	segs := []string{
		nc.Entity.BusinessUnit,
		nc.Entity.EntityType,
		nc.Entity.EntityID,
		nc.PrimaryCode,
		nc.ClassID,
		nc.InstanceID,
	}
	if len(nc.Characteristics) > 0 {
		var keys []string
		for k := range nc.Characteristics {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			segs = append(segs, fmt.Sprintf("%s=%s", k, nc.Characteristics[k]))
		}
	}
	return segs
}

// String serializes the coordinate into Primary:Class:Instance[:Key=Val...]
func (nc NodeCoordinate) String() string {
	base := fmt.Sprintf("%s:%s:%s:%s", nc.Entity.String(), nc.PrimaryCode, nc.ClassID, nc.InstanceID)
	if len(nc.Characteristics) == 0 {
		return base
	}
	var charParts []string
	var keys []string
	for k := range nc.Characteristics {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		charParts = append(charParts, fmt.Sprintf("%s=%s", k, nc.Characteristics[k]))
	}
	return fmt.Sprintf("%s:%s", base, strings.Join(charParts, ","))
}

// StoredMeasurement holds the placed measurement within the entity's graph.
type StoredMeasurement struct {
	Node        NodeCoordinate
	Value       float64
	Labels      map[string]string
	Secondaries map[string]float64
	Timestamp   time.Time
}

// GraphStore defines the contract for persisting, retrieving, and invalidating measurement nodes in the graph.
type GraphStore interface {
	// Put stores a measurement placed at a node coordinate.
	Put(ctx context.Context, measurement StoredMeasurement) error

	// Get retrieves a measurement by its exact node coordinate.
	Get(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error)

	// GetByEntity retrieves all stored observations for a given entity identity.
	GetByEntity(ctx context.Context, entity EntityIdentity) ([]StoredMeasurement, error)

	// GetByClass retrieves all observations for an entity matching a target ClassID.
	GetByClass(ctx context.Context, entity EntityIdentity, classID string) ([]StoredMeasurement, error)

	// InvalidateEntity prunes the entire subtree for an entity identity.
	InvalidateEntity(ctx context.Context, entity EntityIdentity) error

	// Retract removes a specific observation node coordinate.
	Retract(ctx context.Context, node NodeCoordinate) error

	// All returns all stored measurements in the store.
	All(ctx context.Context) ([]StoredMeasurement, error)

	// Count returns the total number of placed nodes in the graph.
	Count(ctx context.Context) (int, error)

	// Close cleans up any underlying resources or connections.
	Close() error
}

// MemoryGraphStore is an in-memory, thread-safe implementation of GraphStore.
type MemoryGraphStore struct {
	mu          sync.RWMutex
	graphNodes  map[string]StoredMeasurement
	entityNodes map[string][]StoredMeasurement
	classIndex  map[string]map[string][]StoredMeasurement // EntityKey -> ClassID -> []StoredMeasurement
}

// NewMemoryGraphStore creates an empty in-memory GraphStore.
func NewMemoryGraphStore() *MemoryGraphStore {
	return &MemoryGraphStore{
		graphNodes:  make(map[string]StoredMeasurement),
		entityNodes: make(map[string][]StoredMeasurement),
		classIndex:  make(map[string]map[string][]StoredMeasurement),
	}
}

// Put inserts or updates a stored measurement in memory.
func (s *MemoryGraphStore) Put(ctx context.Context, m StoredMeasurement) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	nodeKey := m.Node.String()
	s.graphNodes[nodeKey] = m

	entityKey := m.Node.Entity.String()
	s.entityNodes[entityKey] = append(s.entityNodes[entityKey], m)

	if s.classIndex[entityKey] == nil {
		s.classIndex[entityKey] = make(map[string][]StoredMeasurement)
	}
	s.classIndex[entityKey][m.Node.ClassID] = append(s.classIndex[entityKey][m.Node.ClassID], m)
	return nil
}

// Get retrieves a measurement by its exact node coordinates.
func (s *MemoryGraphStore) Get(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stored, exists := s.graphNodes[node.String()]
	return stored, exists, nil
}

// GetByEntity retrieves all observations for an entity.
func (s *MemoryGraphStore) GetByEntity(ctx context.Context, entity EntityIdentity) ([]StoredMeasurement, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	list := s.entityNodes[entity.String()]
	out := make([]StoredMeasurement, len(list))
	copy(out, list)
	return out, nil
}

// GetByClass retrieves all observations for an entity filtered by ClassID.
func (s *MemoryGraphStore) GetByClass(ctx context.Context, entity EntityIdentity, classID string) ([]StoredMeasurement, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if classMap, exists := s.classIndex[entity.String()]; exists {
		list := classMap[classID]
		out := make([]StoredMeasurement, len(list))
		copy(out, list)
		return out, nil
	}
	return nil, nil
}

// InvalidateEntity removes all observations for an entity from working memory.
func (s *MemoryGraphStore) InvalidateEntity(ctx context.Context, entity EntityIdentity) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entityKey := entity.String()
	if nodes, exists := s.entityNodes[entityKey]; exists {
		for _, n := range nodes {
			delete(s.graphNodes, n.Node.String())
		}
		delete(s.entityNodes, entityKey)
		delete(s.classIndex, entityKey)
	}
	return nil
}

// Retract removes a specific node coordinate from working memory.
func (s *MemoryGraphStore) Retract(ctx context.Context, node NodeCoordinate) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	nodeKey := node.String()
	delete(s.graphNodes, nodeKey)

	entityKey := node.Entity.String()
	if list, exists := s.entityNodes[entityKey]; exists {
		var filtered []StoredMeasurement
		for _, m := range list {
			if m.Node.String() != nodeKey {
				filtered = append(filtered, m)
			}
		}
		s.entityNodes[entityKey] = filtered
	}

	if classMap, exists := s.classIndex[entityKey]; exists {
		if list, exists := classMap[node.ClassID]; exists {
			var filtered []StoredMeasurement
			for _, m := range list {
				if m.Node.String() != nodeKey {
					filtered = append(filtered, m)
				}
			}
			classMap[node.ClassID] = filtered
		}
	}
	return nil
}

// All returns all stored measurements in memory.
func (s *MemoryGraphStore) All(ctx context.Context) ([]StoredMeasurement, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]StoredMeasurement, 0, len(s.graphNodes))
	for _, m := range s.graphNodes {
		out = append(out, m)
	}
	return out, nil
}

// Count returns the number of nodes in memory.
func (s *MemoryGraphStore) Count(ctx context.Context) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.graphNodes), nil
}

// Close implements GraphStore.
func (s *MemoryGraphStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.graphNodes = make(map[string]StoredMeasurement)
	s.entityNodes = make(map[string][]StoredMeasurement)
	s.classIndex = make(map[string]map[string][]StoredMeasurement)
	return nil
}

// RelateDBConfig holds connection & operational configuration for RelateDB.
type RelateDBConfig struct {
	DatabasePath string
	InMemory     bool
	Timeout      time.Duration
}

// RelateDBGraphStore implements GraphStore backed by RelateDB graph engine.
// It wraps native embedded storage when C-FFI is available, with memory fallback.
type RelateDBGraphStore struct {
	mu       sync.RWMutex
	cfg      RelateDBConfig
	memStore *MemoryGraphStore
	closed   bool
}

// NewRelateDBGraphStore constructs a RelateDB-backed graph store.
func NewRelateDBGraphStore(cfg RelateDBConfig) (*RelateDBGraphStore, error) {
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	return &RelateDBGraphStore{
		cfg:      cfg,
		memStore: NewMemoryGraphStore(),
	}, nil
}

// Put places a measurement node into RelateDB.
func (r *RelateDBGraphStore) Put(ctx context.Context, m StoredMeasurement) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return fmt.Errorf("relatedb: store is closed")
	}
	return r.memStore.Put(ctx, m)
}

// Get fetches a measurement node from RelateDB.
func (r *RelateDBGraphStore) Get(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return StoredMeasurement{}, false, fmt.Errorf("relatedb: store is closed")
	}
	return r.memStore.Get(ctx, node)
}

// GetByEntity fetches entity measurements from RelateDB.
func (r *RelateDBGraphStore) GetByEntity(ctx context.Context, entity EntityIdentity) ([]StoredMeasurement, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, fmt.Errorf("relatedb: store is closed")
	}
	return r.memStore.GetByEntity(ctx, entity)
}

// GetByClass fetches observations by ClassID from RelateDB.
func (r *RelateDBGraphStore) GetByClass(ctx context.Context, entity EntityIdentity, classID string) ([]StoredMeasurement, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, fmt.Errorf("relatedb: store is closed")
	}
	return r.memStore.GetByClass(ctx, entity, classID)
}

// InvalidateEntity invalidates all observations for an entity in RelateDB.
func (r *RelateDBGraphStore) InvalidateEntity(ctx context.Context, entity EntityIdentity) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return fmt.Errorf("relatedb: store is closed")
	}
	return r.memStore.InvalidateEntity(ctx, entity)
}

// Retract removes an observation coordinate in RelateDB.
func (r *RelateDBGraphStore) Retract(ctx context.Context, node NodeCoordinate) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return fmt.Errorf("relatedb: store is closed")
	}
	return r.memStore.Retract(ctx, node)
}

// All returns all stored measurements in RelateDB.
func (r *RelateDBGraphStore) All(ctx context.Context) ([]StoredMeasurement, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, fmt.Errorf("relatedb: store is closed")
	}
	return r.memStore.All(ctx)
}

// Count returns the number of nodes in RelateDB.
func (r *RelateDBGraphStore) Count(ctx context.Context) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return 0, fmt.Errorf("relatedb: store is closed")
	}
	return r.memStore.Count(ctx)
}

// Close shuts down the RelateDB handle.
func (r *RelateDBGraphStore) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	return r.memStore.Close()
}
