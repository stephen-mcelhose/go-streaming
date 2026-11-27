package storage_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming/examples/measurement_graph/storage"
)

type fakeGraphStore struct {
	putErr              error
	getErr              error
	getByEntityErr      error
	getByClassErr       error
	invalidateEntityErr error
	retractErr          error
	closeErr            error

	putCalls              int
	getCalls              int
	getByEntityCalls      int
	getByClassCalls       int
	invalidateEntityCalls int
	retractCalls          int
	closeCalls            int

	records map[string]storage.StoredMeasurement
}

func newFakeGraphStore() *fakeGraphStore {
	return &fakeGraphStore{
		records: make(map[string]storage.StoredMeasurement),
	}
}

func (f *fakeGraphStore) Put(ctx context.Context, m storage.StoredMeasurement) error {
	f.putCalls++
	if f.putErr != nil {
		return f.putErr
	}
	f.records[m.Node.String()] = m
	return nil
}

func (f *fakeGraphStore) Get(ctx context.Context, node storage.NodeCoordinate) (storage.StoredMeasurement, bool, error) {
	f.getCalls++
	if f.getErr != nil {
		return storage.StoredMeasurement{}, false, f.getErr
	}
	m, ok := f.records[node.String()]
	return m, ok, nil
}

func (f *fakeGraphStore) GetByEntity(ctx context.Context, entity storage.EntityIdentity) ([]storage.StoredMeasurement, error) {
	f.getByEntityCalls++
	if f.getByEntityErr != nil {
		return nil, f.getByEntityErr
	}
	var res []storage.StoredMeasurement
	for _, m := range f.records {
		if m.Node.Entity == entity {
			res = append(res, m)
		}
	}
	return res, nil
}

func (f *fakeGraphStore) GetByClass(ctx context.Context, entity storage.EntityIdentity, classID string) ([]storage.StoredMeasurement, error) {
	f.getByClassCalls++
	if f.getByClassErr != nil {
		return nil, f.getByClassErr
	}
	var res []storage.StoredMeasurement
	for _, m := range f.records {
		if m.Node.Entity == entity && m.Node.ClassID == classID {
			res = append(res, m)
		}
	}
	return res, nil
}

func (f *fakeGraphStore) InvalidateEntity(ctx context.Context, entity storage.EntityIdentity) error {
	f.invalidateEntityCalls++
	if f.invalidateEntityErr != nil {
		return f.invalidateEntityErr
	}
	for k, m := range f.records {
		if m.Node.Entity == entity {
			delete(f.records, k)
		}
	}
	return nil
}

func (f *fakeGraphStore) Retract(ctx context.Context, node storage.NodeCoordinate) error {
	f.retractCalls++
	if f.retractErr != nil {
		return f.retractErr
	}
	delete(f.records, node.String())
	return nil
}

func (f *fakeGraphStore) All(ctx context.Context) ([]storage.StoredMeasurement, error) {
	var res []storage.StoredMeasurement
	for _, m := range f.records {
		res = append(res, m)
	}
	return res, nil
}

func (f *fakeGraphStore) Count(ctx context.Context) (int, error) {
	return len(f.records), nil
}

func (f *fakeGraphStore) Close() error {
	f.closeCalls++
	return f.closeErr
}

func TestTrieCache_Put_Spec(t *testing.T) {
	ctx := context.Background()
	entity := storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_spec_01"}
	node := storage.NodeCoordinate{Entity: entity, PrimaryCode: "GRAIN_YIELD", ClassID: "CLASS_YIELD", InstanceID: "I1"}
	meas := storage.StoredMeasurement{Node: node, Value: 150.0, Timestamp: time.Now()}

	type testCase struct {
		name              string
		setupL2           func() *fakeGraphStore
		measurement       storage.StoredMeasurement
		wantErr           error
		assertSideEffects func(t *testing.T, l2 *fakeGraphStore, cache *storage.TrieCache)
	}

	testCases := []testCase{
		{
			name: "MUST write to L1 and return immediately when L2 is nil",
			setupL2: func() *fakeGraphStore {
				return nil
			},
			measurement: meas,
			wantErr:     nil,
			assertSideEffects: func(t *testing.T, l2 *fakeGraphStore, cache *storage.TrieCache) {
				got, found, err := cache.Get(ctx, node)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, 150.0, got.Value)
			},
		},
		{
			name: "MUST propagate write synchronously to L2 when L2 is configured",
			setupL2: func() *fakeGraphStore {
				return newFakeGraphStore()
			},
			measurement: meas,
			wantErr:     nil,
			assertSideEffects: func(t *testing.T, l2 *fakeGraphStore, cache *storage.TrieCache) {
				require.Equal(t, 1, l2.putCalls)
				require.Contains(t, l2.records, node.String())
				require.Equal(t, 150.0, l2.records[node.String()].Value)
			},
		},
		{
			name: "MUST fail-fast and propagate L2 write error to caller",
			setupL2: func() *fakeGraphStore {
				l2 := newFakeGraphStore()
				l2.putErr = errors.New("db disk write failure")
				return l2
			},
			measurement: meas,
			wantErr:     errors.New("db disk write failure"),
			assertSideEffects: func(t *testing.T, l2 *fakeGraphStore, cache *storage.TrieCache) {
				require.Equal(t, 1, l2.putCalls)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var l2 *fakeGraphStore
			var cache *storage.TrieCache
			if tc.setupL2 != nil {
				l2 = tc.setupL2()
			}
			if l2 == nil {
				cache = storage.NewTrieCache(nil)
			} else {
				cache = storage.NewTrieCache(l2)
			}
			defer cache.Close()

			err := cache.Put(ctx, tc.measurement)
			if tc.wantErr != nil {
				require.Error(t, err)
				require.Equal(t, tc.wantErr.Error(), err.Error())
				return
			}
			require.NoError(t, err)

			if tc.assertSideEffects != nil {
				tc.assertSideEffects(t, l2, cache)
			}
		})
	}
}

func TestTrieCache_Get_Spec(t *testing.T) {
	ctx := context.Background()
	entity := storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_spec_02"}
	node := storage.NodeCoordinate{Entity: entity, PrimaryCode: "GRAIN_MOISTURE", ClassID: "CLASS_MOISTURE", InstanceID: "I2"}
	meas := storage.StoredMeasurement{Node: node, Value: 14.2, Timestamp: time.Now()}

	type testCase struct {
		name              string
		setupCache        func() (*storage.TrieCache, *fakeGraphStore)
		node              storage.NodeCoordinate
		wantFound         bool
		wantVal           float64
		wantErr           error
		assertSideEffects func(t *testing.T, l2 *fakeGraphStore, cache *storage.TrieCache)
	}

	testCases := []testCase{
		{
			name: "MUST serve from L1 hot memory without querying L2 on L1 hit",
			setupCache: func() (*storage.TrieCache, *fakeGraphStore) {
				l2 := newFakeGraphStore()
				cache := storage.NewTrieCache(l2)
				_ = cache.Put(ctx, meas)
				l2.getCalls = 0 // reset counter
				return cache, l2
			},
			node:      node,
			wantFound: true,
			wantVal:   14.2,
			wantErr:   nil,
			assertSideEffects: func(t *testing.T, l2 *fakeGraphStore, cache *storage.TrieCache) {
				require.Equal(t, 0, l2.getCalls, "L2 MUST NOT be called on L1 hit")
			},
		},
		{
			name: "MUST return not found and zero value when missing in L1 and L2 is nil",
			setupCache: func() (*storage.TrieCache, *fakeGraphStore) {
				return storage.NewTrieCache(nil), nil
			},
			node:      node,
			wantFound: false,
			wantVal:   0.0,
			wantErr:   nil,
		},
		{
			name: "MUST fallback to L2 on L1 miss and populate L1 on L2 hit",
			setupCache: func() (*storage.TrieCache, *fakeGraphStore) {
				l2 := newFakeGraphStore()
				l2.records[node.String()] = meas
				return storage.NewTrieCache(l2), l2
			},
			node:      node,
			wantFound: true,
			wantVal:   14.2,
			wantErr:   nil,
			assertSideEffects: func(t *testing.T, l2 *fakeGraphStore, cache *storage.TrieCache) {
				require.Equal(t, 1, l2.getCalls)
				// L1 must now contain the populated value; subsequent call shouldn't touch L2
				l2.getCalls = 0
				got, found, err := cache.Get(ctx, node)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, 14.2, got.Value)
				require.Equal(t, 0, l2.getCalls, "Second read MUST hit populated L1 cache")
			},
		},
		{
			name: "MUST propagate L2 query error when L2 lookup fails",
			setupCache: func() (*storage.TrieCache, *fakeGraphStore) {
				l2 := newFakeGraphStore()
				l2.getErr = errors.New("l2 connection timeout")
				return storage.NewTrieCache(l2), l2
			},
			node:      node,
			wantFound: false,
			wantVal:   0.0,
			wantErr:   errors.New("l2 connection timeout"),
			assertSideEffects: func(t *testing.T, l2 *fakeGraphStore, cache *storage.TrieCache) {
				require.Equal(t, 1, l2.getCalls)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache, l2 := tc.setupCache()
			defer cache.Close()

			got, found, err := cache.Get(ctx, tc.node)
			if tc.wantErr != nil {
				require.Error(t, err)
				require.Equal(t, tc.wantErr.Error(), err.Error())
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantFound, found)
			if found {
				require.Equal(t, tc.wantVal, got.Value)
			}

			if tc.assertSideEffects != nil {
				tc.assertSideEffects(t, l2, cache)
			}
		})
	}
}

func TestTrieCache_SubtreeAndClassQueries_Spec(t *testing.T) {
	ctx := context.Background()
	entity := storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_spec_03"}
	node1 := storage.NodeCoordinate{Entity: entity, PrimaryCode: "GRAIN_YIELD", ClassID: "CLASS_YIELD", InstanceID: "I1"}
	node2 := storage.NodeCoordinate{Entity: entity, PrimaryCode: "GRAIN_MOISTURE", ClassID: "CLASS_MOISTURE", InstanceID: "I2"}
	meas1 := storage.StoredMeasurement{Node: node1, Value: 200.0, Timestamp: time.Now()}
	meas2 := storage.StoredMeasurement{Node: node2, Value: 15.0, Timestamp: time.Now()}

	type testCase struct {
		name              string
		setupCache        func() (*storage.TrieCache, *fakeGraphStore)
		queryType         string // "entity" or "class"
		targetClass       string
		wantCount         int
		wantErr           error
		assertSideEffects func(t *testing.T, l2 *fakeGraphStore, cache *storage.TrieCache)
	}

	testCases := []testCase{
		{
			name: "GetByEntity MUST serve from L1 when hot and avoid L2 call",
			setupCache: func() (*storage.TrieCache, *fakeGraphStore) {
				l2 := newFakeGraphStore()
				cache := storage.NewTrieCache(l2)
				_ = cache.Put(ctx, meas1)
				_ = cache.Put(ctx, meas2)
				l2.getByEntityCalls = 0
				return cache, l2
			},
			queryType: "entity",
			wantCount: 2,
			wantErr:   nil,
			assertSideEffects: func(t *testing.T, l2 *fakeGraphStore, cache *storage.TrieCache) {
				require.Equal(t, 0, l2.getByEntityCalls, "L2 MUST NOT be queried on L1 entity hit")
			},
		},
		{
			name: "GetByEntity MUST fallback to L2 on cold miss and populate L1",
			setupCache: func() (*storage.TrieCache, *fakeGraphStore) {
				l2 := newFakeGraphStore()
				l2.records[node1.String()] = meas1
				l2.records[node2.String()] = meas2
				return storage.NewTrieCache(l2), l2
			},
			queryType: "entity",
			wantCount: 2,
			wantErr:   nil,
			assertSideEffects: func(t *testing.T, l2 *fakeGraphStore, cache *storage.TrieCache) {
				require.Equal(t, 1, l2.getByEntityCalls)
				// L1 is now primed; subsequent GetByEntity query MUST NOT call L2
				l2.getByEntityCalls = 0
				res, err := cache.GetByEntity(ctx, entity)
				require.NoError(t, err)
				require.Len(t, res, 2)
				require.Equal(t, 0, l2.getByEntityCalls)
			},
		},
		{
			name: "GetByClass MUST serve from L1 when hot and avoid L2 call",
			setupCache: func() (*storage.TrieCache, *fakeGraphStore) {
				l2 := newFakeGraphStore()
				cache := storage.NewTrieCache(l2)
				_ = cache.Put(ctx, meas1)
				l2.getByClassCalls = 0
				return cache, l2
			},
			queryType:   "class",
			targetClass: "CLASS_YIELD",
			wantCount:   1,
			wantErr:     nil,
			assertSideEffects: func(t *testing.T, l2 *fakeGraphStore, cache *storage.TrieCache) {
				require.Equal(t, 0, l2.getByClassCalls)
			},
		},
		{
			name: "GetByClass MUST fallback to L2 on cold miss and populate L1",
			setupCache: func() (*storage.TrieCache, *fakeGraphStore) {
				l2 := newFakeGraphStore()
				l2.records[node1.String()] = meas1
				return storage.NewTrieCache(l2), l2
			},
			queryType:   "class",
			targetClass: "CLASS_YIELD",
			wantCount:   1,
			wantErr:     nil,
			assertSideEffects: func(t *testing.T, l2 *fakeGraphStore, cache *storage.TrieCache) {
				require.Equal(t, 1, l2.getByClassCalls)
				l2.getByClassCalls = 0
				res, err := cache.GetByClass(ctx, entity, "CLASS_YIELD")
				require.NoError(t, err)
				require.Len(t, res, 1)
				require.Equal(t, 0, l2.getByClassCalls)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache, l2 := tc.setupCache()
			defer cache.Close()

			var (
				got []storage.StoredMeasurement
				err error
			)
			if tc.queryType == "entity" {
				got, err = cache.GetByEntity(ctx, entity)
			} else {
				got, err = cache.GetByClass(ctx, entity, tc.targetClass)
			}

			if tc.wantErr != nil {
				require.Error(t, err)
				require.Equal(t, tc.wantErr.Error(), err.Error())
				return
			}
			require.NoError(t, err)
			require.Len(t, got, tc.wantCount)

			if tc.assertSideEffects != nil {
				tc.assertSideEffects(t, l2, cache)
			}
		})
	}
}

func TestTrieCache_InvalidationAndClose_Spec(t *testing.T) {
	ctx := context.Background()
	entity := storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_spec_04"}
	node := storage.NodeCoordinate{Entity: entity, PrimaryCode: "GRAIN_YIELD", ClassID: "CLASS_YIELD", InstanceID: "I1"}
	meas := storage.StoredMeasurement{Node: node, Value: 180.0, Timestamp: time.Now()}

	type testCase struct {
		name              string
		setupCache        func() (*storage.TrieCache, *fakeGraphStore)
		action            func(ctx context.Context, cache *storage.TrieCache) error
		wantErr           error
		assertSideEffects func(t *testing.T, l2 *fakeGraphStore, cache *storage.TrieCache)
	}

	testCases := []testCase{
		{
			name: "InvalidateEntity MUST prune L1 hot memory in O(1) and cascade to L2",
			setupCache: func() (*storage.TrieCache, *fakeGraphStore) {
				l2 := newFakeGraphStore()
				cache := storage.NewTrieCache(l2)
				_ = cache.Put(ctx, meas)
				return cache, l2
			},
			action: func(ctx context.Context, cache *storage.TrieCache) error {
				return cache.InvalidateEntity(ctx, entity)
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, l2 *fakeGraphStore, cache *storage.TrieCache) {
				require.Equal(t, 1, l2.invalidateEntityCalls)
				require.Empty(t, l2.records)

				// L1 check
				_, found, err := cache.Get(ctx, node)
				require.NoError(t, err)
				require.False(t, found)
			},
		},
		{
			name: "Retract MUST remove coordinate from L1 and cascade to L2",
			setupCache: func() (*storage.TrieCache, *fakeGraphStore) {
				l2 := newFakeGraphStore()
				cache := storage.NewTrieCache(l2)
				_ = cache.Put(ctx, meas)
				return cache, l2
			},
			action: func(ctx context.Context, cache *storage.TrieCache) error {
				return cache.Retract(ctx, node)
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, l2 *fakeGraphStore, cache *storage.TrieCache) {
				require.Equal(t, 1, l2.retractCalls)
				require.Empty(t, l2.records)

				_, found, err := cache.Get(ctx, node)
				require.NoError(t, err)
				require.False(t, found)
			},
		},
		{
			name: "Close MUST clean up L1 and cascade Close to L2",
			setupCache: func() (*storage.TrieCache, *fakeGraphStore) {
				l2 := newFakeGraphStore()
				return storage.NewTrieCache(l2), l2
			},
			action: func(ctx context.Context, cache *storage.TrieCache) error {
				return cache.Close()
			},
			wantErr: nil,
			assertSideEffects: func(t *testing.T, l2 *fakeGraphStore, cache *storage.TrieCache) {
				require.Equal(t, 1, l2.closeCalls)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache, l2 := tc.setupCache()

			err := tc.action(ctx, cache)
			if tc.wantErr != nil {
				require.Error(t, err)
				require.Equal(t, tc.wantErr.Error(), err.Error())
				return
			}
			require.NoError(t, err)

			if tc.assertSideEffects != nil {
				tc.assertSideEffects(t, l2, cache)
			}
		})
	}
}
