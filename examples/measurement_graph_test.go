package examples_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	mathops "github.com/stephen-mcelhose/go-streaming/operators/math"
)

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
type StreamingMeasurement[N mathops.OrderedNumber] struct {
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

// InstanceSelector defines a concrete classification rule with closed values and open wildcards.
type InstanceSelector struct {
	InstanceID   string            // e.g. "C4220-I2"
	ClassID      string            // Parent Class ID
	MetricName   string            // Primary metric
	ClosedValues map[string]string // Key=Value exact matches required
	OpenKeys     []string          // Key presence required (any value accepted)
}

// Matches evaluates whether a measurement satisfies this instance selector rule.
func (rule InstanceSelector) Matches(m StreamingMeasurement[float64]) bool {
	for k, expectedVal := range rule.ClosedValues {
		actualVal, exists := m.LabelValue(k)
		if !exists || actualVal != expectedVal {
			return false
		}
	}
	for _, k := range rule.OpenKeys {
		if _, exists := m.LabelValue(k); !exists {
			return false
		}
	}
	return true
}

// NodeSelector is the resolved coordinate identifying a variable slot in a calculation.
// Format: (EntityID, InstanceID, MetricName)
type NodeSelector struct {
	Entity     EntityIdentity
	InstanceID string
	MetricName string
}

func (ns NodeSelector) String() string {
	return fmt.Sprintf("%s:%s:%s", ns.Entity.String(), ns.InstanceID, ns.MetricName)
}

// StoredMeasurement holds the placed measurement within the entity's graph.
type StoredMeasurement struct {
	Node        NodeSelector
	Value       float64
	Secondaries map[string]float64
	Timestamp   time.Time
}

// StatefulCalculation defines a multi-variable computation template triggered when
// all required input instance selectors are present for a given entity.
type StatefulCalculation struct {
	CalculationCode string
	OutputMetric    string
	RequiredInputs  []string // Required InstanceIDs (e.g. ["C4220-I2", "C5343-I1"])
	Compute         func(ctx context.Context, entity EntityIdentity, inputs map[string]StoredMeasurement) (StreamingMeasurement[float64], error)
}

// MeasurementGraphStore is a fast in-memory graph repository, classification engine, and reactive calculation router.
type MeasurementGraphStore struct {
	mu sync.RWMutex

	// Classification Tree Index: MetricName -> []InstanceSelector
	rulesByMetric map[string][]InstanceSelector

	// Entity Observation Graph: NodeSelector.String() -> StoredMeasurement
	graphNodes map[string]StoredMeasurement

	// Entity Node Map: Entity.String() -> map[InstanceID]StoredMeasurement
	entityNodes map[string]map[string]StoredMeasurement

	// Registered Stateful Calculations
	calculations []StatefulCalculation
}

// NewMeasurementGraphStore creates an empty measurement graph store.
func NewMeasurementGraphStore() *MeasurementGraphStore {
	return &MeasurementGraphStore{
		rulesByMetric: make(map[string][]InstanceSelector),
		graphNodes:    make(map[string]StoredMeasurement),
		entityNodes:   make(map[string]map[string]StoredMeasurement),
	}
}

// RegisterInstanceRule registers an instance selector rule into the classification tree.
func (s *MeasurementGraphStore) RegisterInstanceRule(rule InstanceSelector) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rulesByMetric[rule.MetricName] = append(s.rulesByMetric[rule.MetricName], rule)
}

// RegisterCalculation registers a stateful calculation listening for completed node selector sets.
func (s *MeasurementGraphStore) RegisterCalculation(calc StatefulCalculation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calculations = append(s.calculations, calc)
}

// ClassifyAndPlace walks the classification tree to find a matching instance rule.
// If found, it stores the measurement under its resolved NodeSelector in the entity's graph
// and evaluates whether any registered StatefulCalculations can now trigger.
func (s *MeasurementGraphStore) ClassifyAndPlace(ctx context.Context, m StreamingMeasurement[float64]) (*NodeSelector, []StreamingMeasurement[float64], bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	candidates, exists := s.rulesByMetric[m.Primary.Key]
	if !exists {
		return nil, nil, false
	}

	for _, rule := range candidates {
		if rule.Matches(m) {
			node := NodeSelector{
				Entity:     m.Identity,
				InstanceID: rule.InstanceID,
				MetricName: m.Primary.Key,
			}

			secMap := make(map[string]float64, len(m.Secondaries))
			for _, sec := range m.Secondaries {
				secMap[sec.Key] = sec.Value
			}

			stored := StoredMeasurement{
				Node:        node,
				Value:       m.Primary.Value,
				Secondaries: secMap,
				Timestamp:   m.Timestamp,
			}

			s.graphNodes[node.String()] = stored

			entityKey := m.Identity.String()
			if s.entityNodes[entityKey] == nil {
				s.entityNodes[entityKey] = make(map[string]StoredMeasurement)
			}
			s.entityNodes[entityKey][rule.InstanceID] = stored

			// Check and evaluate triggered stateful calculations for this entity
			var triggeredOutputs []StreamingMeasurement[float64]
			entityInstanceMap := s.entityNodes[entityKey]

			for _, calc := range s.calculations {
				allPresent := true
				boundInputs := make(map[string]StoredMeasurement, len(calc.RequiredInputs))
				for _, reqInstance := range calc.RequiredInputs {
					meas, hasInstance := entityInstanceMap[reqInstance]
					if !hasInstance {
						allPresent = false
						break
					}
					boundInputs[reqInstance] = meas
				}

				if allPresent {
					outMeas, err := calc.Compute(ctx, m.Identity, boundInputs)
					if err == nil {
						triggeredOutputs = append(triggeredOutputs, outMeas)
					}
				}
			}

			return &node, triggeredOutputs, true
		}
	}

	return nil, nil, false
}

// GetMeasurement retrieves a stored measurement by its NodeSelector coordinates.
func (s *MeasurementGraphStore) GetMeasurement(node NodeSelector) (StoredMeasurement, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stored, exists := s.graphNodes[node.String()]
	return stored, exists
}

// Count returns the total number of stored measurement nodes in the graph.
func (s *MeasurementGraphStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.graphNodes)
}

// StreamingMeasurementRouter is a reactive stream operator:
// 1. Ingests raw measurements from a producer stream.
// 2. Classifies and binds them to the in-memory graph store.
// 3. Automatically triggers and emits computed output measurements when stateful calculations complete.
func StreamingMeasurementRouter(
	ctx context.Context,
	store *MeasurementGraphStore,
	inProducer streaming.Receiver[StreamingMeasurement[float64]],
	outCalculations streaming.Sender[StreamingMeasurement[float64]],
	outUnmatched streaming.Sender[StreamingMeasurement[float64]],
) error {
	return inProducer.Receive(ctx, func(ctx context.Context, m StreamingMeasurement[float64]) error {
		_, triggeredOutputs, placed := store.ClassifyAndPlace(ctx, m)
		if placed {
			if outCalculations != nil {
				for _, computed := range triggeredOutputs {
					if err := outCalculations.Send(ctx, computed); err != nil {
						return err
					}
				}
			}
			return nil
		}
		if outUnmatched != nil {
			return outUnmatched.Send(ctx, m)
		}
		return nil
	})
}

func TestMeasurementGraphClassification(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST classify and store measurements into entity node selectors", func(t *testing.T) {
		store := NewMeasurementGraphStore()

		// Register schema instance rules
		store.RegisterInstanceRule(InstanceSelector{
			InstanceID: "C4220-I2",
			ClassID:    "C4220",
			MetricName: "GRAIN_YIELD",
			ClosedValues: map[string]string{
				"TAXA":   "Corn",
				"METHOD": "Machine Harvest",
			},
		})
		store.RegisterInstanceRule(InstanceSelector{
			InstanceID: "C5343-I1",
			ClassID:    "C5343",
			MetricName: "MOISTURE",
			ClosedValues: map[string]string{
				"TAXA": "Corn",
			},
			OpenKeys: []string{"OPERATOR"},
		})

		// Incoming stream of observations
		inCh := make(chan StreamingMeasurement[float64], 3)
		// Measurement 1: Matches C4220-I2
		inCh <- StreamingMeasurement[float64]{
			Identity: EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_101"},
			Primary:  KeyValue[string, float64]{Key: "GRAIN_YIELD", Value: 185.4},
			Labels: []KeyValue[string, string]{
				{Key: "TAXA", Value: "Corn"},
				{Key: "METHOD", Value: "Machine Harvest"},
			},
		}
		// Measurement 2: Matches C5343-I1 (has TAXA=Corn and open key OPERATOR)
		inCh <- StreamingMeasurement[float64]{
			Identity: EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_101"},
			Primary:  KeyValue[string, float64]{Key: "MOISTURE", Value: 14.2},
			Labels: []KeyValue[string, string]{
				{Key: "TAXA", Value: "Corn"},
				{Key: "OPERATOR", Value: "tech_42"},
			},
		}
		// Measurement 3: Unmatched (TAXA=Soybean)
		inCh <- StreamingMeasurement[float64]{
			Identity: EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_101"},
			Primary:  KeyValue[string, float64]{Key: "GRAIN_YIELD", Value: 55.0},
			Labels: []KeyValue[string, string]{
				{Key: "TAXA", Value: "Soybean"},
			},
		}
		close(inCh)

		unmatchedCh := make(chan StreamingMeasurement[float64], 3)

		err := StreamingMeasurementRouter(ctx, store,
			streaming.NewChanStream(inCh, nil),
			nil,
			streaming.NewChanStream(nil, unmatchedCh),
		)
		require.NoError(t, err)
		close(unmatchedCh)

		var unmatched []StreamingMeasurement[float64]
		for u := range unmatchedCh {
			unmatched = append(unmatched, u)
		}

		// Assertions (RFC 2119)
		require.Equal(t, 1, len(unmatched))
		require.Equal(t, 2, store.Count())

		// Verify stored NodeSelectors
		expectedNode1 := NodeSelector{
			Entity:     EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_101"},
			InstanceID: "C4220-I2",
			MetricName: "GRAIN_YIELD",
		}
		stored1, found1 := store.GetMeasurement(expectedNode1)
		require.True(t, found1)
		require.InDelta(t, 185.4, stored1.Value, 1e-6)

		expectedNode2 := NodeSelector{
			Entity:     EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_101"},
			InstanceID: "C5343-I1",
			MetricName: "MOISTURE",
		}
		stored2, found2 := store.GetMeasurement(expectedNode2)
		require.True(t, found2)
		require.InDelta(t, 14.2, stored2.Value, 1e-6)
	})

	t.Run("MUST trigger stateful calculation when all required node selectors are satisfied", func(t *testing.T) {
		store := NewMeasurementGraphStore()

		store.RegisterInstanceRule(InstanceSelector{
			InstanceID: "C4220-I2",
			MetricName: "GRAIN_YIELD",
			ClosedValues: map[string]string{
				"TAXA": "Corn",
			},
		})
		store.RegisterInstanceRule(InstanceSelector{
			InstanceID: "C5343-I1",
			MetricName: "MOISTURE",
			ClosedValues: map[string]string{
				"TAXA": "Corn",
			},
		})

		// Register calculation: Standardized Yield (adjusted to 15.5% moisture)
		store.RegisterCalculation(StatefulCalculation{
			CalculationCode: "STANDARDIZED_YIELD",
			OutputMetric:    "ADJ_YIELD",
			RequiredInputs:  []string{"C4220-I2", "C5343-I1"},
			Compute: func(ctx context.Context, entity EntityIdentity, inputs map[string]StoredMeasurement) (StreamingMeasurement[float64], error) {
				y := inputs["C4220-I2"].Value
				m := inputs["C5343-I1"].Value
				adj := y * (100.0 - m) / (100.0 - 15.5)
				return StreamingMeasurement[float64]{
					Identity: entity,
					Primary:  KeyValue[string, float64]{Key: "ADJ_YIELD", Value: adj},
				}, nil
			},
		})

		producerCh := make(chan StreamingMeasurement[float64], 2)
		entity := EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_500"}

		// Observation 1: Ingests Grain Yield (calculation should NOT trigger yet)
		producerCh <- StreamingMeasurement[float64]{
			Identity: entity,
			Primary:  KeyValue[string, float64]{Key: "GRAIN_YIELD", Value: 200.0},
			Labels:   []KeyValue[string, string]{{Key: "TAXA", Value: "Corn"}},
		}
		// Observation 2: Ingests Moisture (calculation MUST now trigger!)
		producerCh <- StreamingMeasurement[float64]{
			Identity: entity,
			Primary:  KeyValue[string, float64]{Key: "MOISTURE", Value: 15.5},
			Labels:   []KeyValue[string, string]{{Key: "TAXA", Value: "Corn"}},
		}
		close(producerCh)

		calcOutputCh := make(chan StreamingMeasurement[float64], 5)

		err := StreamingMeasurementRouter(ctx, store,
			streaming.NewChanStream(producerCh, nil),
			streaming.NewChanStream(nil, calcOutputCh),
			nil,
		)
		require.NoError(t, err)
		close(calcOutputCh)

		var results []StreamingMeasurement[float64]
		for r := range calcOutputCh {
			results = append(results, r)
		}

		require.Equal(t, 1, len(results))
		require.Equal(t, "ADJ_YIELD", results[0].Primary.Key)
		require.InDelta(t, 200.0, results[0].Primary.Value, 1e-6) // At 15.5% moisture, adjusted == raw
	})
}

// Example_measurementTwoStreamPipeline demonstrates the complete two-stream reactive model:
//  1. Stream 1 (Producer): Emits incoming domain measurements from field IoT / lab analyzers.
//  2. Stream 2 (Router & Engine): Dynamically classifies measurements to NodeSelectors in the graph store
//     and automatically triggers multi-variable StatefulCalculations as soon as their required inputs are satisfied.
func Example_measurementTwoStreamPipeline() {
	ctx := context.Background()
	store := NewMeasurementGraphStore()

	// 1. Setup Classification Tree Instance Rules
	store.RegisterInstanceRule(InstanceSelector{
		InstanceID: "C4220-I2",
		MetricName: "GRAIN_YIELD",
		ClosedValues: map[string]string{
			"TAXA":   "Corn",
			"METHOD": "Combine",
		},
	})
	store.RegisterInstanceRule(InstanceSelector{
		InstanceID: "C5343-I1",
		MetricName: "MOISTURE",
		ClosedValues: map[string]string{
			"TAXA": "Corn",
		},
		OpenKeys: []string{"DEVICE_ID"},
	})

	// 2. Register Stateful Calculation waiting for NodeSelectors [C4220-I2, C5343-I1]
	// Standardized Yield formula: Yield * (100 - Moisture) / (100 - 15.5)
	store.RegisterCalculation(StatefulCalculation{
		CalculationCode: "CALC_STD_YIELD",
		OutputMetric:    "GRAIN_YIELD_STD15",
		RequiredInputs:  []string{"C4220-I2", "C5343-I1"},
		Compute: func(ctx context.Context, entity EntityIdentity, inputs map[string]StoredMeasurement) (StreamingMeasurement[float64], error) {
			rawYield := inputs["C4220-I2"].Value
			moist := inputs["C5343-I1"].Value
			stdYield := rawYield * (100.0 - moist) / (100.0 - 15.5)

			return StreamingMeasurement[float64]{
				Identity: entity,
				Primary:  KeyValue[string, float64]{Key: "GRAIN_YIELD_STD15", Value: stdYield},
				Labels: []KeyValue[string, string]{
					{Key: "STATUS", Value: "CALCULATED"},
					{Key: "BASE_MOISTURE", Value: "15.5%"},
				},
			}, nil
		},
	})

	// 3. Stream 1: Producer emits raw measurements over time for entity "plot_37711542"
	plotEntity := EntityIdentity{
		BusinessUnit: "Agronomy",
		EntityType:   "PLOT",
		EntityID:     "plot_37711542",
	}

	producerCh := make(chan StreamingMeasurement[float64], 2)
	// First packet: Harvest Yield arrives from combine harvester
	producerCh <- StreamingMeasurement[float64]{
		Identity: plotEntity,
		Primary:  KeyValue[string, float64]{Key: "GRAIN_YIELD", Value: 185.4},
		Labels: []KeyValue[string, string]{
			{Key: "TAXA", Value: "Corn"},
			{Key: "METHOD", Value: "Combine"},
		},
	}
	// Second packet: Lab moisture analysis arrives later
	producerCh <- StreamingMeasurement[float64]{
		Identity: plotEntity,
		Primary:  KeyValue[string, float64]{Key: "MOISTURE", Value: 14.5},
		Labels: []KeyValue[string, string]{
			{Key: "TAXA", Value: "Corn"},
			{Key: "DEVICE_ID", Value: "moist_sensor_09"},
		},
	}
	close(producerCh)

	// 4. Stream 2: Router consumes from Producer, binds to Graph, and emits computed results
	computedOutputStream := make(chan StreamingMeasurement[float64], 2)

	go func() {
		defer close(computedOutputStream)
		_ = StreamingMeasurementRouter(ctx, store,
			streaming.NewChanStream(producerCh, nil),
			streaming.NewChanStream(nil, computedOutputStream),
			nil,
		)
	}()

	// 5. Downstream consumer reads published calculations
	for computed := range computedOutputStream {
		fmt.Printf("Calculation Published for %s -> [%s]: %.2f (Labels: %s=%s)\n",
			computed.Identity.EntityID,
			computed.Primary.Key,
			computed.Primary.Value,
			computed.Labels[0].Key,
			computed.Labels[0].Value,
		)
	}

	// Output:
	// Calculation Published for plot_37711542 -> [GRAIN_YIELD_STD15]: 187.59 (Labels: STATUS=CALCULATED)
}
