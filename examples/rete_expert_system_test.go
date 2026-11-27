package examples_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming"
	mathops "github.com/stephen-mcelhose/go-streaming/operators/math"
)

// ConstraintKind distinguishes exact key-value matches from presence checks.
type ConstraintKind int

const (
	// ConstraintExact requires KEY=VALUE to match exactly on the measurement.
	ConstraintExact ConstraintKind = iota
	// ConstraintPresent requires KEY to exist on the measurement (any value).
	ConstraintPresent
)

// SlotRule represents an individual condition inside a selector pattern.
type SlotRule struct {
	Key   string
	Value string
	Kind  ConstraintKind
}

// PatternSelector represents a declarative slot pattern: EntityType[PRIMARY]{KEY=VALUE, KEY2, ...}
type PatternSelector struct {
	EntityType  string
	PrimaryCode string
	Rules       []SlotRule
}

// ParsePattern parses pattern strings like "PLOT[GRAIN_YIELD]{TAXA=Corn, METHOD, STATUS=VERIFIED}"
func ParsePattern(patternStr string) (PatternSelector, error) {
	patternStr = strings.TrimSpace(patternStr)
	openBracket := strings.Index(patternStr, "[")
	closeBracket := strings.Index(patternStr, "]")

	if openBracket == -1 || closeBracket == -1 || closeBracket < openBracket {
		return PatternSelector{}, fmt.Errorf("invalid pattern syntax: missing [PRIMARY] in %q", patternStr)
	}

	entityType := strings.TrimSpace(patternStr[:openBracket])
	primaryCode := strings.TrimSpace(patternStr[openBracket+1 : closeBracket])

	var rules []SlotRule
	openBrace := strings.Index(patternStr, "{")
	closeBrace := strings.Index(patternStr, "}")

	if openBrace != -1 && closeBrace != -1 && closeBrace > openBrace {
		body := patternStr[openBrace+1 : closeBrace]
		for _, part := range strings.Split(body, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if strings.Contains(part, "=") {
				kv := strings.SplitN(part, "=", 2)
				rules = append(rules, SlotRule{
					Key:   strings.TrimSpace(kv[0]),
					Value: strings.TrimSpace(kv[1]),
					Kind:  ConstraintExact,
				})
			} else {
				rules = append(rules, SlotRule{
					Key:  part,
					Kind: ConstraintPresent,
				})
			}
		}
	}

	return PatternSelector{
		EntityType:  entityType,
		PrimaryCode: primaryCode,
		Rules:       rules,
	}, nil
}

// EvaluateCandidate checks if a candidate measurement satisfies the pattern's hard constraints,
// and computes its specificity fit score.
// Returns (score, matches):
// - matches: true if ALL hard constraints pass.
// - score: higher score indicates a more specific, higher-fidelity match.
func (p PatternSelector) EvaluateCandidate(m StoredObservation) (int, bool) {
	// 1. Primary Code MUST match
	if m.PrimaryCode != p.PrimaryCode {
		return 0, false
	}

	// 2. Entity Type MUST match if specified
	if p.EntityType != "" && m.Entity.EntityType != p.EntityType {
		return 0, false
	}

	score := 0

	// 3. Evaluate each rule
	for _, rule := range p.Rules {
		switch rule.Kind {
		case ConstraintExact:
			actualVal, exists := m.Labels[rule.Key]
			if !exists || actualVal != rule.Value {
				return 0, false // Hard constraint violation: disqualified
			}
			score += 100 // High score weight for satisfying exact closed constraints
		case ConstraintPresent:
			_, inLabels := m.Labels[rule.Key]
			_, inSec := m.Secondaries[rule.Key]
			if !inLabels && !inSec {
				return 0, false // Hard constraint violation: required key missing
			}
			score += 20 // Moderate score weight for satisfying open presence constraints
		}
	}

	// 4. Bonus specificity for auxiliary secondary context
	score += len(m.Labels) + len(m.Secondaries)

	return score, true
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

// StoredObservation holds an indexed observation in an entity's working memory.
type StoredObservation struct {
	Entity      EntityIdentity
	PrimaryCode string
	Value       float64
	Labels      map[string]string
	Secondaries map[string]float64
	Timestamp   time.Time
}

// ReteCalculationRule represents a multi-variable calculation listening for best-fit slot matches.
type ReteCalculationRule struct {
	RuleID       string
	OutputMetric string
	SlotPatterns []PatternSelector
	Compute      func(ctx context.Context, entity EntityIdentity, boundSlots map[string]StoredObservation) (StreamingMeasurement[float64], error)
}

// ReteExpertGraphStore is a forward-chaining expert system working memory.
// It maintains:
// 1. Alpha Memory: Partitions observations by (Entity, PrimaryCode) for fast O(1) candidate lookup.
// 2. Beta Memory & Best-Fit Solver: Evaluates multi-pattern slot sets against working memory.
type ReteExpertGraphStore struct {
	mu sync.RWMutex

	// Alpha Memory: Entity.String() -> map[PrimaryCode][]StoredObservation
	alphaMemory map[string]map[string][]StoredObservation

	// Registered Beta Calculation Rules
	rules []ReteCalculationRule
}

// NewReteExpertGraphStore creates an empty forward-chaining working memory.
func NewReteExpertGraphStore() *ReteExpertGraphStore {
	return &ReteExpertGraphStore{
		alphaMemory: make(map[string]map[string][]StoredObservation),
	}
}

// RegisterRule registers a reactive calculation rule.
func (s *ReteExpertGraphStore) RegisterRule(rule ReteCalculationRule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rules = append(s.rules, rule)
}

// Ingest places an observation into Alpha Memory and evaluates whether any Beta calculation rules fire.
func (s *ReteExpertGraphStore) Ingest(ctx context.Context, m StreamingMeasurement[float64]) ([]StreamingMeasurement[float64], error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entityKey := m.Identity.String()
	if s.alphaMemory[entityKey] == nil {
		s.alphaMemory[entityKey] = make(map[string][]StoredObservation)
	}

	// Convert labels and secondaries to map for indexed querying
	labelMap := make(map[string]string, len(m.Labels))
	for _, l := range m.Labels {
		labelMap[l.Key] = l.Value
	}
	secMap := make(map[string]float64, len(m.Secondaries))
	for _, sec := range m.Secondaries {
		secMap[sec.Key] = sec.Value
	}

	stored := StoredObservation{
		Entity:      m.Identity,
		PrimaryCode: m.Primary.Key,
		Value:       m.Primary.Value,
		Labels:      labelMap,
		Secondaries: secMap,
		Timestamp:   m.Timestamp,
	}

	s.alphaMemory[entityKey][m.Primary.Key] = append(s.alphaMemory[entityKey][m.Primary.Key], stored)

	// Beta Join & Best-Fit Resolution across all registered rules
	var triggeredOutputs []StreamingMeasurement[float64]
	entityAlpha := s.alphaMemory[entityKey]

	for _, rule := range s.rules {
		allSlotsFound := true
		boundSlots := make(map[string]StoredObservation, len(rule.SlotPatterns))

		for _, pattern := range rule.SlotPatterns {
			candidates := entityAlpha[pattern.PrimaryCode]
			if len(candidates) == 0 {
				allSlotsFound = false
				break
			}

			// Best-fit search: find candidate that passes all MUST rules with highest specificity score
			bestScore := -1
			var bestCandidate *StoredObservation

			for i := range candidates {
				cand := candidates[i]
				score, matches := pattern.EvaluateCandidate(cand)
				if matches && score > bestScore {
					bestScore = score
					bestCandidate = &cand
				}
			}

			if bestCandidate == nil {
				allSlotsFound = false
				break
			}

			// Slot key format: PrimaryCode
			boundSlots[pattern.PrimaryCode] = *bestCandidate
		}

		if allSlotsFound {
			outMeas, err := rule.Compute(ctx, m.Identity, boundSlots)
			if err == nil {
				triggeredOutputs = append(triggeredOutputs, outMeas)
			}
		}
	}

	return triggeredOutputs, nil
}

// StreamingReteExpertRouter is a streaming operator that drives the Rete forward-chaining expert system.
func StreamingReteExpertRouter(
	ctx context.Context,
	store *ReteExpertGraphStore,
	inProducer streaming.Receiver[StreamingMeasurement[float64]],
	outCalculations streaming.Sender[StreamingMeasurement[float64]],
) error {
	return inProducer.Receive(ctx, func(ctx context.Context, m StreamingMeasurement[float64]) error {
		outputs, err := store.Ingest(ctx, m)
		if err != nil {
			return err
		}
		for _, out := range outputs {
			if outCalculations != nil {
				if err := outCalculations.Send(ctx, out); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func TestReteBestFitSlotResolution(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST select best-fit candidate when multiple observations exist for the same primary metric", func(t *testing.T) {
		pattern, err := ParsePattern("PLOT[GRAIN_YIELD]{TAXA=Corn, METHOD}")
		require.NoError(t, err)

		// Candidate A: TAXA=Soybean (Violates closed constraint TAXA=Corn) -> DISQUALIFIED
		candA := StoredObservation{
			Entity:      EntityIdentity{EntityType: "PLOT", EntityID: "p1"},
			PrimaryCode: "GRAIN_YIELD",
			Value:       50.0,
			Labels:      map[string]string{"TAXA": "Soybean", "METHOD": "Combine"},
		}
		_, matchA := pattern.EvaluateCandidate(candA)
		require.False(t, matchA, "Candidate A must be disqualified because TAXA != Corn")

		// Candidate B: TAXA=Corn, but missing METHOD -> DISQUALIFIED
		candB := StoredObservation{
			Entity:      EntityIdentity{EntityType: "PLOT", EntityID: "p1"},
			PrimaryCode: "GRAIN_YIELD",
			Value:       170.0,
			Labels:      map[string]string{"TAXA": "Corn"},
		}
		_, matchB := pattern.EvaluateCandidate(candB)
		require.False(t, matchB, "Candidate B must be disqualified because required label METHOD is missing")

		// Candidate C: TAXA=Corn, METHOD=Manual (Qualifies, base specificity)
		candC := StoredObservation{
			Entity:      EntityIdentity{EntityType: "PLOT", EntityID: "p1"},
			PrimaryCode: "GRAIN_YIELD",
			Value:       180.0,
			Labels:      map[string]string{"TAXA": "Corn", "METHOD": "Manual"},
		}
		scoreC, matchC := pattern.EvaluateCandidate(candC)
		require.True(t, matchC)

		// Candidate D: TAXA=Corn, METHOD=Combine, plus extra calibrated secondaries (Higher specificity fit!)
		candD := StoredObservation{
			Entity:      EntityIdentity{EntityType: "PLOT", EntityID: "p1"},
			PrimaryCode: "GRAIN_YIELD",
			Value:       185.4,
			Labels:      map[string]string{"TAXA": "Corn", "METHOD": "Combine", "CALIBRATED": "true"},
			Secondaries: map[string]float64{"TEST_WEIGHT": 58.2},
		}
		scoreD, matchD := pattern.EvaluateCandidate(candD)
		require.True(t, matchD)
		require.Greater(t, scoreD, scoreC, "Candidate D must have higher specificity score than Candidate C")
	})

	t.Run("MUST dynamically resolve best-fit slots and trigger multi-variable calculation", func(t *testing.T) {
		store := NewReteExpertGraphStore()

		// Register calculation with 2 slot patterns:
		// Slot 1: PLOT[GRAIN_YIELD]{TAXA=Corn, METHOD}
		// Slot 2: PLOT[MOISTURE]{TAXA=Corn}
		pat1, _ := ParsePattern("PLOT[GRAIN_YIELD]{TAXA=Corn, METHOD}")
		pat2, _ := ParsePattern("PLOT[MOISTURE]{TAXA=Corn}")

		store.RegisterRule(ReteCalculationRule{
			RuleID:       "STANDARDIZE_YIELD",
			OutputMetric: "GRAIN_YIELD_15",
			SlotPatterns: []PatternSelector{pat1, pat2},
			Compute: func(ctx context.Context, entity EntityIdentity, slots map[string]StoredObservation) (StreamingMeasurement[float64], error) {
				y := slots["GRAIN_YIELD"].Value
				m := slots["MOISTURE"].Value
				adj := y * (100.0 - m) / (100.0 - 15.5)

				return StreamingMeasurement[float64]{
					Identity: entity,
					Primary:  KeyValue[string, float64]{Key: "GRAIN_YIELD_15", Value: adj},
				}, nil
			},
		})

		entity := EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_700"}
		producerCh := make(chan StreamingMeasurement[float64], 3)

		// 1. Partial uncalibrated observation (qualifies, but lower specificity)
		producerCh <- StreamingMeasurement[float64]{
			Identity: entity,
			Primary:  KeyValue[string, float64]{Key: "GRAIN_YIELD", Value: 180.0},
			Labels:   []KeyValue[string, string]{{Key: "TAXA", Value: "Corn"}, {Key: "METHOD", Value: "Generic"}},
		}
		// 2. High-precision calibrated yield observation (best-fit!)
		producerCh <- StreamingMeasurement[float64]{
			Identity: entity,
			Primary:  KeyValue[string, float64]{Key: "GRAIN_YIELD", Value: 190.0},
			Labels: []KeyValue[string, string]{
				{Key: "TAXA", Value: "Corn"},
				{Key: "METHOD", Value: "Combine"},
				{Key: "CALIBRATED", Value: "true"},
			},
			Secondaries: []KeyValue[string, float64]{{Key: "TEST_WEIGHT", Value: 59.0}},
		}
		// 3. Moisture observation (completes the required slot set -> triggers calculation using best-fit yield = 190.0!)
		producerCh <- StreamingMeasurement[float64]{
			Identity: entity,
			Primary:  KeyValue[string, float64]{Key: "MOISTURE", Value: 15.5},
			Labels:   []KeyValue[string, string]{{Key: "TAXA", Value: "Corn"}},
		}
		close(producerCh)

		calcOutCh := make(chan StreamingMeasurement[float64], 5)
		err := StreamingReteExpertRouter(ctx, store,
			streaming.NewChanStream(producerCh, nil),
			streaming.NewChanStream(nil, calcOutCh),
		)
		require.NoError(t, err)
		close(calcOutCh)

		var results []StreamingMeasurement[float64]
		for r := range calcOutCh {
			results = append(results, r)
		}

		require.Equal(t, 1, len(results))
		require.Equal(t, "GRAIN_YIELD_15", results[0].Primary.Key)
		// At 15.5% moisture, 190.0 adjusted == 190.0 (confirms best-fit Candidate 2 was used instead of Candidate 1)
		require.InDelta(t, 190.0, results[0].Primary.Value, 1e-6)
	})
}

// Example_reteExpertSystemBestFit demonstrates how a forward-chaining expert system
// automatically searches available measurements for an entity, chooses the "best-fit"
// observation satisfying pattern constraints, and fires the calculation when ready.
func Example_reteExpertSystemBestFit() {
	ctx := context.Background()
	store := NewReteExpertGraphStore()

	// 1. Calculation specifies abstract pattern requirements (does not need to know internal instance IDs):
	// - Slot 1: Requires PLOT[GRAIN_YIELD] where TAXA=Corn and METHOD is specified.
	// - Slot 2: Requires PLOT[MOISTURE] where TAXA=Corn.
	patYield, _ := ParsePattern("PLOT[GRAIN_YIELD]{TAXA=Corn, METHOD}")
	patMoist, _ := ParsePattern("PLOT[MOISTURE]{TAXA=Corn}")

	store.RegisterRule(ReteCalculationRule{
		RuleID:       "CALC_ADJUSTED_YIELD",
		OutputMetric: "GRAIN_YIELD_STD15",
		SlotPatterns: []PatternSelector{patYield, patMoist},
		Compute: func(ctx context.Context, entity EntityIdentity, slots map[string]StoredObservation) (StreamingMeasurement[float64], error) {
			rawYield := slots["GRAIN_YIELD"].Value
			moisture := slots["MOISTURE"].Value
			adjusted := rawYield * (100.0 - moisture) / (100.0 - 15.5)

			return StreamingMeasurement[float64]{
				Identity: entity,
				Primary:  KeyValue[string, float64]{Key: "GRAIN_YIELD_STD15", Value: adjusted},
				Labels: []KeyValue[string, string]{
					{Key: "STATUS", Value: "CALCULATED"},
					{Key: "SELECTED_METHOD", Value: slots["GRAIN_YIELD"].Labels["METHOD"]},
				},
			}, nil
		},
	})

	entity := EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_37711542"}

	// Stream 1 (Producer): Emits measurements over time
	producerCh := make(chan StreamingMeasurement[float64], 3)
	// Observation 1: Preliminary manual yield estimate (175.0 bu/acre)
	producerCh <- StreamingMeasurement[float64]{
		Identity: entity,
		Primary:  KeyValue[string, float64]{Key: "GRAIN_YIELD", Value: 175.0},
		Labels:   []KeyValue[string, string]{{Key: "TAXA", Value: "Corn"}, {Key: "METHOD", Value: "HandSample"}},
	}
	// Observation 2: Final harvester combine yield with high-precision test weight (185.4 bu/acre) -> BEST FIT
	producerCh <- StreamingMeasurement[float64]{
		Identity:    entity,
		Primary:     KeyValue[string, float64]{Key: "GRAIN_YIELD", Value: 185.4},
		Labels:      []KeyValue[string, string]{{Key: "TAXA", Value: "Corn"}, {Key: "METHOD", Value: "Combine"}, {Key: "QUALITY", Value: "HIGH"}},
		Secondaries: []KeyValue[string, float64]{{Key: "TEST_WEIGHT", Value: 58.2}},
	}
	// Observation 3: Lab moisture reading (14.5%) -> Completes the rule!
	producerCh <- StreamingMeasurement[float64]{
		Identity: entity,
		Primary:  KeyValue[string, float64]{Key: "MOISTURE", Value: 14.5},
		Labels:   []KeyValue[string, string]{{Key: "TAXA", Value: "Corn"}},
	}
	close(producerCh)

	// Stream 2 (Router): Evaluates patterns, picks best-fit slot, and emits triggered calculation
	calculationOutputCh := make(chan StreamingMeasurement[float64], 2)

	go func() {
		defer close(calculationOutputCh)
		_ = StreamingReteExpertRouter(ctx, store,
			streaming.NewChanStream(producerCh, nil),
			streaming.NewChanStream(nil, calculationOutputCh),
		)
	}()

	for res := range calculationOutputCh {
		fmt.Printf("Expert Rule Fired for %s -> [%s]: %.2f (Selected Best-Fit Method: %s)\n",
			res.Identity.EntityID,
			res.Primary.Key,
			res.Primary.Value,
			res.Labels[1].Value,
		)
	}

	// Output:
	// Expert Rule Fired for plot_37711542 -> [GRAIN_YIELD_STD15]: 187.59 (Selected Best-Fit Method: Combine)
}
