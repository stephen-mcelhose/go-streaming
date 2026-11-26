package examples_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// SiblingConstrainer dynamically narrows unbound slot patterns based on observations
// that have already been bound to sibling slots in the same calculation.
type SiblingConstrainer interface {
	Constrain(boundSlot string, boundObs StoredObservation, unboundPattern PatternSelector) PatternSelector
}

// AutoContextConstrainer propagates contextual metadata (such as Taxa or Environment)
// from the first bound observation to all remaining unbound slots in the calculation.
type AutoContextConstrainer struct {
	PropagateKeys []string // e.g. ["TAXA", "ENVIRONMENT", "BUSINESS_UNIT"]
}

// Constrain adds exact closed constraints to the unbound pattern matching the bound observation's context.
func (c AutoContextConstrainer) Constrain(boundSlot string, boundObs StoredObservation, unboundPattern PatternSelector) PatternSelector {
	for _, k := range c.PropagateKeys {
		if val, exists := boundObs.Labels[k]; exists {
			// Avoid duplicate rules
			hasRule := false
			for _, r := range unboundPattern.Rules {
				if r.Key == k {
					hasRule = true
					break
				}
			}
			if !hasRule {
				unboundPattern.Rules = append(unboundPattern.Rules, SlotRule{
					Key:   k,
					Value: val,
					Kind:  ConstraintExact,
				})
			}
		}
	}
	return unboundPattern
}

// ConstrainedReteRule enhances ReteCalculationRule with dynamic sibling pattern constraining.
type ConstrainedReteRule struct {
	ReteCalculationRule
	Constrainer SiblingConstrainer
}

// AdvancedReteExpertGraphStore extends working memory with progressive sibling pattern constraining.
type AdvancedReteExpertGraphStore struct {
	mu sync.RWMutex

	// Alpha Memory: Entity.String() -> map[PrimaryCode][]StoredObservation
	alphaMemory map[string]map[string][]StoredObservation

	// Registered Constrained Rules
	rules []ConstrainedReteRule
}

// NewAdvancedReteExpertGraphStore creates an empty advanced working memory.
func NewAdvancedReteExpertGraphStore() *AdvancedReteExpertGraphStore {
	return &AdvancedReteExpertGraphStore{
		alphaMemory: make(map[string]map[string][]StoredObservation),
	}
}

// RegisterConstrainedRule registers a calculation rule equipped with dynamic sibling constraining.
func (s *AdvancedReteExpertGraphStore) RegisterConstrainedRule(rule ConstrainedReteRule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rules = append(s.rules, rule)
}

// Ingest places an observation into working memory and evaluates constrained calculations.
func (s *AdvancedReteExpertGraphStore) Ingest(ctx context.Context, m StreamingMeasurement[float64]) ([]StreamingMeasurement[float64], error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entityKey := m.Identity.String()
	if s.alphaMemory[entityKey] == nil {
		s.alphaMemory[entityKey] = make(map[string][]StoredObservation)
	}

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

	var triggeredOutputs []StreamingMeasurement[float64]
	entityAlpha := s.alphaMemory[entityKey]

	for _, rule := range s.rules {
		livePatterns := make([]PatternSelector, len(rule.SlotPatterns))
		copy(livePatterns, rule.SlotPatterns)

		boundSlots := make(map[string]StoredObservation, len(livePatterns))
		allSlotsFound := true

		for i, pattern := range livePatterns {
			candidates := entityAlpha[pattern.PrimaryCode]
			if len(candidates) == 0 {
				allSlotsFound = false
				break
			}

			// Best-fit search against current live pattern
			bestScore := -1
			var bestCandidate *StoredObservation

			for _, cand := range candidates {
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

			boundSlots[pattern.PrimaryCode] = *bestCandidate

			// Apply dynamic sibling constraining if configured
			if rule.Constrainer != nil {
				for j := i + 1; j < len(livePatterns); j++ {
					livePatterns[j] = rule.Constrainer.Constrain(pattern.PrimaryCode, *bestCandidate, livePatterns[j])
				}
			}
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

func TestConstrainedReteCalculations(t *testing.T) {
	ctx := context.Background()

	t.Run("MUST dynamically narrow sibling slot pattern when context is bound", func(t *testing.T) {
		store := NewAdvancedReteExpertGraphStore()

		// Sibling 1 requires GRAIN_YIELD (open Taxa)
		// Sibling 2 requires MOISTURE (open Taxa)
		// SiblingConstrainer automatically propagates TAXA from Sibling 1 -> Sibling 2
		pat1, _ := ParsePattern("PLOT[GRAIN_YIELD]{METHOD}")
		pat2, _ := ParsePattern("PLOT[MOISTURE]{}")

		store.RegisterConstrainedRule(ConstrainedReteRule{
			ReteCalculationRule: ReteCalculationRule{
				RuleID:       "AUTO_CONSTRAINED_YIELD",
				OutputMetric: "GRAIN_YIELD_15",
				SlotPatterns: []PatternSelector{pat1, pat2},
				Compute: func(ctx context.Context, entity EntityIdentity, slots map[string]StoredObservation) (StreamingMeasurement[float64], error) {
					y := slots["GRAIN_YIELD"].Value
					m := slots["MOISTURE"].Value
					adj := y * (100.0 - m) / (100.0 - 15.5)

					return StreamingMeasurement[float64]{
						Identity: entity,
						Primary:  KeyValue[string, float64]{Key: "GRAIN_YIELD_15", Value: adj},
						Labels: []KeyValue[string, string]{
							{Key: "BOUND_TAXA", Value: slots["MOISTURE"].Labels["TAXA"]},
						},
					}, nil
				},
			},
			Constrainer: AutoContextConstrainer{
				PropagateKeys: []string{"TAXA"},
			},
		})

		entity := EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_888"}

		// 1. Ingest Corn Grain Yield (binds Slot 1, dynamically constrains Slot 2 to TAXA=Corn!)
		m1 := StreamingMeasurement[float64]{
			Identity: entity,
			Primary:  KeyValue[string, float64]{Key: "GRAIN_YIELD", Value: 190.0},
			Labels:   []KeyValue[string, string]{{Key: "TAXA", Value: "Corn"}, {Key: "METHOD", Value: "Combine"}},
		}
		_, err := store.Ingest(ctx, m1)
		require.NoError(t, err)

		// 2. Ingest Soybean Moisture (must be rejected by Slot 2 because Slot 2 was dynamically narrowed to TAXA=Corn!)
		mSoy := StreamingMeasurement[float64]{
			Identity: entity,
			Primary:  KeyValue[string, float64]{Key: "MOISTURE", Value: 12.0},
			Labels:   []KeyValue[string, string]{{Key: "TAXA", Value: "Soybean"}},
		}
		outSoy, err := store.Ingest(ctx, mSoy)
		require.NoError(t, err)
		require.Empty(t, outSoy, "Soybean moisture must not trigger the Corn calculation")

		// 3. Ingest Corn Moisture (matches the dynamically narrowed Slot 2 -> triggers calculation!)
		mCorn := StreamingMeasurement[float64]{
			Identity: entity,
			Primary:  KeyValue[string, float64]{Key: "MOISTURE", Value: 15.5},
			Labels:   []KeyValue[string, string]{{Key: "TAXA", Value: "Corn"}},
		}
		outCorn, err := store.Ingest(ctx, mCorn)
		require.NoError(t, err)
		require.Equal(t, 1, len(outCorn))
		require.Equal(t, "GRAIN_YIELD_15", outCorn[0].Primary.Key)
		require.Equal(t, "Corn", outCorn[0].Labels[0].Value)
		require.InDelta(t, 190.0, outCorn[0].Primary.Value, 1e-6)
	})
}
