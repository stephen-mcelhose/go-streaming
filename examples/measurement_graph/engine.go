package measurement_graph

import (
	"context"
	"strings"
	"sync"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/examples/measurement_graph/storage"
)

// PlacementResult captures the outcome of classifying and storing an observation in the graph,
// including its resolved coordinate and any reactive calculations triggered as a result.
type PlacementResult struct {
	Coordinate *NodeCoordinate
	Placed     bool
	Triggered  []StreamingMeasurement[float64]
}

// Engine orchestrates hierarchical classification, ontology graph storage, and class-based reactive calculation routing.
type Engine struct {
	mu                sync.Mutex
	classifier        *Classifier
	store             storage.GraphStore
	calcRouter        *CalculationRouter
	firedCalculations map[string]bool // EntityID:CalculationSignature tracking to prevent re-firing across placements
	MaxCascadeDepth   int             // Maximum depth for recursive calculation feedback (default: 10)
	EnableFeedback    bool            // Automatically re-ingest calculation outputs to trigger downstream calculations (default: true)
}

// NewEngine creates a new measurement graph engine backed by the provided GraphStore.
func NewEngine(store storage.GraphStore) *Engine {
	if store == nil {
		store = storage.NewPrefixTrieGraphStore()
	}
	return &Engine{
		classifier:        NewClassifier(),
		store:             store,
		calcRouter:        NewCalculationRouter(),
		firedCalculations: make(map[string]bool),
		MaxCascadeDepth:   10,
		EnableFeedback:    true,
	}
}

// RegisterClassifierRule registers an instance classifier rule into the tree.
func (e *Engine) RegisterClassifierRule(rule InstanceClassifier) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.classifier.RegisterRule(rule)
}

// RegisterCalculation registers a stateful calculation listening for completed ClassSelector sets.
func (e *Engine) RegisterCalculation(calc StatefulCalculation) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calcRouter.RegisterCalculation(calc)
}

// ClassifyAndPlace classifies a measurement, persists it in the GraphStore under Primary:Class:Instance,
// and evaluates whether any registered class-based calculations can now trigger.
// If EnableFeedback is true, calculation outputs are placed into the graph and cascade to downstream calculations.
func (e *Engine) ClassifyAndPlace(ctx context.Context, m StreamingMeasurement[float64]) (PlacementResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	node, matched := e.classifier.Classify(m)
	if !matched {
		return PlacementResult{Placed: false}, nil
	}

	labelMap := make(map[string]string, len(m.Labels))
	for _, l := range m.Labels {
		labelMap[l.Key] = l.Value
	}

	secMap := make(map[string]float64, len(m.Secondaries))
	for _, sec := range m.Secondaries {
		secMap[sec.Key] = sec.Value
	}

	stored := StoredMeasurement{
		Node:        *node,
		Value:       m.Primary.Value,
		Labels:      labelMap,
		Secondaries: secMap,
		Timestamp:   m.Timestamp,
	}

	if err := e.store.Put(ctx, stored); err != nil {
		return PlacementResult{}, err
	}

	var allTriggered []StreamingMeasurement[float64]

	// Forward-chaining evaluation loop
	for depth := 0; depth < e.MaxCascadeDepth; depth++ {
		entityObservations, err := e.store.GetByEntity(ctx, m.Identity)
		if err != nil {
			return PlacementResult{}, err
		}

		newlyTriggered := e.evaluateUnfired(ctx, m.Identity, entityObservations)
		if len(newlyTriggered) == 0 {
			break
		}

		allTriggered = append(allTriggered, newlyTriggered...)

		if !e.EnableFeedback {
			break
		}

		// Re-ingest newly triggered outputs into working memory for next cascade tier
		for _, calcOut := range newlyTriggered {
			outNode, outMatched := e.classifier.Classify(calcOut)
			if outMatched {
				outLabelMap := make(map[string]string, len(calcOut.Labels))
				for _, l := range calcOut.Labels {
					outLabelMap[l.Key] = l.Value
				}
				outSecMap := make(map[string]float64, len(calcOut.Secondaries))
				for _, s := range calcOut.Secondaries {
					outSecMap[s.Key] = s.Value
				}

				outStored := StoredMeasurement{
					Node:        *outNode,
					Value:       calcOut.Primary.Value,
					Labels:      outLabelMap,
					Secondaries: outSecMap,
					Timestamp:   calcOut.Timestamp,
				}
				if err := e.store.Put(ctx, outStored); err != nil {
					return PlacementResult{}, err
				}
			}
		}
	}

	return PlacementResult{
		Coordinate: node,
		Placed:     true,
		Triggered:  allTriggered,
	}, nil
}

func (e *Engine) evaluateUnfired(
	ctx context.Context,
	entity EntityIdentity,
	observations []StoredMeasurement,
) []StreamingMeasurement[float64] {
	var triggeredOutputs []StreamingMeasurement[float64]

	for _, calc := range e.calcRouter.calculations {
		// Find all valid unified assignment combinations across required slots
		matches := e.findUnifiedMatches(calc.RequiredSlots, observations, 0, make(map[string]StoredMeasurement), make(map[string]string))
		if len(matches) == 0 {
			continue
		}

		for _, boundInputs := range matches {
			// Generate a unique firing signature based on entity and bound input node coordinates
			signature := entity.String() + "|" + calc.CalculationCode
			for _, slot := range calc.RequiredSlots {
				if boundObs, ok := boundInputs[slot.SlotName]; ok {
					signature += "|" + boundObs.Node.String()
				}
			}

			if e.firedCalculations[signature] {
				continue
			}

			outMeas, err := calc.Compute(ctx, entity, boundInputs)
			if err == nil {
				e.firedCalculations[signature] = true
				triggeredOutputs = append(triggeredOutputs, outMeas)
			}
		}
	}

	return triggeredOutputs
}

// findUnifiedMatches performs recursive backtracking to find all valid, unified slot bindings
// across variables declaring ConstraintDynamic (?var or @).
func (e *Engine) findUnifiedMatches(
	slots []ClassSelector,
	observations []StoredMeasurement,
	slotIdx int,
	currentBindings map[string]StoredMeasurement,
	unifiedVars map[string]string,
) []map[string]StoredMeasurement {
	if slotIdx >= len(slots) {
		// Completed a valid binding across all slots
		result := make(map[string]StoredMeasurement, len(currentBindings))
		for k, v := range currentBindings {
			result[k] = v
		}
		return []map[string]StoredMeasurement{result}
	}

	slot := slots[slotIdx]
	var results []map[string]StoredMeasurement

	// Group candidates that satisfy static match and current dynamic unification bindings
	for _, obs := range observations {
		if !slot.StaticMatches(obs) {
			continue
		}

		// Check and attempt unification on dynamic constraints
		constraints := slot.ParsedConstraints()
		unificationValid := true
		newVars := make(map[string]string, len(unifiedVars))
		for k, v := range unifiedVars {
			newVars[k] = v
		}

		for _, c := range constraints {
			if c.Kind == ConstraintDynamic {
				actualVal, hasVal := obs.Labels[c.Key]
				if !hasVal {
					unificationValid = false
					break
				}

				if boundVal, alreadyBound := newVars[c.VarName]; alreadyBound {
					if boundVal != actualVal {
						unificationValid = false
						break
					}
				} else {
					newVars[c.VarName] = actualVal
				}
			}
		}

		if !unificationValid {
			continue
		}

		// Recurse to next slot with updated bindings
		currentBindings[slot.SlotName] = obs
		subMatches := e.findUnifiedMatches(slots, observations, slotIdx+1, currentBindings, newVars)
		results = append(results, subMatches...)
		delete(currentBindings, slot.SlotName)
	}

	return results
}

// InvalidateEntity prunes the working memory subtree for the given entity.
func (e *Engine) InvalidateEntity(ctx context.Context, entity EntityIdentity) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Clear firing history for this entity
	entityPrefix := entity.String() + "|"
	for sig := range e.firedCalculations {
		if strings.HasPrefix(sig, entityPrefix) {
			delete(e.firedCalculations, sig)
		}
	}

	return e.store.InvalidateEntity(ctx, entity)
}

// Retract removes a specific node coordinate from the graph.
func (e *Engine) Retract(ctx context.Context, node NodeCoordinate) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.store.Retract(ctx, node)
}

// GetMeasurement retrieves a stored measurement by its NodeCoordinate.
func (e *Engine) GetMeasurement(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error) {
	return e.store.Get(ctx, node)
}

// Count returns the total number of stored measurement nodes in the graph.
func (e *Engine) Count(ctx context.Context) (int, error) {
	return e.store.Count(ctx)
}

// Router is a reactive stream operator:
// 1. Ingests raw measurements from a producer stream.
// 2. Classifies and places them in the ontology graph store via Engine.
// 3. Automatically triggers and emits computed output measurements when class-selected variable slots are satisfied.
func Router(
	ctx context.Context,
	engine *Engine,
	inProducer streaming.Receiver[StreamingMeasurement[float64]],
	outCalculations streaming.Sender[StreamingMeasurement[float64]],
	outUnmatched streaming.Sender[StreamingMeasurement[float64]],
) error {
	return inProducer.Receive(ctx, func(ctx context.Context, m StreamingMeasurement[float64]) error {
		res, err := engine.ClassifyAndPlace(ctx, m)
		if err != nil {
			return err
		}

		if res.Placed {
			if outCalculations != nil {
				for _, computed := range res.Triggered {
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
