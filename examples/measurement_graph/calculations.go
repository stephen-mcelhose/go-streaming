package measurement_graph

import (
	"context"
)

// ClassSelector specifies a domain class required by a calculation variable slot.
type ClassSelector struct {
	SlotName    string            // Variable slot name in formula (e.g. "Yield", "Moisture")
	ClassID     string            // Required ontology ClassID (e.g. "CLASS_GRAIN_YIELD")
	Constraints map[string]string // Optional context constraints required on candidate observation
}

// Matches checks if a candidate stored measurement satisfies the slot's class and constraints.
func (cs ClassSelector) Matches(obs StoredMeasurement) bool {
	if obs.Node.ClassID != cs.ClassID {
		return false
	}
	for k, v := range cs.Constraints {
		if actual, ok := obs.Labels[k]; !ok || actual != v {
			return false
		}
	}
	return true
}

// StatefulCalculation defines a multi-variable computation template triggered when
// all required ClassSelectors are satisfied for a given entity in the graph.
type StatefulCalculation struct {
	CalculationCode string
	OutputMetric    string
	RequiredSlots   []ClassSelector
	Compute         func(ctx context.Context, entity EntityIdentity, inputs map[string]StoredMeasurement) (StreamingMeasurement[float64], error)
}

// CalculationRouter evaluates calculations against entity graph nodes and produces output measurements.
type CalculationRouter struct {
	calculations []StatefulCalculation
}

// NewCalculationRouter creates a calculation router.
func NewCalculationRouter() *CalculationRouter {
	return &CalculationRouter{}
}

// RegisterCalculation registers a stateful calculation.
func (r *CalculationRouter) RegisterCalculation(calc StatefulCalculation) {
	r.calculations = append(r.calculations, calc)
}

// Evaluate checks if any registered calculations are satisfied by the entity's observations.
func (r *CalculationRouter) Evaluate(ctx context.Context, entity EntityIdentity, observations []StoredMeasurement) []StreamingMeasurement[float64] {
	var triggeredOutputs []StreamingMeasurement[float64]

	for _, calc := range r.calculations {
		boundInputs := make(map[string]StoredMeasurement, len(calc.RequiredSlots))
		allSatisfied := true

		for _, slot := range calc.RequiredSlots {
			var bestCandidate *StoredMeasurement

			for _, obs := range observations {
				if slot.Matches(obs) {
					// Select latest observation candidate for the slot
					if bestCandidate == nil || obs.Timestamp.After(bestCandidate.Timestamp) {
						obsCopy := obs
						bestCandidate = &obsCopy
					}
				}
			}

			if bestCandidate == nil {
				allSatisfied = false
				break
			}
			boundInputs[slot.SlotName] = *bestCandidate
		}

		if allSatisfied {
			outMeas, err := calc.Compute(ctx, entity, boundInputs)
			if err == nil {
				triggeredOutputs = append(triggeredOutputs, outMeas)
			}
		}
	}

	return triggeredOutputs
}
