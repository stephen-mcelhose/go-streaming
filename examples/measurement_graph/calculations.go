package measurement_graph

import (
	"context"
	"strings"
)

// ConstraintKind classifies the evaluation semantics of a calculation slot constraint.
type ConstraintKind int

const (
	// ConstraintExact requires an exact literal string match: obs.Labels[Key] == Value
	ConstraintExact ConstraintKind = iota
	// ConstraintDynamic unifies a variable across slots: obs.Labels[Key] == ?var
	ConstraintDynamic
	// ConstraintExistence requires key presence only: _, ok := obs.Labels[Key]; ok
	ConstraintExistence
	// ConstraintForbidden requires key absence: _, ok := obs.Labels[Key]; !ok
	ConstraintForbidden
)

// SlotConstraint defines an individual constraint rule on a ClassSelector slot.
type SlotConstraint struct {
	Key      string         // The label key being constrained (e.g. "STAGE", "TAXA")
	Value    string         // The expected literal value, or variable identifier (e.g. "?stage")
	Kind     ConstraintKind // Exact, Dynamic, Existence, or Forbidden
	VarName  string         // The unification variable name (e.g. "stage")
}

// ParseConstraint parses constraint syntax into a structured SlotConstraint:
// - "KEY=Val" or "KEY: Val" -> ConstraintExact
// - "KEY=?" or "KEY: ?" or "KEY=@" or "KEY: ?var" -> ConstraintDynamic
// - "KEY=*" or "KEY: *" or "KEY" -> ConstraintExistence
// - "!KEY" or "!KEY: *" or "!KEY: ''" -> ConstraintForbidden
func ParseConstraint(key, val string) SlotConstraint {
	key = strings.TrimSpace(key)
	val = strings.TrimSpace(val)

	// Check for negative/forbidden constraint (!KEY)
	if strings.HasPrefix(key, "!") {
		cleanKey := strings.TrimPrefix(key, "!")
		return SlotConstraint{
			Key:     cleanKey,
			Kind:    ConstraintForbidden,
		}
	}

	// Dynamic Unification (@ or ? or ?varName)
	if val == "@" || val == "?" || strings.HasPrefix(val, "?") {
		varName := val
		if val == "@" || val == "?" {
			varName = key // Default variable scope to the key name
		} else {
			varName = strings.TrimPrefix(val, "?")
		}
		return SlotConstraint{
			Key:     key,
			Value:   val,
			Kind:    ConstraintDynamic,
			VarName: varName,
		}
	}

	// Existence / Presence only (*)
	if val == "*" || val == "" {
		return SlotConstraint{
			Key:  key,
			Kind: ConstraintExistence,
		}
	}

	// Exact / Closed Value
	return SlotConstraint{
		Key:   key,
		Value: val,
		Kind:  ConstraintExact,
	}
}

// ClassSelector specifies a domain class required by a calculation variable slot.
type ClassSelector struct {
	SlotName      string            // Variable slot name in formula (e.g. "Yield", "Moisture")
	ClassID       string            // Required ontology ClassID (e.g. "CLASS_GRAIN_YIELD")
	SecondaryKey  string            // Optional: extracts numeric value from Secondaries[SecondaryKey] rather than Primary
	ForbiddenKeys []string          // Optional: keys that MUST NOT exist on candidate observation
	Constraints   map[string]string // Key-value constraints (Exact, Dynamic "?var"/"@", Existence "*", Forbidden "!KEY")
}

// ParsedConstraints returns the slice of structured SlotConstraints.
func (cs ClassSelector) ParsedConstraints() []SlotConstraint {
	var list []SlotConstraint

	for _, forbidden := range cs.ForbiddenKeys {
		list = append(list, SlotConstraint{
			Key:  forbidden,
			Kind: ConstraintForbidden,
		})
	}

	for k, v := range cs.Constraints {
		list = append(list, ParseConstraint(k, v))
	}
	return list
}

// StaticMatches checks if a candidate stored measurement satisfies the slot's ClassID,
// static exact constraints, existence checks, forbidden keys, and secondary key availability.
// Dynamic unification constraints are evaluated during Beta join backtracking.
func (cs ClassSelector) StaticMatches(obs StoredMeasurement) bool {
	if obs.Node.ClassID != cs.ClassID {
		return false
	}

	// If a secondary key is required, verify it exists in Secondaries
	if cs.SecondaryKey != "" {
		if _, ok := obs.Secondaries[cs.SecondaryKey]; !ok {
			return false
		}
	}

	constraints := cs.ParsedConstraints()
	for _, c := range constraints {
		switch c.Kind {
		case ConstraintForbidden:
			if _, exists := obs.Labels[c.Key]; exists {
				return false
			}
		case ConstraintExistence:
			if _, exists := obs.Labels[c.Key]; !exists {
				return false
			}
		case ConstraintExact:
			actual, exists := obs.Labels[c.Key]
			if !exists || actual != c.Value {
				return false
			}
		case ConstraintDynamic:
			// Must at least possess the dynamic key to be considered for unification
			if _, exists := obs.Labels[c.Key]; !exists {
				return false
			}
		}
	}

	return true
}

// Matches delegates to StaticMatches for standalone single-observation checks.
func (cs ClassSelector) Matches(obs StoredMeasurement) bool {
	return cs.StaticMatches(obs)
}

// ValueForObservation extracts the effective numeric value from an observation for this slot.
// If SecondaryKey is declared, it extracts from Secondaries; otherwise it returns obs.Value.
func (cs ClassSelector) ValueForObservation(obs StoredMeasurement) (float64, bool) {
	if cs.SecondaryKey != "" {
		val, ok := obs.Secondaries[cs.SecondaryKey]
		return val, ok
	}
	return obs.Value, true
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
