package measurement_graph

// InstanceClassifier defines a classification rule mapping matching observations to:
// Primary : Class : Instance : [Characteristics...]
type InstanceClassifier struct {
	PrimaryCode     string            // Primary question code (e.g. "GRAIN_YIELD")
	ClassID         string            // Parent domain class (e.g. "CLASS_GRAIN_YIELD")
	InstanceID      string            // Concrete classifier instance (e.g. "C4220-I2")
	ClosedValues    map[string]string // Exact label matches required
	OpenKeys        []string          // Key presence required (any value accepted)
	ForbiddenKeys   []string          // Negative condition: key MUST NOT exist on observation
	Characteristics map[string]string // Additional identifying characteristics attached to placed node
}

// SpecificityScore computes a quantitative specificity score for rule candidate ranking.
// Exact (closed) matches contribute higher weight than open (presence) matches.
// If the measurement violates any forbidden key or does not match required fields, score is -1 and matched is false.
func (rule InstanceClassifier) SpecificityScore(m StreamingMeasurement[float64]) (score int, matched bool) {
	if m.Primary.Key != rule.PrimaryCode {
		return -1, false
	}

	// 1. Evaluate Negative Conditions / Forbidden Keys (Alpha Negation)
	for _, forbidden := range rule.ForbiddenKeys {
		if _, exists := m.LabelValue(forbidden); exists {
			return -1, false
		}
	}

	// 2. Evaluate Exact Closed Values
	for k, expectedVal := range rule.ClosedValues {
		actualVal, exists := m.LabelValue(k)
		if !exists || actualVal != expectedVal {
			return -1, false
		}
	}

	// 3. Evaluate Open Key Presence
	for _, k := range rule.OpenKeys {
		if _, exists := m.LabelValue(k); !exists {
			return -1, false
		}
	}

	// Exact closed value matches contribute 10 points; open key presence matches contribute 1 point.
	score = len(rule.ClosedValues)*10 + len(rule.OpenKeys)
	return score, true
}

// Matches evaluates whether a measurement satisfies this instance selector rule.
func (rule InstanceClassifier) Matches(m StreamingMeasurement[float64]) bool {
	_, matched := rule.SpecificityScore(m)
	return matched
}

// Classifier resolves raw streaming measurements into concrete hierarchical NodeCoordinates.
type Classifier struct {
	rulesByPrimary map[string][]InstanceClassifier
}

// NewClassifier creates an empty measurement classifier.
func NewClassifier() *Classifier {
	return &Classifier{
		rulesByPrimary: make(map[string][]InstanceClassifier),
	}
}

// RegisterRule adds an instance classifier rule.
func (c *Classifier) RegisterRule(rule InstanceClassifier) {
	c.rulesByPrimary[rule.PrimaryCode] = append(c.rulesByPrimary[rule.PrimaryCode], rule)
}

// Classify matches a streaming measurement against registered instance selector rules
// to produce a resolved NodeCoordinate. When multiple rules match, the candidate with
// the highest specificity score (most exact value matches) is deterministically selected.
func (c *Classifier) Classify(m StreamingMeasurement[float64]) (*NodeCoordinate, bool) {
	candidates, exists := c.rulesByPrimary[m.Primary.Key]
	if !exists {
		return nil, false
	}

	var bestRule *InstanceClassifier
	bestScore := -1

	for i := range candidates {
		rule := &candidates[i]
		if score, matched := rule.SpecificityScore(m); matched {
			if score > bestScore {
				bestScore = score
				bestRule = rule
			}
		}
	}

	if bestRule != nil {
		chars := make(map[string]string)
		for k, v := range bestRule.Characteristics {
			chars[k] = v
		}

		return &NodeCoordinate{
			Entity:          m.Identity,
			PrimaryCode:     bestRule.PrimaryCode,
			ClassID:         bestRule.ClassID,
			InstanceID:      bestRule.InstanceID,
			Characteristics: chars,
		}, true
	}

	return nil, false
}
