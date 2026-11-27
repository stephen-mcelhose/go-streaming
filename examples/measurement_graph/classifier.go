package measurement_graph

// InstanceClassifier defines a classification rule mapping matching observations to:
// Primary : Class : Instance : [Characteristics...]
type InstanceClassifier struct {
	PrimaryCode     string            // Primary question code (e.g. "GRAIN_YIELD")
	ClassID         string            // Parent domain class (e.g. "CLASS_GRAIN_YIELD")
	InstanceID      string            // Concrete classifier instance (e.g. "C4220-I2")
	ClosedValues    map[string]string // Exact label matches required
	OpenKeys        []string          // Key presence required (any value accepted)
	Characteristics map[string]string // Additional identifying characteristics attached to placed node
}

// Matches evaluates whether a measurement satisfies this instance selector rule.
func (rule InstanceClassifier) Matches(m StreamingMeasurement[float64]) bool {
	if m.Primary.Key != rule.PrimaryCode {
		return false
	}
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
// to produce a resolved NodeCoordinate.
func (c *Classifier) Classify(m StreamingMeasurement[float64]) (*NodeCoordinate, bool) {
	candidates, exists := c.rulesByPrimary[m.Primary.Key]
	if !exists {
		return nil, false
	}

	for _, rule := range candidates {
		if rule.Matches(m) {
			return &NodeCoordinate{
				Entity:          m.Identity,
				PrimaryCode:     rule.PrimaryCode,
				ClassID:         rule.ClassID,
				InstanceID:      rule.InstanceID,
				Characteristics: rule.Characteristics,
			}, true
		}
	}

	return nil, false
}
