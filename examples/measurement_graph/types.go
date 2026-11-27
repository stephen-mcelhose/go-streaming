package measurement_graph

import (
	"github.com/stephen-mcelhose/go-streaming/examples/measurement_graph/storage"
)

// Re-export domain types from storage layer for convenience
type (
	KeyValue[K comparable, V any]                 = storage.KeyValue[K, V]
	EntityIdentity                                = storage.EntityIdentity
	StreamingMeasurement[N storage.OrderedNumber] = storage.StreamingMeasurement[N]
	NodeCoordinate                                = storage.NodeCoordinate
	StoredMeasurement                             = storage.StoredMeasurement
)
