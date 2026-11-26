package aggregation_test

import (
	"context"
	"fmt"
	"time"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/aggregation"
)

// SensorReading represents an IoT telemetry reading.
type SensorReading struct {
	DeviceID string
	Voltage  float64
}

// ExampleBatch demonstrates micro-batching sensor readings for bulk database inserts.
func ExampleBatch() {
	ctx := context.Background()

	readings := make(chan SensorReading, 5)
	readings <- SensorReading{DeviceID: "sensor-1", Voltage: 3.3}
	readings <- SensorReading{DeviceID: "sensor-2", Voltage: 3.2}
	readings <- SensorReading{DeviceID: "sensor-3", Voltage: 3.4}
	close(readings)

	dbInsertBatches := make(chan []SensorReading, 5)
	inStream := streaming.NewChanStream(readings, nil)
	outStream := streaming.NewChanStream(nil, dbInsertBatches)

	go func() {
		defer close(dbInsertBatches)
		// Micro-batch either every 2 records OR every 100ms
		_ = aggregation.Batch(ctx, aggregation.BatchConfig{
			MaxSize: 2,
			MaxWait: 100 * time.Millisecond,
		}, inStream, outStream)
	}()

	batchNum := 1
	for batch := range dbInsertBatches {
		fmt.Printf("Bulk-insert batch %d: %d records\n", batchNum, len(batch))
		batchNum++
	}

	// Output:
	// Bulk-insert batch 1: 2 records
	// Bulk-insert batch 2: 1 records
}
