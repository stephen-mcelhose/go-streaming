//go:build spanner
// +build spanner

package storage_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming/examples/measurement_graph/storage"
)

func TestLiveSpannerGraphStore_Integration(t *testing.T) {
	ctx := context.Background()

	emulatorHost := os.Getenv("SPANNER_EMULATOR_HOST")
	if emulatorHost == "" {
		emulatorHost = "localhost:9010"
		_ = os.Setenv("SPANNER_EMULATOR_HOST", emulatorHost)
	}

	projectID := "test-project"
	instanceID := "test-instance"
	dbID := fmt.Sprintf("test_graph_%d", time.Now().UnixNano()%1000000)

	// 1. Create Instance in Emulator
	instAdmin, err := instance.NewInstanceAdminClient(ctx)
	require.NoError(t, err)
	defer instAdmin.Close()

	_, _ = instAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectID),
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", projectID),
			DisplayName: "Test Instance",
			NodeCount:   1,
		},
	})

	// 2. Provision Database with GQL Property Graph Schema
	dbAdmin, err := database.NewDatabaseAdminClient(ctx)
	require.NoError(t, err)
	defer dbAdmin.Close()

	ddlStatements := []string{
		`CREATE TABLE Entity (
			entity_key STRING(256) NOT NULL,
			business_unit STRING(64) NOT NULL,
			entity_type STRING(64) NOT NULL,
			entity_id STRING(128) NOT NULL,
		) PRIMARY KEY (entity_key)`,

		`CREATE TABLE Class (
			class_id STRING(64) NOT NULL,
		) PRIMARY KEY (class_id)`,

		`CREATE TABLE Instance (
			instance_id STRING(64) NOT NULL,
			class_id STRING(64) NOT NULL,
		) PRIMARY KEY (instance_id)`,

		`CREATE TABLE Observation (
			node_key STRING(512) NOT NULL,
			entity_key STRING(256) NOT NULL,
			primary_code STRING(64) NOT NULL,
			class_id STRING(64) NOT NULL,
			instance_id STRING(64) NOT NULL,
			val FLOAT64 NOT NULL,
			ts INT64 NOT NULL,
		) PRIMARY KEY (node_key)`,

		`CREATE PROPERTY GRAPH MeasurementOntology
		  NODE TABLES (
			Entity,
			Class,
			Instance,
			Observation
		  )
		  EDGE TABLES (
			Observation AS Observed
			  SOURCE KEY (entity_key) REFERENCES Entity (entity_key)
			  DESTINATION KEY (node_key) REFERENCES Observation (node_key)
			  LABEL has_observation,
			Observation AS InstanceOf
			  SOURCE KEY (node_key) REFERENCES Observation (node_key)
			  DESTINATION KEY (instance_id) REFERENCES Instance (instance_id)
			  LABEL is_instance_of,
			Instance AS MemberOf
			  SOURCE KEY (instance_id) REFERENCES Instance (instance_id)
			  DESTINATION KEY (class_id) REFERENCES Class (class_id)
			  LABEL is_member_of
		  )`,
	}

	op, err := dbAdmin.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", dbID),
		ExtraStatements: ddlStatements,
	})
	require.NoError(t, err)

	_, err = op.Wait(ctx)
	require.NoError(t, err)

	// 3. Connect Live Native Client
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbID)
	rawClient, err := spanner.NewClient(ctx, dbPath)
	require.NoError(t, err)
	defer rawClient.Close()

	nativeClient := storage.NewNativeSpannerClient(rawClient)
	store := storage.NewSpannerGraphStore(nativeClient)
	defer store.Close()

	// 4. Test Live Put & Get via GQL
	meas := storage.StoredMeasurement{
		Node: storage.NodeCoordinate{
			Entity:      storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: "plot_live_01"},
			PrimaryCode: "GRAIN_YIELD",
			ClassID:     "CLASS_GRAIN_YIELD",
			InstanceID:  "COMBINE_ROTARY",
		},
		Value:     188.5,
		Timestamp: time.Now(),
	}

	err = store.Put(ctx, meas)
	require.NoError(t, err)

	got, found, err := store.Get(ctx, meas.Node)
	require.NoError(t, err)
	require.True(t, found)
	require.InDelta(t, 188.5, got.Value, 1e-6)

	// 5. Test Live Multi-Hop GetByClass Traversal
	byClass, err := store.GetByClass(ctx, meas.Node.Entity, "CLASS_GRAIN_YIELD")
	require.NoError(t, err)
	require.Len(t, byClass, 1)
	require.InDelta(t, 188.5, byClass[0].Value, 1e-6)

	// 6. Test Live InvalidateEntity
	err = store.InvalidateEntity(ctx, meas.Node.Entity)
	require.NoError(t, err)

	byEntityAfter, err := store.GetByEntity(ctx, meas.Node.Entity)
	require.NoError(t, err)
	require.Empty(t, byEntityAfter)

	// 7. Test Live InvalidateFleet (Sensor Fleet Revocation across Entities)
	for i := 0; i < 5; i++ {
		_ = store.Put(ctx, storage.StoredMeasurement{
			Node: storage.NodeCoordinate{
				Entity:      storage.EntityIdentity{BusinessUnit: "Agronomy", EntityType: "PLOT", EntityID: fmt.Sprintf("plot_fleet_%d", i)},
				PrimaryCode: "GRAIN_YIELD",
				ClassID:     "CLASS_GRAIN_YIELD",
				InstanceID:  "COMBINE_BROKEN",
			},
			Value: 175.0,
		})
	}

	deletedCount, err := store.InvalidateFleet(ctx, "COMBINE_BROKEN")
	require.NoError(t, err)
	require.Equal(t, int64(5), deletedCount)
}
