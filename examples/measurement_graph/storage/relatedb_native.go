//go:build relate
// +build relate

package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/relatedb/relate/clients/go"
)

// NativeRelateDBGraphStore implements GraphStore executing Cypher graph queries
// directly against an embedded RelateDB instance using the ontology hierarchy:
// (Entity) -[:OBSERVED]-> (Observation) -[:INSTANCE_OF]-> (Instance) -[:MEMBER_OF]-> (Class)
type NativeRelateDBGraphStore struct {
	mu     sync.Mutex
	db     *relate.Database
	conn   *relate.Connection
	closed bool
}

// NewNativeRelateDBGraphStore opens a live RelateDB database and initializes schema.
func NewNativeRelateDBGraphStore(path string) (*NativeRelateDBGraphStore, error) {
	if path == "" {
		path = ":memory:"
	}
	db, err := relate.Open(path)
	if err != nil {
		return nil, fmt.Errorf("relatedb open: %w", err)
	}

	conn, err := db.Connect()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("relatedb connect: %w", err)
	}

	return &NativeRelateDBGraphStore{
		db:   db,
		conn: conn,
	}, nil
}

// Put creates or updates the ontology hierarchy in RelateDB:
// (Entity) -[:OBSERVED]-> (Observation) -[:INSTANCE_OF]-> (Instance) -[:MEMBER_OF]-> (Class)
func (n *NativeRelateDBGraphStore) Put(ctx context.Context, m StoredMeasurement) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return fmt.Errorf("relatedb: store is closed")
	}

	cypher := `
MERGE (e:Entity {key: $entityKey})
SET e.businessUnit = $bu, e.entityType = $type, e.entityID = $id
MERGE (c:Class {id: $classID})
MERGE (inst:Instance {id: $instanceID})
MERGE (inst)-[:MEMBER_OF]->(c)
MERGE (obs:Observation {nodeKey: $nodeKey})
SET obs.primaryCode = $primaryCode,
    obs.classID = $classID,
    obs.instanceID = $instanceID,
    obs.value = $val,
    obs.timestamp = $ts
MERGE (e)-[:OBSERVED]->(obs)
MERGE (obs)-[:INSTANCE_OF]->(inst)
`
	params := map[string]any{
		"entityKey":   m.Node.Entity.String(),
		"bu":          m.Node.Entity.BusinessUnit,
		"type":        m.Node.Entity.EntityType,
		"id":          m.Node.Entity.EntityID,
		"nodeKey":     m.Node.String(),
		"primaryCode": m.Node.PrimaryCode,
		"classID":     m.Node.ClassID,
		"instanceID":  m.Node.InstanceID,
		"val":         m.Value,
		"ts":          m.Timestamp.UnixMilli(),
	}

	_, err := n.conn.QueryParams(cypher, params)
	return err
}

// Get fetches an Observation node by its NodeCoordinate key.
func (n *NativeRelateDBGraphStore) Get(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return StoredMeasurement{}, false, fmt.Errorf("relatedb: store is closed")
	}

	cypher := `
MATCH (obs:Observation {nodeKey: $nodeKey})
RETURN obs.primaryCode, obs.classID, obs.instanceID, obs.value, obs.timestamp
`
	res, err := n.conn.QueryParams(cypher, map[string]any{"nodeKey": node.String()})
	if err != nil {
		return StoredMeasurement{}, false, err
	}
	if len(res.Rows) == 0 {
		return StoredMeasurement{}, false, nil
	}

	row := res.Rows[0]
	val, ok := row[3].(float64)
	if !ok {
		if intVal, isInt := row[3].(int64); isInt {
			val = float64(intVal)
		}
	}

	var ts time.Time
	if tsMillis, ok := row[4].(int64); ok {
		ts = time.UnixMilli(tsMillis)
	}

	return StoredMeasurement{
		Node:      node,
		Value:     val,
		Timestamp: ts,
	}, true, nil
}

// GetByEntity fetches all observations linked to an Entity.
func (n *NativeRelateDBGraphStore) GetByEntity(ctx context.Context, entity EntityIdentity) ([]StoredMeasurement, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, fmt.Errorf("relatedb: store is closed")
	}

	cypher := `
MATCH (e:Entity {key: $entityKey})-[:OBSERVED]->(obs:Observation)
RETURN obs.primaryCode, obs.classID, obs.instanceID, obs.value, obs.timestamp, obs.nodeKey
`
	res, err := n.conn.QueryParams(cypher, map[string]any{"entityKey": entity.String()})
	if err != nil {
		return nil, err
	}

	var out []StoredMeasurement
	for _, row := range res.Rows {
		primary, _ := row[0].(string)
		classID, _ := row[1].(string)
		instanceID, _ := row[2].(string)
		val, ok := row[3].(float64)
		if !ok {
			if intVal, isInt := row[3].(int64); isInt {
				val = float64(intVal)
			}
		}
		var ts time.Time
		if tsMillis, ok := row[4].(int64); ok {
			ts = time.UnixMilli(tsMillis)
		}

		out = append(out, StoredMeasurement{
			Node: NodeCoordinate{
				Entity:      entity,
				PrimaryCode: primary,
				ClassID:     classID,
				InstanceID:  instanceID,
			},
			Value:     val,
			Timestamp: ts,
		})
	}
	return out, nil
}

// GetByClass traverses (Entity)-[:OBSERVED]->(Observation)-[:INSTANCE_OF]->(:Instance)-[:MEMBER_OF]->(Class {id: $classID}).
func (n *NativeRelateDBGraphStore) GetByClass(ctx context.Context, entity EntityIdentity, classID string) ([]StoredMeasurement, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, fmt.Errorf("relatedb: store is closed")
	}

	cypher := `
MATCH (e:Entity {key: $entityKey})-[:OBSERVED]->(obs:Observation)-[:INSTANCE_OF]->(:Instance)-[:MEMBER_OF]->(c:Class {id: $classID})
RETURN obs.primaryCode, obs.classID, obs.instanceID, obs.value, obs.timestamp, obs.nodeKey
`
	res, err := n.conn.QueryParams(cypher, map[string]any{
		"entityKey": entity.String(),
		"classID":   classID,
	})
	if err != nil {
		return nil, err
	}

	var out []StoredMeasurement
	for _, row := range res.Rows {
		primary, _ := row[0].(string)
		cID, _ := row[1].(string)
		instanceID, _ := row[2].(string)
		val, ok := row[3].(float64)
		if !ok {
			if intVal, isInt := row[3].(int64); isInt {
				val = float64(intVal)
			}
		}
		var ts time.Time
		if tsMillis, ok := row[4].(int64); ok {
			ts = time.UnixMilli(tsMillis)
		}

		out = append(out, StoredMeasurement{
			Node: NodeCoordinate{
				Entity:      entity,
				PrimaryCode: primary,
				ClassID:     cID,
				InstanceID:  instanceID,
			},
			Value:     val,
			Timestamp: ts,
		})
	}
	return out, nil
}

// InvalidateEntity deletes all observations and entity nodes for an entity.
func (n *NativeRelateDBGraphStore) InvalidateEntity(ctx context.Context, entity EntityIdentity) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return fmt.Errorf("relatedb: store is closed")
	}

	cypher := `
MATCH (e:Entity {key: $entityKey})
OPTIONAL MATCH (e)-[:OBSERVED]->(obs:Observation)
DETACH DELETE obs, e
`
	_, err := n.conn.QueryParams(cypher, map[string]any{"entityKey": entity.String()})
	return err
}

// Retract removes a specific observation node coordinate.
func (n *NativeRelateDBGraphStore) Retract(ctx context.Context, node NodeCoordinate) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return fmt.Errorf("relatedb: store is closed")
	}

	cypher := `
MATCH (obs:Observation {nodeKey: $nodeKey})
DETACH DELETE obs
`
	_, err := n.conn.QueryParams(cypher, map[string]any{"nodeKey": node.String()})
	return err
}

// All returns all stored measurements in RelateDB.
func (n *NativeRelateDBGraphStore) All(ctx context.Context) ([]StoredMeasurement, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, fmt.Errorf("relatedb: store is closed")
	}

	cypher := `
MATCH (e:Entity)-[:OBSERVED]->(obs:Observation)
RETURN e.bu, e.entityType, e.entityId, obs.primaryCode, obs.classID, obs.instanceID, obs.value, obs.timestamp
`
	res, err := n.conn.Query(cypher)
	if err != nil {
		return nil, err
	}

	var out []StoredMeasurement
	for _, row := range res.Rows {
		bu, _ := row[0].(string)
		entType, _ := row[1].(string)
		entID, _ := row[2].(string)
		primary, _ := row[3].(string)
		cID, _ := row[4].(string)
		instanceID, _ := row[5].(string)
		val, ok := row[6].(float64)
		if !ok {
			if intVal, isInt := row[6].(int64); isInt {
				val = float64(intVal)
			}
		}
		var ts time.Time
		if tsMillis, ok := row[7].(int64); ok {
			ts = time.UnixMilli(tsMillis)
		}

		out = append(out, StoredMeasurement{
			Node: NodeCoordinate{
				Entity: EntityIdentity{
					BusinessUnit: bu,
					EntityType:   entType,
					EntityID:     entID,
				},
				PrimaryCode: primary,
				ClassID:     cID,
				InstanceID:  instanceID,
			},
			Value:     val,
			Timestamp: ts,
		})
	}
	return out, nil
}

// Count returns the total number of Observation nodes in the RelateDB graph.
func (n *NativeRelateDBGraphStore) Count(ctx context.Context) (int, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return 0, fmt.Errorf("relatedb: store is closed")
	}

	res, err := n.conn.Query("MATCH (obs:Observation) RETURN count(obs) AS c")
	if err != nil {
		return 0, err
	}
	if len(res.Rows) == 0 {
		return 0, nil
	}

	if c, ok := res.Rows[0][0].(int64); ok {
		return int(c), nil
	}
	return 0, nil
}

// Close shuts down the connection and database.
func (n *NativeRelateDBGraphStore) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil
	}
	n.closed = true
	n.conn.Close()
	n.db.Close()
	return nil
}
