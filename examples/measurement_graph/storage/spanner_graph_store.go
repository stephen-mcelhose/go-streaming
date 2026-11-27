package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// SpannerGraphClient is an interface abstracting Google Cloud Spanner Graph queries and mutations.
// This allows testing in-memory/fakes or integrating directly with `cloud.google.com/go/spanner`.
type SpannerGraphClient interface {
	// ExecuteQuery executes a GQL / Spanner Graph query with parameters and returns rows.
	ExecuteQuery(ctx context.Context, statement string, params map[string]any) ([]map[string]any, error)

	// ExecuteMutation executes an insert, update, or delete DML mutation against Spanner Graph.
	ExecuteMutation(ctx context.Context, statement string, params map[string]any) (int64, error)

	// Close cleans up Spanner client sessions.
	Close() error
}

// MemorySpannerGraphClient is an in-memory test double executing GQL graph statements.
type MemorySpannerGraphClient struct {
	mu           sync.RWMutex
	entities     map[string]map[string]any
	classes      map[string]map[string]any
	instances    map[string]map[string]any
	observations map[string]map[string]any
	edges        []graphEdge
	closed       bool
}

type graphEdge struct {
	edgeType string
	src      string
	dst      string
}

// NewMemorySpannerGraphClient creates an in-memory client for Spanner Graph testing.
func NewMemorySpannerGraphClient() *MemorySpannerGraphClient {
	return &MemorySpannerGraphClient{
		entities:     make(map[string]map[string]any),
		classes:      make(map[string]map[string]any),
		instances:    make(map[string]map[string]any),
		observations: make(map[string]map[string]any),
	}
}

func (m *MemorySpannerGraphClient) ExecuteQuery(ctx context.Context, statement string, params map[string]any) ([]map[string]any, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, fmt.Errorf("spanner client is closed")
	}

	upperStmt := strings.ToUpper(statement)

	// 1. Get exact observation: MATCH (obs:Observation {nodeKey: @nodeKey})
	if strings.Contains(upperStmt, "MATCH (OBS:OBSERVATION {NODEKEY: @NODEKEY})") {
		nodeKey, _ := params["nodeKey"].(string)
		obs, found := m.observations[nodeKey]
		if !found {
			return nil, nil
		}
		return []map[string]any{obs}, nil
	}

	// 2. GetByClass: MATCH (e:Entity {key: @entityKey})-[:OBSERVED]->(obs:Observation)-[:INSTANCE_OF]->(inst:Instance)-[:MEMBER_OF]->(c:Class {id: @classID})
	if strings.Contains(upperStmt, "-[:MEMBER_OF]->(C:CLASS {ID: @CLASSID})") {
		entityKey, _ := params["entityKey"].(string)
		classID, _ := params["classID"].(string)
		var rows []map[string]any
		for _, edge := range m.edges {
			if edge.edgeType == "OBSERVED" && edge.src == entityKey {
				if obs, found := m.observations[edge.dst]; found {
					if obsClass, _ := obs["classID"].(string); obsClass == classID {
						rows = append(rows, obs)
					}
				}
			}
		}
		return rows, nil
	}

	// 3. GetByEntity: MATCH (e:Entity {key: @entityKey})-[:OBSERVED]->(obs:Observation)
	if strings.Contains(upperStmt, "MATCH (E:ENTITY {KEY: @ENTITYKEY})-[:OBSERVED]->(OBS:OBSERVATION)") {
		entityKey, _ := params["entityKey"].(string)
		var rows []map[string]any
		for _, edge := range m.edges {
			if edge.edgeType == "OBSERVED" && edge.src == entityKey {
				if obs, found := m.observations[edge.dst]; found {
					rows = append(rows, obs)
				}
			}
		}
		return rows, nil
	}

	// 4. All observations: MATCH (obs:Observation)
	if strings.Contains(upperStmt, "MATCH (OBS:OBSERVATION)") && strings.Contains(upperStmt, "RETURN OBS") {
		var rows []map[string]any
		for _, obs := range m.observations {
			rows = append(rows, obs)
		}
		return rows, nil
	}

	// 5. Count: MATCH (obs:Observation) RETURN COUNT
	if strings.Contains(upperStmt, "COUNT") {
		return []map[string]any{
			{"count": int64(len(m.observations))},
		}, nil
	}

	return nil, nil
}

func (m *MemorySpannerGraphClient) ExecuteMutation(ctx context.Context, statement string, params map[string]any) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return 0, fmt.Errorf("spanner client is closed")
	}

	upperStmt := strings.ToUpper(statement)

	// MERGE / INSERT Node & Observation
	if strings.Contains(upperStmt, "MERGE") || strings.Contains(upperStmt, "INSERT") {
		entityKey, _ := params["entityKey"].(string)
		classID, _ := params["classID"].(string)
		instanceID, _ := params["instanceID"].(string)
		nodeKey, _ := params["nodeKey"].(string)

		m.entities[entityKey] = map[string]any{
			"key":          entityKey,
			"businessUnit": params["bu"],
			"entityType":   params["type"],
			"entityID":     params["id"],
		}
		m.classes[classID] = map[string]any{"id": classID}
		m.instances[instanceID] = map[string]any{"id": instanceID}

		m.observations[nodeKey] = map[string]any{
			"nodeKey":     nodeKey,
			"entityKey":   entityKey,
			"bu":          params["bu"],
			"type":        params["type"],
			"id":          params["id"],
			"primaryCode": params["primaryCode"],
			"classID":     classID,
			"instanceID":  instanceID,
			"value":       params["val"],
			"timestamp":   params["ts"],
		}

		m.edges = append(m.edges,
			graphEdge{edgeType: "OBSERVED", src: entityKey, dst: nodeKey},
			graphEdge{edgeType: "INSTANCE_OF", src: nodeKey, dst: instanceID},
			graphEdge{edgeType: "MEMBER_OF", src: instanceID, dst: classID},
		)
		return 1, nil
	}

	// InvalidateEntity: MATCH (e:Entity {key: @entityKey})-[r:OBSERVED]->(obs:Observation) DETACH DELETE obs
	if strings.Contains(upperStmt, "DETACH DELETE OBS") && strings.Contains(upperStmt, "@ENTITYKEY") {
		entityKey, _ := params["entityKey"].(string)
		var deleted int64
		var newEdges []graphEdge
		for _, edge := range m.edges {
			if edge.edgeType == "OBSERVED" && edge.src == entityKey {
				delete(m.observations, edge.dst)
				deleted++
			} else {
				newEdges = append(newEdges, edge)
			}
		}
		m.edges = newEdges
		return deleted, nil
	}

	// Fleet Invalidation: MATCH (inst:Instance {id: @instanceID})<-[:INSTANCE_OF]-(obs:Observation) DETACH DELETE obs
	if strings.Contains(upperStmt, "DETACH DELETE OBS") && strings.Contains(upperStmt, "@INSTANCEID") {
		instanceID, _ := params["instanceID"].(string)
		var deleted int64
		var newEdges []graphEdge
		for _, edge := range m.edges {
			if edge.edgeType == "INSTANCE_OF" && edge.dst == instanceID {
				delete(m.observations, edge.src)
				deleted++
			} else {
				newEdges = append(newEdges, edge)
			}
		}
		m.edges = newEdges
		return deleted, nil
	}

	// Retract single coordinate: MATCH (obs:Observation {nodeKey: @nodeKey}) DETACH DELETE obs
	if strings.Contains(upperStmt, "DETACH DELETE OBS") && strings.Contains(upperStmt, "@NODEKEY") {
		nodeKey, _ := params["nodeKey"].(string)
		if _, found := m.observations[nodeKey]; found {
			delete(m.observations, nodeKey)
			var newEdges []graphEdge
			for _, edge := range m.edges {
				if edge.src != nodeKey && edge.dst != nodeKey {
					newEdges = append(newEdges, edge)
				}
			}
			m.edges = newEdges
			return 1, nil
		}
		return 0, nil
	}

	return 0, nil
}

func (m *MemorySpannerGraphClient) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// SpannerGraphStore implements the GraphStore interface using Google Cloud Spanner Graph
// via declarative Graph Query Language (GQL) schema queries:
// (Entity) -[:OBSERVED]-> (Observation) -[:INSTANCE_OF]-> (Instance) -[:MEMBER_OF]-> (Class)
type SpannerGraphStore struct {
	client SpannerGraphClient
}

// NewSpannerGraphStore creates a GraphStore adapter backed by a SpannerGraphClient.
func NewSpannerGraphStore(client SpannerGraphClient) *SpannerGraphStore {
	if client == nil {
		client = NewMemorySpannerGraphClient()
	}
	return &SpannerGraphStore{
		client: client,
	}
}

// Put creates or updates graph nodes and relationships in Spanner Graph.
func (s *SpannerGraphStore) Put(ctx context.Context, m StoredMeasurement) error {
	gql := `
GRAPH MeasurementOntology
MERGE (e:Entity {key: @entityKey})
SET e.businessUnit = @bu, e.entityType = @type, e.entityID = @id
MERGE (c:Class {id: @classID})
MERGE (inst:Instance {id: @instanceID})
MERGE (inst)-[:MEMBER_OF]->(c)
MERGE (obs:Observation {nodeKey: @nodeKey})
SET obs.primaryCode = @primaryCode,
    obs.classID = @classID,
    obs.instanceID = @instanceID,
    obs.value = @val,
    obs.timestamp = @ts
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

	_, err := s.client.ExecuteMutation(ctx, gql, params)
	return err
}

// Get fetches an exact Observation node by NodeCoordinate.
func (s *SpannerGraphStore) Get(ctx context.Context, node NodeCoordinate) (StoredMeasurement, bool, error) {
	gql := `
GRAPH MeasurementOntology
MATCH (obs:Observation {nodeKey: @nodeKey})
RETURN obs.nodeKey, obs.bu, obs.type, obs.id, obs.primaryCode, obs.classID, obs.instanceID, obs.value, obs.timestamp
`
	rows, err := s.client.ExecuteQuery(ctx, gql, map[string]any{"nodeKey": node.String()})
	if err != nil {
		return StoredMeasurement{}, false, err
	}
	if len(rows) == 0 {
		return StoredMeasurement{}, false, nil
	}

	obs := s.rowToStoredMeasurement(rows[0], node)
	return obs, true, nil
}

// GetByEntity fetches all observations for an entity identity.
func (s *SpannerGraphStore) GetByEntity(ctx context.Context, entity EntityIdentity) ([]StoredMeasurement, error) {
	gql := `
GRAPH MeasurementOntology
MATCH (e:Entity {key: @entityKey})-[:OBSERVED]->(obs:Observation)
RETURN obs.nodeKey, obs.bu, obs.type, obs.id, obs.primaryCode, obs.classID, obs.instanceID, obs.value, obs.timestamp
`
	rows, err := s.client.ExecuteQuery(ctx, gql, map[string]any{"entityKey": entity.String()})
	if err != nil {
		return nil, err
	}

	var results []StoredMeasurement
	for _, row := range rows {
		results = append(results, s.rowToStoredMeasurement(row, NodeCoordinate{Entity: entity}))
	}
	return results, nil
}

// GetByClass fetches all observations for an entity matching a target ClassID.
func (s *SpannerGraphStore) GetByClass(ctx context.Context, entity EntityIdentity, classID string) ([]StoredMeasurement, error) {
	gql := `
GRAPH MeasurementOntology
MATCH (e:Entity {key: @entityKey})-[:OBSERVED]->(obs:Observation)-[:INSTANCE_OF]->(inst:Instance)-[:MEMBER_OF]->(c:Class {id: @classID})
RETURN obs.nodeKey, obs.bu, obs.type, obs.id, obs.primaryCode, obs.classID, obs.instanceID, obs.value, obs.timestamp
`
	params := map[string]any{
		"entityKey": entity.String(),
		"classID":   classID,
	}
	rows, err := s.client.ExecuteQuery(ctx, gql, params)
	if err != nil {
		return nil, err
	}

	var results []StoredMeasurement
	for _, row := range rows {
		results = append(results, s.rowToStoredMeasurement(row, NodeCoordinate{Entity: entity, ClassID: classID}))
	}
	return results, nil
}

// InvalidateEntity deletes all observation nodes and relationships for an entity.
func (s *SpannerGraphStore) InvalidateEntity(ctx context.Context, entity EntityIdentity) error {
	gql := `
GRAPH MeasurementOntology
MATCH (e:Entity {key: @entityKey})-[r:OBSERVED]->(obs:Observation)
DETACH DELETE obs
`
	_, err := s.client.ExecuteMutation(ctx, gql, map[string]any{"entityKey": entity.String()})
	return err
}

// InvalidateFleet deletes all observation nodes matching a sensor fleet InstanceID across all entities.
func (s *SpannerGraphStore) InvalidateFleet(ctx context.Context, instanceID string) (int64, error) {
	gql := `
GRAPH MeasurementOntology
MATCH (inst:Instance {id: @instanceID})<-[:INSTANCE_OF]-(obs:Observation)
DETACH DELETE obs
`
	return s.client.ExecuteMutation(ctx, gql, map[string]any{"instanceID": instanceID})
}

// Retract removes a specific Observation node coordinate.
func (s *SpannerGraphStore) Retract(ctx context.Context, node NodeCoordinate) error {
	gql := `
GRAPH MeasurementOntology
MATCH (obs:Observation {nodeKey: @nodeKey})
DETACH DELETE obs
`
	_, err := s.client.ExecuteMutation(ctx, gql, map[string]any{"nodeKey": node.String()})
	return err
}

// All retrieves all stored measurements in Spanner Graph.
func (s *SpannerGraphStore) All(ctx context.Context) ([]StoredMeasurement, error) {
	gql := `
GRAPH MeasurementOntology
MATCH (obs:Observation)
RETURN obs.nodeKey, obs.bu, obs.type, obs.id, obs.primaryCode, obs.classID, obs.instanceID, obs.value, obs.timestamp
`
	rows, err := s.client.ExecuteQuery(ctx, gql, nil)
	if err != nil {
		return nil, err
	}

	var results []StoredMeasurement
	for _, row := range rows {
		results = append(results, s.rowToStoredMeasurement(row, NodeCoordinate{}))
	}
	return results, nil
}

// Count returns the total number of Observation nodes.
func (s *SpannerGraphStore) Count(ctx context.Context) (int, error) {
	gql := `
GRAPH MeasurementOntology
MATCH (obs:Observation)
RETURN COUNT(obs) AS count
`
	rows, err := s.client.ExecuteQuery(ctx, gql, nil)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	if c, ok := rows[0]["count"].(int64); ok {
		return int(c), nil
	}
	return 0, nil
}

// Close closes the underlying Spanner client session.
func (s *SpannerGraphStore) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

func (s *SpannerGraphStore) rowToStoredMeasurement(row map[string]any, defaultNode NodeCoordinate) StoredMeasurement {
	val, _ := row["value"].(float64)
	if val == 0 {
		if intVal, ok := row["value"].(int64); ok {
			val = float64(intVal)
		}
	}

	var ts time.Time
	if tsMillis, ok := row["timestamp"].(int64); ok {
		ts = time.UnixMilli(tsMillis)
	}

	bu, _ := row["bu"].(string)
	if bu == "" {
		bu = defaultNode.Entity.BusinessUnit
	}
	entityType, _ := row["type"].(string)
	if entityType == "" {
		entityType = defaultNode.Entity.EntityType
	}
	entityID, _ := row["id"].(string)
	if entityID == "" {
		entityID = defaultNode.Entity.EntityID
	}

	primaryCode, _ := row["primaryCode"].(string)
	if primaryCode == "" {
		primaryCode = defaultNode.PrimaryCode
	}
	classID, _ := row["classID"].(string)
	if classID == "" {
		classID = defaultNode.ClassID
	}
	instanceID, _ := row["instanceID"].(string)
	if instanceID == "" {
		instanceID = defaultNode.InstanceID
	}

	return StoredMeasurement{
		Node: NodeCoordinate{
			Entity: EntityIdentity{
				BusinessUnit: bu,
				EntityType:   entityType,
				EntityID:     entityID,
			},
			PrimaryCode: primaryCode,
			ClassID:     classID,
			InstanceID:  instanceID,
		},
		Value:     val,
		Timestamp: ts,
	}
}
