//go:build spanner
// +build spanner

package storage

import (
	"context"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/structpb"
)

// NativeSpannerClient wraps a live Google Cloud Spanner Client to satisfy SpannerGraphClient.
type NativeSpannerClient struct {
	client *spanner.Client
}

// NewNativeSpannerClient constructs a live client from a connected *spanner.Client.
func NewNativeSpannerClient(client *spanner.Client) *NativeSpannerClient {
	return &NativeSpannerClient{client: client}
}

// ExecuteQuery executes a GQL or SQL query against Spanner and returns rows as field maps.
func (c *NativeSpannerClient) ExecuteQuery(ctx context.Context, statement string, params map[string]any) ([]map[string]any, error) {
	stmt := spanner.Statement{
		SQL:    statement,
		Params: params,
	}
	iter := c.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var rows []map[string]any
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		m := make(map[string]any)
		for _, col := range row.ColumnNames() {
			var val spanner.GenericColumnValue
			if err := row.ColumnByName(col, &val); err == nil {
				if val.Value != nil {
					switch p := val.Value.Kind.(type) {
					case *structpb.Value_StringValue:
						m[col] = p.StringValue
					case *structpb.Value_NumberValue:
						m[col] = p.NumberValue
					case *structpb.Value_BoolValue:
						m[col] = p.BoolValue
					default:
						m[col] = val.Value
					}
				}
			}
		}
		rows = append(rows, m)
	}
	return rows, nil
}

// ExecuteMutation executes an insert, update, or delete DML statement within a ReadWriteTransaction.
func (c *NativeSpannerClient) ExecuteMutation(ctx context.Context, statement string, params map[string]any) (int64, error) {
	stmt := spanner.Statement{
		SQL:    statement,
		Params: params,
	}
	var count int64
	_, err := c.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		cnt, err := txn.Update(ctx, stmt)
		count = cnt
		return err
	})
	return count, err
}

// Close closes the underlying Spanner client connection pool.
func (c *NativeSpannerClient) Close() error {
	if c.client != nil {
		c.client.Close()
	}
	return nil
}
