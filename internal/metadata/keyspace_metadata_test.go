/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package metadata

import (
	"testing"

	"github.com/scylladb/gocql/v2/metadata"
)

func TestKeyspaceMetadata(t *testing.T) {
	t.Run("Compile", func(t *testing.T) {
		t.Parallel()

		keyspace := &metadata.Keyspace{
			Name: "V2Keyspace",
		}
		tables := []metadata.Table{
			{
				Keyspace: "V2Keyspace",
				Name:     "Table1",
			},
			{
				Keyspace: "V2Keyspace",
				Name:     "Table2",
			},
		}
		columns := []metadata.Column{
			{
				Keyspace:       "V2Keyspace",
				Table:          "Table1",
				Name:           "KEY1",
				Kind:           metadata.ColumnKindPartitionKey,
				ComponentIndex: 0,
				Type:           "text",
			},
			{
				Keyspace:       "V2Keyspace",
				Table:          "Table1",
				Name:           "Key1",
				Kind:           metadata.ColumnKindPartitionKey,
				ComponentIndex: 0,
				Type:           "text",
			},
			{
				Keyspace:       "V2Keyspace",
				Table:          "Table2",
				Name:           "Column1",
				Kind:           metadata.ColumnKindPartitionKey,
				ComponentIndex: 0,
				Type:           "text",
			},
			{
				Keyspace:       "V2Keyspace",
				Table:          "Table2",
				Name:           "Column2",
				Kind:           metadata.ColumnKindClusteringKey,
				ComponentIndex: 0,
				Type:           "text",
			},
			{
				Keyspace:        "V2Keyspace",
				Table:           "Table2",
				Name:            "Column3",
				Kind:            metadata.ColumnKindClusteringKey,
				ComponentIndex:  1,
				Type:            "text",
				ClusteringOrder: metadata.ColumnOrderDesc,
			},
			{
				Keyspace: "V2Keyspace",
				Table:    "Table2",
				Name:     "Column4",
				Kind:     metadata.ColumnKindRegular,
				Type:     "text",
			},
			{
				Keyspace: "V2Keyspace",
				Table:    "view",
				Name:     "ColReg",
				Kind:     metadata.ColumnKindRegular,
				Type:     "text",
			},
			{
				Keyspace: "V2Keyspace",
				Table:    "view",
				Name:     "ColCK",
				Kind:     metadata.ColumnKindClusteringKey,
				Type:     "text",
			},
			{
				Keyspace: "V2Keyspace",
				Table:    "view",
				Name:     "ColPK",
				Kind:     metadata.ColumnKindPartitionKey,
				Type:     "text",
			},
			{
				Keyspace:        "V2Keyspace",
				Table:           "buckets_by_owner_index",
				Name:            "idx_token",
				Kind:            metadata.ColumnKindClusteringKey,
				ComponentIndex:  0,
				Type:            "bigint",
				ClusteringOrder: metadata.ColumnOrderAsc,
			},
			{
				Keyspace:        "V2Keyspace",
				Table:           "buckets_by_owner_index",
				Name:            "name",
				Kind:            metadata.ColumnKindClusteringKey,
				ComponentIndex:  1,
				Type:            "text",
				ClusteringOrder: metadata.ColumnOrderAsc,
			},
			{
				Keyspace: "V2Keyspace",
				Table:    "buckets_by_owner_index",
				Name:     "owner",
				Kind:     metadata.ColumnKindPartitionKey,
				Type:     "text",
			},
		}
		// Consider an index by column `owner` on the base table `buckets` with
		// partition key `name`.
		//
		// CREATE INDEX buckets_by_owner ON stg_msk_a.buckets(owner);
		indexes := []metadata.Index{
			{Name: "buckets_by_owner"},
		}
		views := []metadata.View{
			{
				Keyspace: "V2Keyspace",
				Name:     "view",
			},
			{
				Keyspace: "V2Keyspace",
				Name:     "buckets_by_owner_index",
			},
		}
		compileMetadata(keyspace, tables, columns, nil, nil, nil, indexes, views, nil)
		assertKeyspaceMetadata(
			t,
			keyspace,
			&metadata.Keyspace{
				Name: "V2Keyspace",
				Views: map[string]*metadata.View{
					"view": {
						PartitionKey: []*metadata.Column{
							{
								Name: "ColPK",
								Type: "text",
							},
						},
						ClusteringColumns: []*metadata.Column{
							{
								Name: "ColCK",
								Type: "text",
							},
						},
						OrderedColumns: []*metadata.Column{{Name: "ColPK"}, {Name: "ColCK"}, {Name: "ColReg"}},
						Columns: map[string]*metadata.Column{
							"ColPK": {
								Name: "ColPK",
								Kind: metadata.ColumnKindPartitionKey,
								Type: "text",
							},
							"ColCK": {
								Name: "ColCK",
								Kind: metadata.ColumnKindClusteringKey,
								Type: "text",
							},
							"ColReg": {
								Name: "ColReg",
								Kind: metadata.ColumnKindRegular,
								Type: "text",
							},
						},
					},
				},
				Tables: map[string]*metadata.Table{
					"Table1": {
						PartitionKey: []*metadata.Column{
							{
								Name: "Key1",
								Type: "text",
							},
						},
						ClusteringColumns: []*metadata.Column{},
						Columns: map[string]*metadata.Column{
							"KEY1": {
								Name: "KEY1",
								Type: "text",
								Kind: metadata.ColumnKindPartitionKey,
							},
							"Key1": {
								Name: "Key1",
								Type: "text",
								Kind: metadata.ColumnKindPartitionKey,
							},
						},
						OrderedColumns: []*metadata.Column{{Name: "Key1"}},
					},
					"Table2": {
						PartitionKey: []*metadata.Column{
							{
								Name: "Column1",
								Type: "text",
							},
						},
						ClusteringColumns: []*metadata.Column{
							{
								Name: "Column2",
								Type: "text",
							},
							{
								Name:            "Column3",
								Type:            "text",
								ClusteringOrder: metadata.ColumnOrderDesc,
							},
						},
						Columns: map[string]*metadata.Column{
							"Column1": {
								Name: "Column1",
								Type: "text",
								Kind: metadata.ColumnKindPartitionKey,
							},
							"Column2": {
								Name: "Column2",
								Type: "text",
								Kind: metadata.ColumnKindClusteringKey,
							},
							"Column3": {
								Name:            "Column3",
								Type:            "text",
								ClusteringOrder: metadata.ColumnOrderDesc,
								Kind:            metadata.ColumnKindClusteringKey,
							},
							"Column4": {
								Name: "Column4",
								Type: "text",
								Kind: metadata.ColumnKindRegular,
							},
						},
						OrderedColumns: []*metadata.Column{{Name: "Column1"}, {Name: "Column2"}, {Name: "Column3"}, {Name: "Column4"}},
					},
				},
				Indexes: map[string]*metadata.Index{
					"buckets_by_owner": {
						Name: "buckets_by_owner",
						BackingView: &metadata.View{
							PartitionKey: []*metadata.Column{
								{Name: "owner", Type: "text"},
							},
							ClusteringColumns: []*metadata.Column{
								{Name: "idx_token", Type: "bigint", ClusteringOrder: metadata.ColumnOrderAsc},
								{Name: "name", Type: "text", ClusteringOrder: metadata.ColumnOrderAsc},
							},
							OrderedColumns: []*metadata.Column{{Name: "owner"}, {Name: "idx_token"}, {Name: "name"}},
							Columns: map[string]*metadata.Column{
								"owner": {
									Name: "owner",
									Type: "text",
									Kind: metadata.ColumnKindPartitionKey,
								},
								"idx_token": {
									Name:            "idx_token",
									Type:            "bigint",
									ClusteringOrder: metadata.ColumnOrderAsc,
									Kind:            metadata.ColumnKindClusteringKey,
								},
								"name": {
									Name:            "name",
									Type:            "text",
									ClusteringOrder: metadata.ColumnOrderAsc,
									Kind:            metadata.ColumnKindClusteringKey,
								},
							},
						},
					},
				},
			},
		)
	})
}

// TestCompileFunctionKeys checks that functions and aggregates are keyed by
// signature whether or not their name is overloaded, so the
// metadata.Keyspace lookups find them.
func TestCompileFunctionKeys(t *testing.T) {
	t.Parallel()
	keyspace := &metadata.Keyspace{Name: "ks"}
	functions := []metadata.Function{
		{Name: "plus", ArgumentTypes: []string{"int", "int"}},
		{Name: "plus", ArgumentTypes: []string{"bigint", "bigint"}},
		{Name: "now"},
		{Name: "state", ArgumentTypes: []string{"int", "int"}, ReturnType: "int"},
	}
	aggregates := []metadata.Aggregate{{Name: "total", StateType: "int", ArgumentTypes: []string{"int"}}}
	AggregateMetadataData(&aggregates[0]).InternalStateFunc = "state"
	compileMetadata(keyspace, nil, nil, functions, aggregates, nil, nil, nil, nil)

	for _, key := range []string{"plus(int,int)", "plus(bigint,bigint)", "now()", "state(int,int)"} {
		if keyspace.Functions[key] == nil {
			t.Errorf("Functions[%q] missing; keys %v", key, keyspace.Functions)
		}
	}
	if f, ok := keyspace.Function("plus", "bigint", "bigint"); !ok || f.ArgumentTypes[0] != "bigint" {
		t.Errorf("Function(plus, bigint, bigint) = %v, %v", f, ok)
	}
	if f, ok := keyspace.Function("now"); !ok || f.Name != "now" {
		t.Errorf("Function(now) = %v, %v", f, ok)
	}
	if _, ok := keyspace.Function("plus"); ok {
		t.Error("Function(plus) found an overload with other arguments")
	}
	if got := keyspace.FunctionsNamed("plus"); len(got) != 2 || got[0].ArgumentTypes[0] != "bigint" || got[1].ArgumentTypes[0] != "int" {
		t.Errorf("FunctionsNamed(plus) = %v", got)
	}
	if got := keyspace.FunctionsNamed("minus"); got != nil {
		t.Errorf("FunctionsNamed(minus) = %v", got)
	}
	a, ok := keyspace.Aggregate("total", "int")
	if !ok || a.StateFunc.Value.Name != "state" {
		t.Fatalf("Aggregate(total, int) = %v, %v", a, ok)
	}
	if got := keyspace.AggregatesNamed("total"); len(got) != 1 || got[0] != a {
		t.Errorf("AggregatesNamed(total) = %v", got)
	}
	var none *metadata.Keyspace
	if _, ok := none.Function("plus"); ok || none.FunctionsNamed("plus") != nil || none.AggregatesNamed("total") != nil {
		t.Error("a nil Keyspace found functions")
	}
	if _, ok := none.Aggregate("total"); ok {
		t.Error("a nil Keyspace found an aggregate")
	}
}
