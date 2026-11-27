// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata_test

import (
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
)

func populatedFunction() *metadata.Function {
	return &metadata.Function{
		Keyspace: "ks", Name: "fn", Body: "return a;", Language: "lua", ReturnType: "int",
		ArgumentTypes: []string{"int"}, ArgumentNames: []string{"a"}, CalledOnNullInput: true,
	}
}

func populatedType() *metadata.Type {
	return &metadata.Type{Keyspace: "ks", Name: "addr", FieldNames: []string{"street"}, FieldTypes: []string{"text"}}
}

func populatedView() *metadata.View {
	t := populatedTableMetadata()
	return &metadata.View{
		Columns: t.Columns, Extensions: t.Extensions, WhereClause: "pk IS NOT NULL", BaseTable: "tbl",
		Keyspace: "ks", Name: "v", OrderedColumns: t.OrderedColumns, PartitionKey: t.PartitionKey,
		ClusteringColumns: t.ClusteringColumns, Options: t.Options, ID: cql.UUID{1}, BaseTableID: cql.UUID{2},
		IncludeAllColumns: true,
	}
}

func populatedIndex() *metadata.Index {
	t := populatedTableMetadata()
	return &metadata.Index{
		Name: "ix", Keyspace: "ks", Table: "tbl", Kind: metadata.IndexKindComposites,
		Options: map[string]string{"target": "ck"}, Columns: t.Columns, OrderedColumns: t.OrderedColumns,
		PartitionKey: t.PartitionKey, ClusteringColumns: t.ClusteringColumns,
	}
}

func populatedKeyspace() *metadata.Keyspace {
	return &metadata.Keyspace{
		StrategyOptions: map[string]string{"dc1": "3"},
		Tables:          map[string]*metadata.Table{"tbl": populatedTableMetadata()},
		Functions:       map[string]*metadata.Function{"fn(int)": populatedFunction()},
		Aggregates: map[string]*metadata.Aggregate{"agg(int)": {
			Keyspace: "ks", Name: "agg", StateType: "int", ArgumentTypes: []string{"int"},
			StateFunc: cql.Some(*populatedFunction()),
		}},
		Types:           map[string]*metadata.Type{"addr": populatedType()},
		Indexes:         map[string]*metadata.Index{"ix": populatedIndex()},
		Views:           map[string]*metadata.View{"v": populatedView()},
		ConsistencyMode: "global",
		Name:            "ks",
		StrategyClass:   "NetworkTopologyStrategy",
		InitialTablets:  8,
		TabletBased:     true,
		DurableWrites:   true,
	}
}

// TestSchemaEqual checks that the Equal methods of Keyspace, View, Index,
// Type and Function compare every field, and handle nil as Table.Equal does.
func TestSchemaEqual(t *testing.T) {
	t.Parallel()
	for name, c := range map[string]struct {
		populated func() any
		equal     func(a, b any) bool
		nilEqual  func() (bool, bool)
	}{
		"Keyspace": {func() any { return populatedKeyspace() }, func(a, b any) bool { return a.(*metadata.Keyspace).Equal(b.(*metadata.Keyspace)) },
			func() (bool, bool) { var n *metadata.Keyspace; return n.Equal(nil), n.Equal(populatedKeyspace()) }},
		"View": {func() any { return populatedView() }, func(a, b any) bool { return a.(*metadata.View).Equal(b.(*metadata.View)) },
			func() (bool, bool) { var n *metadata.View; return n.Equal(nil), n.Equal(populatedView()) }},
		"Index": {func() any { return populatedIndex() }, func(a, b any) bool { return a.(*metadata.Index).Equal(b.(*metadata.Index)) },
			func() (bool, bool) { var n *metadata.Index; return n.Equal(nil), n.Equal(populatedIndex()) }},
		"Type": {func() any { return populatedType() }, func(a, b any) bool { return a.(*metadata.Type).Equal(b.(*metadata.Type)) },
			func() (bool, bool) { var n *metadata.Type; return n.Equal(nil), n.Equal(populatedType()) }},
		"Function": {func() any { return populatedFunction() }, func(a, b any) bool { return a.(*metadata.Function).Equal(b.(*metadata.Function)) },
			func() (bool, bool) { var n *metadata.Function; return n.Equal(nil), n.Equal(populatedFunction()) }},
	} {
		t.Run(name, func(t *testing.T) {
			if !c.equal(c.populated(), c.populated()) {
				t.Fatal("Equal() = false for two identically populated values")
			}
			if both, one := c.nilEqual(); !both || one {
				t.Fatalf("nil handling: nil.Equal(nil) = %v, nil.Equal(non-nil) = %v", both, one)
			}
			assertEveryFieldCompared(t, c.populated(), c.equal)
		})
	}
}
