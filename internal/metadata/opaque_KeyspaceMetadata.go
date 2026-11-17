// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"github.com/scylladb/gocql/v2/internal/link"
	"github.com/scylladb/gocql/v2/metadata"
)

// KeyspaceState is the state the schema loader keeps on a metadata.Keyspace.
type KeyspaceState struct {
	// InternalTablesInvalidated names the tables whose schema changed and must
	// be reloaded.
	InternalTablesInvalidated map[string]struct{}
}

// keyspaceRep mirrors metadata.Keyspace.
type keyspaceRep struct {
	StrategyOptions map[string]string
	Tables          map[string]*metadata.Table
	Functions       map[string]*metadata.Function
	Aggregates      map[string]*metadata.Aggregate
	Types           map[string]*metadata.Type
	Indexes         map[string]*metadata.Index
	Views           map[string]*metadata.View
	state           KeyspaceState
	ConsistencyMode string
	Name            string
	StrategyClass   string
	CreateStmts     string
	TabletBased     bool
	DurableWrites   bool
}

// KeyspaceData returns the loader's state on v.
func KeyspaceData(v *metadata.Keyspace) *KeyspaceState { return &link.Mirror[keyspaceRep](v).state }

// removeTableData drops tableName and its indexes and views from ks.
func removeTableData(ks *metadata.Keyspace, tableName string) {
	if ks.Tables != nil {
		delete(ks.Tables, tableName)
	}
	for name, idx := range ks.Indexes {
		if idx != nil && idx.TableName == tableName {
			delete(ks.Indexes, name)
		}
	}
	for name, view := range ks.Views {
		if view != nil && view.BaseTableName == tableName {
			delete(ks.Views, name)
		}
	}
}

// invalidateTable drops tableName from ks and marks it for reloading.
func invalidateTable(ks *metadata.Keyspace, tableName string) {
	removeTableData(ks, tableName)
	state := KeyspaceData(ks)
	if state.InternalTablesInvalidated == nil {
		state.InternalTablesInvalidated = make(map[string]struct{})
	}
	state.InternalTablesInvalidated[tableName] = struct{}{}
}

// cqlNativeTypes are the scalar built-ins, the set package metadata's ToCQL
// also uses: a name in it is never a user type.
var cqlNativeTypes = map[string]struct{}{
	"ascii": {}, "bigint": {}, "blob": {}, "boolean": {}, "counter": {},
	"date": {}, "decimal": {}, "double": {}, "duration": {}, "float": {},
	"inet": {}, "int": {}, "smallint": {}, "text": {}, "time": {},
	"timestamp": {}, "timeuuid": {}, "tinyint": {}, "uuid": {}, "varchar": {},
	"varint": {},
}
