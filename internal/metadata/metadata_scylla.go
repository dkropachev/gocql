// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package metadata

import (
	"fmt"
	"maps"
	"strings"
	"sync"
	"sync/atomic"

	metadata2 "github.com/scylladb/gocql/v2/metadata"
)

func InternalKeyspaceMetadataRemoveTable(ks *metadata2.Keyspace, tableName string) {
	removeTableData(ks, tableName)
	if KeyspaceData(ks).InternalTablesInvalidated != nil {
		delete(KeyspaceData(ks).InternalTablesInvalidated, tableName)
	}
}

// cowTabletList implements a copy on write keyspace metadata map, its equivalent type is map[string]*KeyspaceMetadata
type InternalCowKeyspaceMetadataMap struct {
	InternalKeyspaceMap atomic.Pointer[map[string]*metadata2.Keyspace]
	mu                  sync.Mutex
}

func (c *InternalCowKeyspaceMetadataMap) InternalGet() map[string]*metadata2.Keyspace {
	l := c.InternalKeyspaceMap.Load()
	if l == nil {
		return nil
	}
	return *l
}

func (c *InternalCowKeyspaceMetadataMap) InternalGetKeyspace(keyspaceName string) (*metadata2.Keyspace, bool) {
	m := c.InternalKeyspaceMap.Load()
	if m == nil {
		return nil, false
	}
	val, ok := (*m)[keyspaceName]
	return val, ok
}

func (c *InternalCowKeyspaceMetadataMap) InternalSet(keyspaceName string, keyspaceMetadata *metadata2.Keyspace) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.InternalGet()

	newM := map[string]*metadata2.Keyspace{}
	for name, metadata := range m {
		newM[name] = metadata
	}
	newM[keyspaceName] = keyspaceMetadata

	c.InternalKeyspaceMap.Store(&newM)
	return true
}

func (c *InternalCowKeyspaceMetadataMap) InternalInvalidateTable(keyspaceName, tableName string) {
	c.InternalUpdateKeyspace(keyspaceName, func(ks *metadata2.Keyspace) {
		invalidateTable(ks, tableName)
	})
}

func (c *InternalCowKeyspaceMetadataMap) InternalRemoveKeyspace(keyspaceName string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.InternalGet()
	newM := maps.Clone(m)
	delete(newM, keyspaceName)

	c.InternalKeyspaceMap.Store(&newM)
}

// updateKeyspace atomically clones a keyspace's mutable maps, applies fn to
// the clone, and publishes the result. This prevents data races between
// concurrent readers and writers of the same KeyspaceMetadata.
// Returns false if the keyspace was not found (no update applied).
func (c *InternalCowKeyspaceMetadataMap) InternalUpdateKeyspace(keyspaceName string, fn func(ks *metadata2.Keyspace)) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.InternalGet()
	ks, ok := m[keyspaceName]
	if !ok || ks == nil {
		return false
	}

	cloned := shallowCloneKeyspace(ks)
	fn(cloned)

	newM := maps.Clone(m)
	newM[keyspaceName] = cloned
	c.InternalKeyspaceMap.Store(&newM)
	return true
}

func columnKindFromSchema(kind string) (metadata2.ColumnKind, error) {
	switch kind {
	case "partition_key":
		return metadata2.ColumnKindPartitionKey, nil
	case "clustering_key", "clustering":
		return metadata2.ColumnKindClusteringKey, nil
	case "regular":
		return metadata2.ColumnKindRegular, nil
	case "compact_value":
		return metadata2.ColumnKindCompact, nil
	case "static":
		return metadata2.ColumnKindStatic, nil
	default:
		return metadata2.ColumnKindUnknown, fmt.Errorf("unknown column kind: %q", kind)
	}
}

func compileMetadata(
	keyspace *metadata2.Keyspace,
	tables []metadata2.Table,
	columns []metadata2.Column,
	functions []metadata2.Function,
	aggregates []metadata2.Aggregate,
	types []metadata2.Type,
	indexes []metadata2.Index,
	views []metadata2.View,
	createStmts []byte,
) {
	keyspace.Tables = make(map[string]*metadata2.Table)
	for i := range tables {
		tables[i].Columns = make(map[string]*metadata2.Column)
		keyspace.Tables[tables[i].Name] = &tables[i]
	}
	keyspace.Functions = make(map[string]*metadata2.Function, len(functions))
	for i := range functions {
		keyspace.Functions[v2FunctionIdentity(functions[i].Name, functions[i].ArgumentTypes)] = &functions[i]
	}
	keyspace.Aggregates = make(map[string]*metadata2.Aggregate, len(aggregates))
	for _, aggregate := range aggregates {
		if f := resolveV2Function(functions, AggregateMetadataData(&aggregate).InternalFinalFunc, []string{aggregate.StateType}); f != nil {
			aggregate.FinalFunc = *f
		} else if f := uniqueV2Function(functions, AggregateMetadataData(&aggregate).InternalFinalFunc); f != nil {
			aggregate.FinalFunc = *f
		}
		if f := resolveV2Function(functions, AggregateMetadataData(&aggregate).InternalStateFunc, append([]string{aggregate.StateType}, aggregate.ArgumentTypes...)); f != nil {
			aggregate.StateFunc = *f
		} else if f := uniqueV2Function(functions, AggregateMetadataData(&aggregate).InternalStateFunc); f != nil {
			aggregate.StateFunc = *f
		}
		keyspace.Aggregates[v2FunctionIdentity(aggregate.Name, aggregate.ArgumentTypes)] = &aggregate
	}
	keyspace.Types = make(map[string]*metadata2.Type, len(types))
	for i := range types {
		keyspace.Types[types[i].Name] = &types[i]
	}
	keyspace.Indexes = make(map[string]*metadata2.Index, len(indexes))
	for i := range indexes {
		indexes[i].Columns = make(map[string]*metadata2.Column)
		keyspace.Indexes[indexes[i].Name] = &indexes[i]

	}
	keyspace.Views = make(map[string]*metadata2.View, len(views))
	for i := range views {
		v := &views[i]
		if _, ok := keyspace.Indexes[strings.TrimSuffix(v.Name, "_index")]; ok {
			continue
		}

		v.Columns = make(map[string]*metadata2.Column)
		keyspace.Views[v.Name] = v
	}

	for i := range columns {
		col := &columns[i]

		table, ok := keyspace.Tables[col.Table]
		if !ok {

			if indexName, found := strings.CutSuffix(col.Table, "_index"); found {
				ix, ok := keyspace.Indexes[indexName]
				if ok {
					ix.Columns[col.Name] = col
					ix.OrderedColumns = append(ix.OrderedColumns, col.Name)
					continue
				}
			}

			view, ok := keyspace.Views[col.Table]
			if !ok {

				continue
			}

			view.Columns[col.Name] = col
			view.OrderedColumns = append(view.OrderedColumns, col.Name)
			continue
		}

		table.Columns[col.Name] = col
		table.OrderedColumns = append(table.OrderedColumns, col.Name)
	}

	for i := range tables {
		t := &tables[i]
		t.PartitionKey, t.ClusteringColumns, t.OrderedColumns = compileColumns(t.Columns, t.OrderedColumns)
	}
	for i := range views {
		v := &views[i]
		v.PartitionKey, v.ClusteringColumns, v.OrderedColumns = compileColumns(v.Columns, v.OrderedColumns)
	}
	for i := range indexes {
		ix := &indexes[i]
		ix.PartitionKey, ix.ClusteringColumns, ix.OrderedColumns = compileColumns(ix.Columns, ix.OrderedColumns)
	}

	KeyspaceData(keyspace).ServerCQL = string(createStmts)
}

func InternalCompileTableMetadata(
	keyspace *metadata2.Keyspace,
	tables []metadata2.Table,
	columns []metadata2.Column,
	indexes []metadata2.Index,
	views []metadata2.View,
) {
	if keyspace.Tables == nil {
		keyspace.Tables = make(map[string]*metadata2.Table)
	}
	for i := range tables {
		tables[i].Columns = make(map[string]*metadata2.Column)
		keyspace.Tables[tables[i].Name] = &tables[i]
	}

	if keyspace.Indexes == nil {
		keyspace.Indexes = make(map[string]*metadata2.Index)
	}
	for name, ix := range keyspace.Indexes {
		for i := range tables {
			if ix.Table == tables[i].Name {
				delete(keyspace.Indexes, name)
			}
		}
	}
	for i := range indexes {
		indexes[i].Columns = make(map[string]*metadata2.Column)
		keyspace.Indexes[indexes[i].Name] = &indexes[i]
	}

	if keyspace.Views == nil {
		keyspace.Views = make(map[string]*metadata2.View)
	}
	for name, v := range keyspace.Views {
		for i := range tables {
			if v.BaseTable == tables[i].Name {
				delete(keyspace.Views, name)
			}
		}
	}
	for i := range views {
		v := &views[i]
		if _, ok := keyspace.Indexes[strings.TrimSuffix(v.Name, "_index")]; ok {
			continue
		}
		v.Columns = make(map[string]*metadata2.Column)
		keyspace.Views[v.Name] = v
	}

	for i := range columns {
		col := &columns[i]

		table, ok := keyspace.Tables[col.Table]
		if !ok {
			if indexName, found := strings.CutSuffix(col.Table, "_index"); found {
				ix, ok := keyspace.Indexes[indexName]
				if ok {
					ix.Columns[col.Name] = col
					ix.OrderedColumns = append(ix.OrderedColumns, col.Name)
					continue
				}
			}

			view, ok := keyspace.Views[col.Table]
			if !ok {
				continue
			}

			view.Columns[col.Name] = col
			view.OrderedColumns = append(view.OrderedColumns, col.Name)
			continue
		}

		table.Columns[col.Name] = col
		table.OrderedColumns = append(table.OrderedColumns, col.Name)
	}

	for i := range tables {
		t := &tables[i]
		t.PartitionKey, t.ClusteringColumns, t.OrderedColumns = compileColumns(t.Columns, t.OrderedColumns)
	}
	for i := range views {
		v := &views[i]
		v.PartitionKey, v.ClusteringColumns, v.OrderedColumns = compileColumns(v.Columns, v.OrderedColumns)
	}
	for i := range indexes {
		ix := &indexes[i]
		ix.PartitionKey, ix.ClusteringColumns, ix.OrderedColumns = compileColumns(ix.Columns, ix.OrderedColumns)
	}
}

func compileColumns(columns map[string]*metadata2.Column, orderedColumns []string) (
	partitionKey, clusteringColumns []*metadata2.Column, sortedColumns []string) {
	clusteringColumnCount := componentColumnCountOfType(columns, metadata2.ColumnKindClusteringKey)
	clusteringColumns = make([]*metadata2.Column, clusteringColumnCount)

	partitionKeyCount := componentColumnCountOfType(columns, metadata2.ColumnKindPartitionKey)
	partitionKey = make([]*metadata2.Column, partitionKeyCount)

	var otherColumns []string
	for _, columnName := range orderedColumns {
		column := columns[columnName]
		if column.Kind == metadata2.ColumnKindPartitionKey {
			partitionKey[column.ComponentIndex] = column
		} else if column.Kind == metadata2.ColumnKindClusteringKey {
			clusteringColumns[column.ComponentIndex] = column
		} else {
			otherColumns = append(otherColumns, columnName)
		}
	}

	sortedColumns = orderedColumns[:0]
	for _, pk := range partitionKey {
		sortedColumns = append(sortedColumns, pk.Name)
	}
	for _, ck := range clusteringColumns {
		sortedColumns = append(sortedColumns, ck.Name)
	}
	for _, oc := range otherColumns {
		sortedColumns = append(sortedColumns, oc)
	}

	return
}

func componentColumnCountOfType(columns map[string]*metadata2.Column, kind metadata2.ColumnKind) int {
	maxComponentIndex := -1
	for _, column := range columns {
		if column.Kind == kind && column.ComponentIndex > maxComponentIndex {
			maxComponentIndex = column.ComponentIndex
		}
	}
	return maxComponentIndex + 1
}

const InternalColumnMetadataColumns = `table_name, column_name, clustering_order, type, kind, position`
