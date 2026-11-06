// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package metadata

import "testing"

func assertPartitionKey(t *testing.T, keyspaceName, tableName string, actual, expected []*ColumnMetadata) {
	if len(expected) != len(actual) {
		t.Errorf("Expected len(%s.Tables[%s].PartitionKey) to be %v but was %v", keyspaceName, tableName, len(expected), len(actual))
	} else {
		for i := range expected {
			if expected[i].Name != actual[i].Name {
				t.Errorf("Expected %s.Tables[%s].PartitionKey[%d].Name to be '%v' but was '%v'", keyspaceName, tableName, i, expected[i].Name, actual[i].Name)
			}
			if keyspaceName != actual[i].Keyspace {
				t.Errorf("Expected %s.Tables[%s].PartitionKey[%d].Keyspace to be '%v' but was '%v'", keyspaceName, tableName, i, keyspaceName, actual[i].Keyspace)
			}
			if tableName != actual[i].Table {
				t.Errorf("Expected %s.Tables[%s].PartitionKey[%d].Table to be '%v' but was '%v'", keyspaceName, tableName, i, tableName, actual[i].Table)
			}
			if expected[i].Type != actual[i].Type {
				t.Errorf("Expected %s.Tables[%s].PartitionKey[%d].Type.Type to be %v but was %v", keyspaceName, tableName, i, expected[i].Type, actual[i].Type)
			}
			if i != actual[i].ComponentIndex {
				t.Errorf("Expected %s.Tables[%s].PartitionKey[%d].ComponentIndex to be %v but was %v", keyspaceName, tableName, i, i, actual[i].ComponentIndex)
			}
			if ColumnPartitionKey != actual[i].Kind {
				t.Errorf("Expected %s.Tables[%s].PartitionKey[%d].Kind to be '%v' but was '%v'", keyspaceName, tableName, i, ColumnPartitionKey, actual[i].Kind)
			}
		}
	}
}
func assertClusteringColumns(t *testing.T, keyspaceName, tableName string, actual, expected []*ColumnMetadata) {
	if len(expected) != len(actual) {
		t.Errorf("Expected len(%s.Tables[%s].ClusteringColumns) to be %v but was %v", keyspaceName, tableName, len(expected), len(actual))
	} else {
		for i := range expected {
			if actual[i] == nil {
				t.Fatalf("Unexpected nil value: %s.Tables[%s].ClusteringColumns[%d]", keyspaceName, tableName, i)
			}
			if expected[i].Name != actual[i].Name {
				t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].Name to be '%v' but was '%v'", keyspaceName, tableName, i, expected[i].Name, actual[i].Name)
			}
			if keyspaceName != actual[i].Keyspace {
				t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].Keyspace to be '%v' but was '%v'", keyspaceName, tableName, i, keyspaceName, actual[i].Keyspace)
			}
			if tableName != actual[i].Table {
				t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].Table to be '%v' but was '%v'", keyspaceName, tableName, i, tableName, actual[i].Table)
			}
			if expected[i].Type != actual[i].Type {
				t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].Type.Type to be %v but was %v", keyspaceName, tableName, i, expected[i].Type, actual[i].Type)
			}
			if i != actual[i].ComponentIndex {
				t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].ComponentIndex to be %v but was %v", keyspaceName, tableName, i, i, actual[i].ComponentIndex)
			}
			if expected[i].Order != actual[i].Order {
				t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].Order to be %v but was %v", keyspaceName, tableName, i, expected[i].Order, actual[i].Order)
			}
			if ColumnClusteringKey != actual[i].Kind {
				t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].Kind to be '%v' but was '%v'", keyspaceName, tableName, i, ColumnClusteringKey, actual[i].Kind)
			}
		}
	}
}

func assertColumns(t *testing.T, keyspaceName, tableName string, actual, expected map[string]*ColumnMetadata) {
	if len(expected) != len(actual) {
		eKeys := make([]string, 0, len(expected))
		for key := range expected {
			eKeys = append(eKeys, key)
		}
		aKeys := make([]string, 0, len(actual))
		for key := range actual {
			aKeys = append(aKeys, key)
		}
		t.Errorf("Expected len(%s.Tables[%s].Columns) to be %v (keys:%v) but was %v (keys:%v)", keyspaceName, tableName, len(expected), eKeys, len(actual), aKeys)
	} else {
		for keyC := range expected {
			ec := expected[keyC]
			ac, found := actual[keyC]

			if !found {
				t.Errorf("Expected %s.Tables[%s].Columns[%s] but was not found", keyspaceName, tableName, keyC)
			} else {
				if keyC != ac.Name {
					t.Errorf("Expected %s.Tables[%s].Columns[%s].Name to be '%v' but was '%v'", keyspaceName, tableName, keyC, keyC, tableName)
				}
				if keyspaceName != ac.Keyspace {
					t.Errorf("Expected %s.Tables[%s].Columns[%s].Keyspace to be '%v' but was '%v'", keyspaceName, tableName, keyC, keyspaceName, ac.Keyspace)
				}
				if tableName != ac.Table {
					t.Errorf("Expected %s.Tables[%s].Columns[%s].Table to be '%v' but was '%v'", keyspaceName, tableName, keyC, tableName, ac.Table)
				}
				if ec.Type != ac.Type {
					t.Errorf("Expected %s.Tables[%s].Columns[%s].Type.Type to be %v but was %v", keyspaceName, tableName, keyC, ec.Type, ac.Type)
				}
				if ec.Order != ac.Order {
					t.Errorf("Expected %s.Tables[%s].Columns[%s].Order to be %v but was %v", keyspaceName, tableName, keyC, ec.Order, ac.Order)
				}
				if ec.Kind != ac.Kind {
					t.Errorf("Expected %s.Tables[%s].Columns[%s].Kind to be '%v' but was '%v'", keyspaceName, tableName, keyC, ec.Kind, ac.Kind)
				}
			}
		}
	}
}

func assertOrderedColumns(t *testing.T, keyspaceName, tableName string, actual, expected []string) {
	if len(expected) != len(actual) {
		t.Errorf("Expected len(%s.Tables[%s].OrderedColumns to be %v but was %v", keyspaceName, tableName, len(expected), len(actual))
	} else {
		for i, eoc := range expected {
			aoc := actual[i]
			if eoc != aoc {
				t.Errorf("Expected %s.Tables[%s].OrderedColumns[%d] to be %s, but was %s", keyspaceName, tableName, i, eoc, aoc)
			}
		}
	}
}

func assertTableMetadata(t *testing.T, keyspaceName string, actual, expected map[string]*TableMetadata) {
	if len(expected) != len(actual) {
		t.Errorf("Expected len(%s.Tables) to be %v but was %v", keyspaceName, len(expected), len(actual))
	}
	for keyT := range expected {
		et := expected[keyT]
		at, found := actual[keyT]

		if !found {
			t.Errorf("Expected %s.Tables[%s] but was not found", keyspaceName, keyT)
		} else {
			if keyT != at.Name {
				t.Errorf("Expected %s.Tables[%s].Name to be %v but was %v", keyspaceName, keyT, keyT, at.Name)
			}
			assertPartitionKey(t, keyspaceName, keyT, at.PartitionKey, et.PartitionKey)
			assertClusteringColumns(t, keyspaceName, keyT, at.ClusteringColumns, et.ClusteringColumns)
			assertColumns(t, keyspaceName, keyT, at.Columns, et.Columns)
			assertOrderedColumns(t, keyspaceName, keyT, at.OrderedColumns, et.OrderedColumns)
		}
	}
}

func assertViewsMetadata(t *testing.T, keyspaceName string, actual, expected map[string]*ViewMetadata) {
	if len(expected) != len(actual) {
		t.Errorf("Expected len(%s.Views) to be %v but was %v", keyspaceName, len(expected), len(actual))
	}
	for keyT := range expected {
		et := expected[keyT]
		at, found := actual[keyT]

		if !found {
			t.Errorf("Expected %s.Views[%s] but was not found", keyspaceName, keyT)
		} else {
			if keyT != at.ViewName {
				t.Errorf("Expected %s.Views[%s].Name to be %v but was %v", keyspaceName, keyT, keyT, at.ViewName)
			}
			assertPartitionKey(t, keyspaceName, keyT, at.PartitionKey, et.PartitionKey)
			assertClusteringColumns(t, keyspaceName, keyT, at.ClusteringColumns, et.ClusteringColumns)
			assertColumns(t, keyspaceName, keyT, at.Columns, et.Columns)
			assertOrderedColumns(t, keyspaceName, keyT, at.OrderedColumns, et.OrderedColumns)
		}
	}
}

func assertIndicesMetadata(t *testing.T, keyspaceName string, actual, expected map[string]*IndexMetadata) {
	if len(expected) != len(actual) {
		t.Errorf("Expected len(%s.Indexes) to be %v but was %v", keyspaceName, len(expected), len(actual))
	}
	for key := range expected {
		viewName := key + "_index"
		et := expected[key]
		at, found := actual[key]

		if !found {
			t.Errorf("Expected %s.Indexes[%s] but was not found", keyspaceName, key)
		} else {
			if et.Name != at.Name {
				t.Errorf("Expected %s.Indexes[%s].Name to be %v but was %v", keyspaceName, key, et.Name, at.Name)
			}
			assertPartitionKey(t, keyspaceName, viewName, at.PartitionKey, et.PartitionKey)
			assertClusteringColumns(t, keyspaceName, viewName, at.ClusteringColumns, et.ClusteringColumns)
			assertColumns(t, keyspaceName, viewName, at.Columns, et.Columns)
			assertOrderedColumns(t, keyspaceName, viewName, at.OrderedColumns, et.OrderedColumns)
		}
	}
}

func assertKeyspaceMetadata(t *testing.T, actual, expected *KeyspaceMetadata) {
	assertTableMetadata(t, expected.Name, actual.Tables, expected.Tables)
	assertViewsMetadata(t, expected.Name, actual.Views, expected.Views)
	assertIndicesMetadata(t, expected.Name, actual.Indexes, expected.Indexes)
}
