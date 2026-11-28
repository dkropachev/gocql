// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"bytes"
	"maps"
	"slices"
)

// Equal reports whether ks and other hold the same keyspace: its name,
// replication, options and every table, view, type, function, aggregate and
// index in it, compared with their Equal methods. ServerCQL is not compared.
// Two nil keyspaces are equal, and nil never equals non-nil.
func (ks *Keyspace) Equal(other *Keyspace) bool {
	if ks == nil || other == nil {
		return ks == other
	}
	return ks.Name == other.Name &&
		ks.StrategyClass == other.StrategyClass &&
		compareStringMaps(ks.StrategyOptions, other.StrategyOptions) &&
		ks.ConsistencyMode == other.ConsistencyMode &&
		ks.InitialTablets == other.InitialTablets &&
		ks.TabletBased == other.TabletBased &&
		ks.DurableWrites == other.DurableWrites &&
		maps.EqualFunc(ks.Tables, other.Tables, (*Table).Equal) &&
		maps.EqualFunc(ks.Views, other.Views, (*View).Equal) &&
		maps.EqualFunc(ks.Types, other.Types, (*Type).Equal) &&
		maps.EqualFunc(ks.Functions, other.Functions, (*Function).Equal) &&
		maps.EqualFunc(ks.Aggregates, other.Aggregates, aggregateEqual) &&
		maps.EqualFunc(ks.Indexes, other.Indexes, (*Index).Equal)
}

// Equal reports whether v and other describe the same materialized view:
// IDs, names, base table, WHERE clause, columns, options and extensions. Two
// nil views are equal, and nil never equals non-nil.
func (v *View) Equal(other *View) bool {
	if v == nil || other == nil {
		return v == other
	}
	return v.ID == other.ID &&
		v.BaseTableID == other.BaseTableID &&
		v.Keyspace == other.Keyspace &&
		v.Name == other.Name &&
		v.BaseTable == other.BaseTable &&
		v.WhereClause == other.WhereClause &&
		v.IncludeAllColumns == other.IncludeAllColumns &&
		columnsEqual(v.Columns, other.Columns, v.PartitionKey, other.PartitionKey, v.ClusteringColumns, other.ClusteringColumns, v.OrderedColumns, other.OrderedColumns) &&
		v.Options.Equal(&other.Options) &&
		maps.EqualFunc(v.Extensions, other.Extensions, bytes.Equal)
}

// Equal reports whether i and other describe the same secondary index:
// names, kind, target, options and backing view. Two nil indexes are
// equal, and nil never equals non-nil.
func (i *Index) Equal(other *Index) bool {
	if i == nil || other == nil {
		return i == other
	}
	return i.Name == other.Name &&
		i.Keyspace == other.Keyspace &&
		i.Table == other.Table &&
		i.Kind == other.Kind &&
		i.Target == other.Target &&
		i.TargetKind == other.TargetKind &&
		compareStringMaps(i.Options, other.Options) &&
		i.BackingView.Equal(other.BackingView)
}

// Equal reports whether t and other describe the same user-defined type:
// names and fields, in order. Two nil types are equal, and nil never equals
// non-nil.
func (t *Type) Equal(other *Type) bool {
	if t == nil || other == nil {
		return t == other
	}
	return t.Keyspace == other.Keyspace &&
		t.Name == other.Name &&
		slices.Equal(t.FieldNames, other.FieldNames) &&
		slices.Equal(t.FieldTypes, other.FieldTypes)
}

// Equal reports whether f and other describe the same user-defined function:
// names, arguments, return type, language, body and null handling. Two nil
// functions are equal, and nil never equals non-nil.
func (f *Function) Equal(other *Function) bool {
	if f == nil || other == nil {
		return f == other
	}
	return f.Keyspace == other.Keyspace &&
		f.Name == other.Name &&
		f.Body == other.Body &&
		f.Language == other.Language &&
		f.ReturnType == other.ReturnType &&
		slices.Equal(f.ArgumentTypes, other.ArgumentTypes) &&
		slices.Equal(f.ArgumentNames, other.ArgumentNames) &&
		f.CalledOnNullInput == other.CalledOnNullInput
}

// aggregateEqual reports whether a and b describe the same aggregate.
func aggregateEqual(a, b *Aggregate) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Keyspace == b.Keyspace &&
		a.Name == b.Name &&
		a.InitCond == b.InitCond &&
		a.ReturnType == b.ReturnType &&
		a.StateType == b.StateType &&
		slices.Equal(a.ArgumentTypes, b.ArgumentTypes) &&
		a.FinalFunc.Set == b.FinalFunc.Set && a.FinalFunc.Value.Equal(&b.FinalFunc.Value) &&
		a.StateFunc.Set == b.StateFunc.Set && a.StateFunc.Value.Equal(&b.StateFunc.Value) &&
		a.privateStorage == b.privateStorage
}

// columnsEqual compares the columns of two views: the map and the
// three lists.
func columnsEqual(a, b map[string]*Column, aPK, bPK, aCK, bCK, aAll, bAll []*Column) bool {
	return maps.EqualFunc(a, b, (*Column).Equal) &&
		slices.EqualFunc(aPK, bPK, (*Column).Equal) &&
		slices.EqualFunc(aCK, bCK, (*Column).Equal) &&
		slices.EqualFunc(aAll, bAll, (*Column).Equal)
}
