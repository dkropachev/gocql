// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"bytes"
	"fmt"
	"maps"
	"reflect"
)

// Keyspace is the schema of a keyspace: its replication and the tables,
// views, types, functions, aggregates and indexes in it, as far as Mode
// loads them.
//
// The driver fills it; its zero value is empty.
type Keyspace struct {
	StrategyOptions map[string]string
	Tables          map[string]*Table
	Functions       map[string]*Function
	Aggregates      map[string]*Aggregate
	Types           map[string]*Type
	Indexes         map[string]*Index
	Views           map[string]*View
	state           keyspaceState
	ConsistencyMode string
	Name            string
	StrategyClass   string
	TabletBased     bool
	DurableWrites   bool
}

// Clone returns a shallow copy of the keyspace metadata with
// cloned Tables, Indexes, Views, and tablesInvalidated maps so that mutations
// do not race with concurrent readers of the original.
func (ks *Keyspace) Clone() *Keyspace {
	cloned := &Keyspace{
		TabletBased: ks.TabletBased, ConsistencyMode: ks.ConsistencyMode,
		Name:            ks.Name,
		DurableWrites:   ks.DurableWrites,
		StrategyClass:   ks.StrategyClass,
		StrategyOptions: maps.Clone(ks.StrategyOptions),
		Tables:          maps.Clone(ks.Tables),
		Functions:       maps.Clone(ks.Functions),
		Aggregates:      maps.Clone(ks.Aggregates),
		Types:           maps.Clone(ks.Types),
		Indexes:         maps.Clone(ks.Indexes),
		Views:           maps.Clone(ks.Views),
	}
	cloned.state.ServerCQL = ks.state.ServerCQL
	if ks.state.InternalTablesInvalidated != nil {
		cloned.state.InternalTablesInvalidated = maps.Clone(ks.state.InternalTablesInvalidated)
	}
	return cloned
}

// Table is the schema of a table (a column family).
//
// The driver fills it; its zero value is empty.
type Table struct {
	ID                string
	Columns           map[string]*Column
	Extensions        map[string][]byte
	Keyspace          string
	Name              string
	PartitionKey      []*Column
	ClusteringColumns []*Column
	OrderedColumns    []string
	Flags             []string
	Options           TableOptions
}

// TableOptions are the options a table was created with.
//
// The driver fills it; its zero value is empty.
type TableOptions struct {
	Caching                 map[string]string
	Compaction              map[string]string
	Compression             map[string]string
	CDC                     map[string]string
	SpeculativeRetry        string
	Comment                 string
	Version                 string
	Partitioner             string
	GcGraceSeconds          int
	MaxIndexInterval        int
	MemtableFlushPeriodInMs int
	MinIndexInterval        int
	ReadRepairChance        float64
	BloomFilterFpChance     float64
	DefaultTimeToLive       int
	DcLocalReadRepairChance float64
	CrcCheckChance          float64
	InMemory                bool
}

func (t *TableOptions) Equals(other *TableOptions) bool {
	if t == nil || other == nil {
		return t == other // Both must be nil to be equal
	}

	if t.BloomFilterFpChance != other.BloomFilterFpChance ||
		t.Comment != other.Comment ||
		t.CrcCheckChance != other.CrcCheckChance ||
		t.DcLocalReadRepairChance != other.DcLocalReadRepairChance ||
		t.DefaultTimeToLive != other.DefaultTimeToLive ||
		t.GcGraceSeconds != other.GcGraceSeconds ||
		t.MaxIndexInterval != other.MaxIndexInterval ||
		t.MemtableFlushPeriodInMs != other.MemtableFlushPeriodInMs ||
		t.MinIndexInterval != other.MinIndexInterval ||
		t.ReadRepairChance != other.ReadRepairChance ||
		t.SpeculativeRetry != other.SpeculativeRetry ||
		t.InMemory != other.InMemory ||
		t.Partitioner != other.Partitioner ||
		t.Version != other.Version {
		return false
	}

	if !compareStringMaps(t.Caching, other.Caching) ||
		!compareStringMaps(t.Compaction, other.Compaction) ||
		!compareStringMaps(t.Compression, other.Compression) ||
		!compareStringMaps(t.CDC, other.CDC) {
		return false
	}

	return true
}

// View is the schema of a materialized view.
//
// The driver fills it; its zero value is empty.
type View struct {
	Columns                 map[string]*Column
	Extensions              map[string][]byte
	WhereClause             string
	BaseTableName           string
	ID                      string
	KeyspaceName            string
	BaseTableID             string
	ViewName                string
	OrderedColumns          []string
	PartitionKey            []*Column
	ClusteringColumns       []*Column
	Options                 TableOptions
	DcLocalReadRepairChance float64 // After Scylla 4.2 by default read_repair turned off
	ReadRepairChance        float64 // After Scylla 4.2 by default read_repair turned off
	IncludeAllColumns       bool
}

// Column is the schema of a column of a table or view.
//
// The driver fills it; its zero value is empty.
type Column struct {
	Index           ColumnIndex
	Keyspace        string
	Table           string
	Name            string
	Type            string
	ClusteringOrder string
	ComponentIndex  int
	Kind            ColumnKind
	Order           ColumnOrder
}

func (c *Column) Equals(other *Column) bool {
	if c == nil || other == nil {
		return c == other
	}

	return c.Keyspace == other.Keyspace &&
		c.Table == other.Table &&
		c.Name == other.Name &&
		c.ComponentIndex == other.ComponentIndex &&
		c.Kind == other.Kind &&
		c.Type == other.Type &&
		c.ClusteringOrder == other.ClusteringOrder &&
		c.Order == other.Order &&
		c.Index.Equals(&other.Index)
}

// Function is a user-defined function.
//
// The driver fills it; its zero value is empty.
type Function struct {
	Keyspace          string
	Name              string
	Body              string
	Language          string
	ReturnType        string
	ArgumentTypes     []string
	ArgumentNames     []string
	CalledOnNullInput bool
}

// Aggregate is a user-defined aggregate.
//
// The driver fills it; its zero value is empty.
type Aggregate struct {
	privateStorage aggregateStorage
	Keyspace       string
	Name           string
	InitCond       string
	ReturnType     string
	StateType      string
	ArgumentTypes  []string
	FinalFunc      Function
	StateFunc      Function
}

// Type is a user-defined type: its fields' names and types, in order.
//
// The driver fills it; its zero value is empty.
type Type struct {
	Keyspace   string
	Name       string
	FieldNames []string
	FieldTypes []string
}

// Index is a secondary index.
//
// The driver fills it; its zero value is empty.
type Index struct {
	Name              string
	KeyspaceName      string
	TableName         string // Name of corresponding view.
	Kind              IndexKind
	Options           map[string]string
	Columns           map[string]*Column
	OrderedColumns    []string
	PartitionKey      []*Column
	ClusteringColumns []*Column
}

func (t *Table) Equals(other *Table) bool {
	if t == nil || other == nil {
		return t == other
	}

	if t.ID != other.ID || t.Keyspace != other.Keyspace || t.Name != other.Name {
		return false
	}

	if len(t.PartitionKey) != len(other.PartitionKey) || !compareColumnSlices(t.PartitionKey, other.PartitionKey) {
		return false
	}

	if len(t.ClusteringColumns) != len(other.ClusteringColumns) || !compareColumnSlices(t.ClusteringColumns, other.ClusteringColumns) {
		return false
	}

	if len(t.Columns) != len(other.Columns) || !compareColumnsMap(t.Columns, other.Columns) {
		return false
	}

	if len(t.OrderedColumns) != len(other.OrderedColumns) || !compareStringSlices(t.OrderedColumns, other.OrderedColumns) {
		return false
	}

	if !t.Options.Equals(&other.Options) {
		return false
	}

	if len(t.Flags) != len(other.Flags) || !compareStringSlices(t.Flags, other.Flags) {
		return false
	}

	if !maps.EqualFunc(t.Extensions, other.Extensions, bytes.Equal) {
		return false
	}

	return true
}

func compareColumnSlices(a, b []*Column) bool {
	for i := range a {
		if !a[i].Equals(b[i]) {
			return false
		}
	}
	return true
}

func compareColumnsMap(a, b map[string]*Column) bool {
	for k, v := range a {
		otherValue, exists := b[k]
		if !exists || !v.Equals(otherValue) {
			return false
		}
	}
	return true
}

func compareStringSlices(a, b []string) bool {
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func compareStringMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if otherValue, exists := b[k]; !exists || v != otherValue {
			return false
		}
	}
	return true
}

// IndexKind is the kind of a secondary index, as system_schema.indexes
// stores it; a kind the driver does not name keeps the server's text. Later
// releases may add values, so a switch over them needs a default case.
type IndexKind string

// The kinds of secondary indexes.
const (
	IndexKindKeys       IndexKind = "KEYS"
	IndexKindComposites IndexKind = "COMPOSITES"
	IndexKindCustom     IndexKind = "CUSTOM"
)

// ColumnOrder is the ordering of a clustering column with regard to its
// comparator.
type ColumnOrder bool

// The orders of clustering columns.
const (
	ASC  ColumnOrder = false
	DESC ColumnOrder = true
)

// ColumnIndex is the index on a column, in the schema of older servers.
//
// The driver fills it; its zero value is empty.
type ColumnIndex struct {
	Options map[string]any
	Name    string
	Type    string
}

func (c *ColumnIndex) Equals(other *ColumnIndex) bool {
	if c == nil || other == nil {
		return c == other
	}

	if c.Name != other.Name || c.Type != other.Type {
		return false
	}

	// Compare the Options map
	if len(c.Options) != len(other.Options) {
		return false
	}
	for k, v := range c.Options {
		otherValue, exists := other.Options[k]
		if !exists || !reflect.DeepEqual(v, otherValue) {
			return false
		}
	}

	return true
}

// ColumnKind is the role of a column in its table. The zero ColumnKind is
// ColumnKindUnknown. Later releases may add values, so a switch over them needs
// a default case.
type ColumnKind int

// The kinds of columns.
const (
	ColumnKindUnknown ColumnKind = iota
	ColumnPartitionKey
	ColumnClusteringKey
	ColumnRegular
	ColumnCompact
	ColumnStatic
)

// String returns the kind name, or ColumnKind(n) for a value it does not
// define.
func (c ColumnKind) String() string {
	switch c {
	case ColumnKindUnknown:
		return "unknown"
	case ColumnPartitionKey:
		return "partition_key"
	case ColumnClusteringKey:
		return "clustering_key"
	case ColumnRegular:
		return "regular"
	case ColumnCompact:
		return "compact"
	case ColumnStatic:
		return "static"
	default:
		return fmt.Sprintf("ColumnKind(%d)", int(c))
	}
}

type keyspaceState struct {
	InternalTablesInvalidated map[string]struct{}
	ServerCQL                 string
}
