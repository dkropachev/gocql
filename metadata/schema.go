// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"bytes"
	"fmt"
	"maps"
)

// Keyspace is the schema of a keyspace: its replication and the tables,
// views, types, functions, aggregates and indexes in it, as far as Mode
// loads them.
//
// The driver fills it; its zero value is empty.
type Keyspace struct {
	// StrategyOptions holds the replication options other than the class,
	// such as replication factors by datacenter.
	StrategyOptions map[string]string
	// Tables holds the keyspace's tables by name.
	Tables map[string]*Table
	// Functions holds the user-defined functions by name, or by
	// name(argtype,...) for an overloaded name.
	Functions map[string]*Function
	// Aggregates holds the user-defined aggregates, keyed like Functions.
	Aggregates map[string]*Aggregate
	// Types holds the user-defined types by name.
	Types map[string]*Type
	// Indexes holds the secondary indexes by name.
	Indexes map[string]*Index
	// Views holds the materialized views by name. A view that backs a
	// secondary index is listed in Indexes instead.
	Views map[string]*View
	// ConsistencyMode is "global" for a ScyllaDB tablet keyspace created with
	// consistency = 'global' (a strongly consistent keyspace), and empty
	// otherwise. It is read only when the control connection negotiated
	// TABLETS_ROUTING_V2_EXPERIMENTAL, the extension that reports tablet
	// leaders.
	ConsistencyMode string
	// Name is the keyspace's name.
	Name string
	// StrategyClass is the replication strategy class, such as
	// "org.apache.cassandra.locator.NetworkTopologyStrategy".
	StrategyClass string
	state         keyspaceState
	// InitialTablets is the tablet count a tablet keyspace was created with.
	// Zero lets the server choose.
	InitialTablets int
	// TabletBased reports a ScyllaDB keyspace that distributes its data in
	// tablets rather than vnodes.
	TabletBased bool
	// DurableWrites reports whether writes go through the commit log.
	DurableWrites bool
}

// Clone returns a shallow copy of the keyspace metadata with
// cloned Tables, Indexes, Views, and tablesInvalidated maps so that mutations
// do not race with concurrent readers of the original.
func (ks *Keyspace) Clone() *Keyspace {
	cloned := &Keyspace{
		TabletBased: ks.TabletBased, InitialTablets: ks.InitialTablets, ConsistencyMode: ks.ConsistencyMode,
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
	cloned.state.TabletsOption = ks.state.TabletsOption
	if ks.state.InternalTablesInvalidated != nil {
		cloned.state.InternalTablesInvalidated = maps.Clone(ks.state.InternalTablesInvalidated)
	}
	return cloned
}

// Table is the schema of a table (a column family).
//
// The driver fills it; its zero value is empty.
type Table struct {
	// ID is the table's UUID, in its string form.
	ID string
	// Columns holds the table's columns by name.
	Columns map[string]*Column
	// Extensions holds the table's schema extensions by name, as raw bytes.
	Extensions map[string][]byte
	// Keyspace is the name of the table's keyspace.
	Keyspace string
	// Name is the table's name.
	Name string
	// PartitionKey lists the partition key columns, in key order.
	PartitionKey []*Column
	// ClusteringColumns lists the clustering columns, in key order.
	ClusteringColumns []*Column
	// OrderedColumns lists the column names: the partition key, then the
	// clustering columns, then the others.
	OrderedColumns []string
	// Flags holds the table's flags, such as "compound".
	Flags []string
	// Options holds the table's options.
	Options TableOptions
}

// TableOptions are the options a table was created with.
//
// The driver fills it; its zero value is empty.
type TableOptions struct {
	// Caching holds the caching options.
	Caching map[string]string
	// Compaction holds the compaction options, the class included.
	Compaction map[string]string
	// Compression holds the compression options.
	Compression map[string]string
	// CDC holds ScyllaDB's change data capture options; it is empty on
	// Cassandra.
	CDC map[string]string
	// SpeculativeRetry is the server-side speculative retry setting, such as
	// "99.0PERCENTILE".
	SpeculativeRetry string
	// Comment is the table's comment.
	Comment string
	// Version is ScyllaDB's schema version UUID of the table, in its string
	// form; it is empty on Cassandra.
	Version string
	// Partitioner is the table's partitioner class on ScyllaDB, and empty
	// when the table uses the cluster's partitioner or on Cassandra.
	Partitioner string
	// GcGraceSeconds is how long tombstones are kept, in seconds.
	GcGraceSeconds int
	// MaxIndexInterval is the maximum gap between partition index samples.
	MaxIndexInterval int
	// MemtableFlushPeriodInMs is the forced memtable flush period, in
	// milliseconds; zero disables it.
	MemtableFlushPeriodInMs int
	// MinIndexInterval is the minimum gap between partition index samples.
	MinIndexInterval int
	// ReadRepairChance is the read repair probability, on servers that still
	// have the option.
	ReadRepairChance float64
	// BloomFilterFpChance is the bloom filter's false positive probability.
	BloomFilterFpChance float64
	// DefaultTimeToLive is the default TTL of written data, in seconds; zero
	// means none.
	DefaultTimeToLive int
	// DcLocalReadRepairChance is the datacenter-local read repair
	// probability, on servers that still have the option.
	DcLocalReadRepairChance float64
	// CrcCheckChance is the probability of checking checksums of compressed
	// data on read.
	CrcCheckChance float64
	// InMemory reports ScyllaDB's in_memory option; it is false on
	// Cassandra.
	InMemory bool
}

// Equals reports whether t and other hold equal options. Two nil options are
// equal, and nil never equals non-nil.
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
	// Columns holds the view's columns by name.
	Columns map[string]*Column
	// Extensions holds the view's schema extensions by name, as raw bytes.
	Extensions map[string][]byte
	// WhereClause is the view's WHERE clause.
	WhereClause string
	// BaseTableName is the name of the table the view selects from.
	BaseTableName string
	// ID is the view's UUID, in its string form.
	ID string
	// KeyspaceName is the name of the view's keyspace.
	KeyspaceName string
	// BaseTableID is the base table's UUID, in its string form.
	BaseTableID string
	// ViewName is the view's name.
	ViewName string
	// OrderedColumns lists the column names in Table.OrderedColumns order.
	OrderedColumns []string
	// PartitionKey lists the partition key columns, in key order.
	PartitionKey []*Column
	// ClusteringColumns lists the clustering columns, in key order.
	ClusteringColumns []*Column
	// Options holds the view's options.
	Options TableOptions
	// IncludeAllColumns reports a view that selects every column of the base
	// table.
	IncludeAllColumns bool
}

// Column is the schema of a column of a table or view.
//
// The driver fills it; its zero value is empty.
type Column struct {
	// Keyspace is the name of the column's keyspace.
	Keyspace string
	// Table is the name of the column's table or view.
	Table string
	// Name is the column's name.
	Name string
	// Type is the column's CQL type, as the schema spells it.
	Type string
	// ClusteringOrder is "asc" or "desc" for a clustering column, and "none"
	// or empty for any other.
	ClusteringOrder string
	// ComponentIndex is the column's position in the partition key or among
	// the clustering columns; it is -1 for other columns.
	ComponentIndex int
	// Kind is the column's role in the table.
	Kind ColumnKind
	// Order is DESC when ClusteringOrder is "desc", and ASC otherwise.
	Order ColumnOrder
}

// Equals reports whether c and other describe the same column. Two nil
// columns are equal, and nil never equals non-nil.
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
		c.Order == other.Order
}

// Function is a user-defined function.
//
// The driver fills it; its zero value is empty.
type Function struct {
	// Keyspace is the name of the function's keyspace.
	Keyspace string
	// Name is the function's name.
	Name string
	// Body is the function's source code.
	Body string
	// Language is the language of Body, such as "lua" or "java".
	Language string
	// ReturnType is the CQL type the function returns.
	ReturnType string
	// ArgumentTypes lists the CQL types of the arguments, in order.
	ArgumentTypes []string
	// ArgumentNames lists the names of the arguments, in order.
	ArgumentNames []string
	// CalledOnNullInput reports whether the function is called when an
	// argument is null, rather than returning null.
	CalledOnNullInput bool
}

// Aggregate is a user-defined aggregate.
//
// The driver fills it; its zero value is empty.
type Aggregate struct {
	privateStorage aggregateStorage
	// Keyspace is the name of the aggregate's keyspace.
	Keyspace string
	// Name is the aggregate's name.
	Name string
	// InitCond is the initial state, as CQL literal text, or empty.
	InitCond string
	// ReturnType is the CQL type the aggregate returns.
	ReturnType string
	// StateType is the CQL type of the aggregate's state.
	StateType string
	// ArgumentTypes lists the CQL types of the arguments, in order.
	ArgumentTypes []string
	// FinalFunc is the final function, or the zero Function when there is
	// none or it was not found.
	FinalFunc Function
	// StateFunc is the state function, or the zero Function when it was not
	// found.
	StateFunc Function
}

// Type is a user-defined type: its fields' names and types, in order.
//
// The driver fills it; its zero value is empty.
type Type struct {
	// Keyspace is the name of the type's keyspace.
	Keyspace string
	// Name is the type's name.
	Name string
	// FieldNames lists the names of the fields, in order.
	FieldNames []string
	// FieldTypes lists the CQL types of the fields, in FieldNames order.
	FieldTypes []string
}

// Index is a secondary index.
//
// The driver fills it; its zero value is empty.
type Index struct {
	// Name is the index's name.
	Name string
	// KeyspaceName is the name of the index's keyspace.
	KeyspaceName string
	// TableName is the name of the indexed table.
	TableName string
	// Kind is the kind of index.
	Kind IndexKind
	// Options holds the index's options, such as "target".
	Options map[string]string
	// Columns holds, on ScyllaDB, the columns of the view that backs the
	// index, by name.
	Columns map[string]*Column
	// OrderedColumns lists the backing view's column names in
	// Table.OrderedColumns order.
	OrderedColumns []string
	// PartitionKey lists the backing view's partition key columns, in key
	// order.
	PartitionKey []*Column
	// ClusteringColumns lists the backing view's clustering columns, in key
	// order.
	ClusteringColumns []*Column
}

// Equals reports whether t and other have the same ID, keyspace, name,
// columns, options, flags and extensions. Two nil tables are equal, and nil
// never equals non-nil.
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
// stores it; a kind the driver does not name keeps the server's text. The
// zero IndexKind is empty and names no kind. Later releases may add values,
// so a switch over them needs a default case.
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
	TabletsOption             bool
}
