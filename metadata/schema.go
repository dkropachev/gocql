// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/scylladb/gocql/v2/cql"
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
	// Functions holds the user-defined functions by signature: the name
	// and the argument types, comma-separated without spaces, such as
	// "add(int,int)" or "now()". Function and FunctionsNamed look them up.
	Functions map[string]*Function
	// Aggregates holds the user-defined aggregates by signature, keyed like
	// Functions. Aggregate and AggregatesNamed look them up.
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
	// otherwise. It is read only when the session's connections negotiate
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

// Function returns the user-defined function name(args), where args are
// the argument types as the server writes them, such as "int" or
// "frozen<list<text>>".
func (ks *Keyspace) Function(name string, args ...string) (*Function, bool) {
	if ks == nil {
		return nil, false
	}
	f, ok := ks.Functions[signature(name, args)]
	return f, ok && f != nil
}

// FunctionsNamed returns the overloads of the user-defined function name,
// sorted by signature, or nil when there are none.
func (ks *Keyspace) FunctionsNamed(name string) []*Function {
	if ks == nil {
		return nil
	}
	return named(ks.Functions, name, func(f *Function) string { return f.Name })
}

// Aggregate returns the user-defined aggregate name(args), where args are
// the argument types, as for Function.
func (ks *Keyspace) Aggregate(name string, args ...string) (*Aggregate, bool) {
	if ks == nil {
		return nil, false
	}
	a, ok := ks.Aggregates[signature(name, args)]
	return a, ok && a != nil
}

// AggregatesNamed returns the overloads of the user-defined aggregate name,
// sorted by signature, or nil when there are none.
func (ks *Keyspace) AggregatesNamed(name string) []*Aggregate {
	if ks == nil {
		return nil
	}
	return named(ks.Aggregates, name, func(a *Aggregate) string { return a.Name })
}

// signature is the key of a function or aggregate in its Keyspace map.
func signature(name string, args []string) string {
	return name + "(" + strings.Join(args, ",") + ")"
}

// named returns the values of m called name, sorted by key.
func named[T any](m map[string]*T, name string, nameOf func(*T) string) []*T {
	var out []*T
	for _, k := range slices.Sorted(maps.Keys(m)) {
		if v := m[k]; v != nil && nameOf(v) == name {
			out = append(out, v)
		}
	}
	return out
}

// Table is the schema of a table (a column family).
//
// The driver fills it; its zero value is empty.
type Table struct {
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
	// OrderedColumns lists the columns: the partition key, then the
	// clustering columns, then the others.
	OrderedColumns []*Column
	// Flags holds the table's flags, such as "compound".
	Flags []string
	// Options holds the table's options.
	Options TableOptions
	// ID is the table's UUID.
	ID cql.UUID
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
	// Partitioner is the table's partitioner class on ScyllaDB, and empty
	// when the table uses the cluster's partitioner or on Cassandra.
	Partitioner string
	// Version is ScyllaDB's schema version UUID of the table; it is the zero
	// UUID on Cassandra.
	Version cql.UUID
	// GCGraceSeconds is how long tombstones are kept, in seconds.
	GCGraceSeconds int
	// MaxIndexInterval is the maximum gap between partition index samples.
	MaxIndexInterval int
	// MemtableFlushPeriodInMS is the forced memtable flush period, in
	// milliseconds; zero disables it.
	MemtableFlushPeriodInMS int
	// MinIndexInterval is the minimum gap between partition index samples.
	MinIndexInterval int
	// ReadRepairChance is the read repair probability, on servers that still
	// have the option.
	ReadRepairChance float64
	// BloomFilterFPChance is the bloom filter's false positive probability.
	BloomFilterFPChance float64
	// DefaultTimeToLive is the default TTL of written data, in seconds; zero
	// means none.
	DefaultTimeToLive int
	// DCLocalReadRepairChance is the datacenter-local read repair
	// probability, on servers that still have the option.
	DCLocalReadRepairChance float64
	// CRCCheckChance is the probability of checking checksums of compressed
	// data on read.
	CRCCheckChance float64
	// InMemory reports ScyllaDB's in_memory option; it is false on
	// Cassandra.
	InMemory bool
}

// Equal reports whether t and other hold equal options. Two nil options are
// equal, and nil never equals non-nil.
func (t *TableOptions) Equal(other *TableOptions) bool {
	if t == nil || other == nil {
		return t == other // Both must be nil to be equal
	}

	if t.BloomFilterFPChance != other.BloomFilterFPChance ||
		t.Comment != other.Comment ||
		t.CRCCheckChance != other.CRCCheckChance ||
		t.DCLocalReadRepairChance != other.DCLocalReadRepairChance ||
		t.DefaultTimeToLive != other.DefaultTimeToLive ||
		t.GCGraceSeconds != other.GCGraceSeconds ||
		t.MaxIndexInterval != other.MaxIndexInterval ||
		t.MemtableFlushPeriodInMS != other.MemtableFlushPeriodInMS ||
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
	// BaseTable is the name of the table the view selects from.
	BaseTable string
	// Keyspace is the name of the view's keyspace.
	Keyspace string
	// Name is the view's name.
	Name string
	// OrderedColumns lists the columns in Table.OrderedColumns order.
	OrderedColumns []*Column
	// PartitionKey lists the partition key columns, in key order.
	PartitionKey []*Column
	// ClusteringColumns lists the clustering columns, in key order.
	ClusteringColumns []*Column
	// Options holds the view's options.
	Options TableOptions
	// ID is the view's UUID.
	ID cql.UUID
	// BaseTableID is the UUID of the table the view selects from.
	BaseTableID cql.UUID
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
	// ComponentIndex is the column's position in the partition key or among
	// the clustering columns; it is -1 for other columns.
	ComponentIndex int
	// Kind is the column's role in the table.
	Kind ColumnKind
	// ClusteringOrder is the order of a clustering column, and
	// ColumnOrderNone for any other column.
	ClusteringOrder ColumnOrder
}

// Equal reports whether c and other describe the same column. Two nil
// columns are equal, and nil never equals non-nil.
func (c *Column) Equal(other *Column) bool {
	if c == nil || other == nil {
		return c == other
	}

	return c.Keyspace == other.Keyspace &&
		c.Table == other.Table &&
		c.Name == other.Name &&
		c.ComponentIndex == other.ComponentIndex &&
		c.Kind == other.Kind &&
		c.Type == other.Type &&
		c.ClusteringOrder == other.ClusteringOrder
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
	// FinalFunc is the final function, unset when the aggregate has none or
	// the snapshot does not hold it.
	FinalFunc cql.Option[Function]
	// StateFunc is the state function, unset when the snapshot does not hold
	// it.
	StateFunc cql.Option[Function]
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
	// Keyspace is the name of the index's keyspace.
	Keyspace string
	// Table is the name of the indexed table.
	Table string
	// Kind is the kind of index.
	Kind IndexKind
	// Options holds the index's options, such as "target".
	Options map[string]string
	// Columns holds, on ScyllaDB, the columns of the view that backs the
	// index, by name.
	Columns map[string]*Column
	// OrderedColumns lists the backing view's columns in
	// Table.OrderedColumns order.
	OrderedColumns []*Column
	// PartitionKey lists the backing view's partition key columns, in key
	// order.
	PartitionKey []*Column
	// ClusteringColumns lists the backing view's clustering columns, in key
	// order.
	ClusteringColumns []*Column
}

// Equal reports whether t and other have the same ID, keyspace, name,
// columns, options, flags and extensions. Two nil tables are equal, and nil
// never equals non-nil.
func (t *Table) Equal(other *Table) bool {
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

	if len(t.OrderedColumns) != len(other.OrderedColumns) || !compareColumnSlices(t.OrderedColumns, other.OrderedColumns) {
		return false
	}

	if !t.Options.Equal(&other.Options) {
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
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}

func compareColumnsMap(a, b map[string]*Column) bool {
	for k, v := range a {
		otherValue, exists := b[k]
		if !exists || !v.Equal(otherValue) {
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

// ColumnOrder is the order of a clustering column, as the CLUSTERING ORDER BY
// clause states it. The zero ColumnOrder is ColumnOrderNone, the order of a
// column that is not a clustering column. Later releases may add values, so a
// switch over them needs a default case.
type ColumnOrder uint8

// The orders of columns.
const (
	ColumnOrderNone ColumnOrder = iota
	ColumnOrderAsc
	ColumnOrderDesc
)

// String returns "none", "asc" or "desc", the server's spelling, or
// ColumnOrder(n) for a value it does not define.
func (o ColumnOrder) String() string {
	switch o {
	case ColumnOrderNone:
		return "none"
	case ColumnOrderAsc:
		return "asc"
	case ColumnOrderDesc:
		return "desc"
	default:
		return fmt.Sprintf("ColumnOrder(%d)", uint8(o))
	}
}

// ColumnKind is the role of a column in its table. The zero ColumnKind is
// ColumnKindUnknown. Later releases may add values, so a switch over them needs
// a default case.
type ColumnKind uint8

// The kinds of columns.
const (
	ColumnKindUnknown ColumnKind = iota
	ColumnKindPartitionKey
	ColumnKindClusteringKey
	ColumnKindRegular
	ColumnKindCompact
	ColumnKindStatic
)

// String returns the kind name, or ColumnKind(n) for a value it does not
// define.
func (c ColumnKind) String() string {
	switch c {
	case ColumnKindUnknown:
		return "unknown"
	case ColumnKindPartitionKey:
		return "partition_key"
	case ColumnKindClusteringKey:
		return "clustering_key"
	case ColumnKindRegular:
		return "regular"
	case ColumnKindCompact:
		return "compact"
	case ColumnKindStatic:
		return "static"
	default:
		return fmt.Sprintf("ColumnKind(%d)", uint8(c))
	}
}

type keyspaceState struct {
	InternalTablesInvalidated map[string]struct{}
	ServerCQL                 string
	TabletsOption             bool
}
