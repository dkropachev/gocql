// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/metadata"
)

// cloneV2Extensions deep-copies schema extensions.
func cloneV2Extensions(m map[string][]byte) map[string][]byte {
	if m == nil {
		return nil
	}
	r := make(map[string][]byte, len(m))
	for k, v := range m {
		r[k] = bytes.Clone(v)
	}
	return r
}
func cloneV2Column(c *metadata.Column) *metadata.Column {
	if c == nil {
		return nil
	}
	r := *c
	return &r
}
func cloneV2Columns(c map[string]*metadata.Column) map[string]*metadata.Column {
	if c == nil {
		return nil
	}
	r := make(map[string]*metadata.Column, len(c))
	for k, v := range c {
		r[k] = cloneV2Column(v)
	}
	return r
}
func cloneV2ColumnList(c []*metadata.Column) []*metadata.Column {
	if c == nil {
		return nil
	}
	r := make([]*metadata.Column, len(c))
	for i, v := range c {
		r[i] = cloneV2Column(v)
	}
	return r
}
func cloneV2TableOptions(o metadata.TableOptions) metadata.TableOptions {
	o.Caching = maps.Clone(o.Caching)
	o.Compaction = maps.Clone(o.Compaction)
	o.Compression = maps.Clone(o.Compression)
	o.CDC = maps.Clone(o.CDC)
	return o
}
func cloneV2Function(f metadata.Function) metadata.Function {
	f.ArgumentTypes = append([]string(nil), f.ArgumentTypes...)
	f.ArgumentNames = append([]string(nil), f.ArgumentNames...)
	return f
}

// InternalCloneV2Table returns a deep copy of t.
func InternalCloneV2Table(t *metadata.Table) *metadata.Table {
	if t == nil {
		return nil
	}
	v := *t
	v.Columns = cloneV2Columns(t.Columns)
	v.Extensions = cloneV2Extensions(t.Extensions)
	v.PartitionKey = cloneV2ColumnList(t.PartitionKey)
	v.ClusteringColumns = cloneV2ColumnList(t.ClusteringColumns)
	v.OrderedColumns = append([]string(nil), t.OrderedColumns...)
	v.Flags = append([]string(nil), t.Flags...)
	v.Options = cloneV2TableOptions(t.Options)
	return &v
}

// shallowCloneKeyspace copies k and its maps, but not the values in them, so
// that adding or removing entries does not race with readers of k.
func shallowCloneKeyspace(k *metadata.Keyspace) *metadata.Keyspace {
	r := *k
	r.StrategyOptions = maps.Clone(k.StrategyOptions)
	r.Tables = maps.Clone(k.Tables)
	r.Functions = maps.Clone(k.Functions)
	r.Aggregates = maps.Clone(k.Aggregates)
	r.Types = maps.Clone(k.Types)
	r.Indexes = maps.Clone(k.Indexes)
	r.Views = maps.Clone(k.Views)
	KeyspaceData(&r).InternalTablesInvalidated = maps.Clone(KeyspaceData(k).InternalTablesInvalidated)
	return &r
}

func InternalCloneV2Keyspace(k *metadata.Keyspace) *metadata.Keyspace {
	if k == nil {
		return nil
	}
	r := *k
	r.StrategyOptions = maps.Clone(k.StrategyOptions)
	KeyspaceData(&r).InternalTablesInvalidated = maps.Clone(KeyspaceData(k).InternalTablesInvalidated)
	r.Tables = make(map[string]*metadata.Table, len(k.Tables))
	for n, t := range k.Tables {
		if t == nil {
			r.Tables[n] = nil
			continue
		}
		r.Tables[n] = InternalCloneV2Table(t)
	}
	r.Functions = make(map[string]*metadata.Function, len(k.Functions))
	for n, f := range k.Functions {
		if f == nil {
			r.Functions[n] = nil
			continue
		}
		v := cloneV2Function(*f)
		r.Functions[n] = &v
	}
	r.Aggregates = make(map[string]*metadata.Aggregate, len(k.Aggregates))
	for n, a := range k.Aggregates {
		if a == nil {
			r.Aggregates[n] = nil
			continue
		}
		v := *a
		v.ArgumentTypes = append([]string(nil), a.ArgumentTypes...)
		v.FinalFunc = cloneV2Function(a.FinalFunc)
		v.StateFunc = cloneV2Function(a.StateFunc)
		r.Aggregates[n] = &v
	}
	r.Types = make(map[string]*metadata.Type, len(k.Types))
	for n, t := range k.Types {
		if t == nil {
			r.Types[n] = nil
			continue
		}
		v := *t
		v.FieldNames = append([]string(nil), t.FieldNames...)
		v.FieldTypes = append([]string(nil), t.FieldTypes...)
		r.Types[n] = &v
	}
	r.Views = make(map[string]*metadata.View, len(k.Views))
	for n, t := range k.Views {
		if t == nil {
			r.Views[n] = nil
			continue
		}
		v := *t
		v.Columns = cloneV2Columns(t.Columns)
		v.Extensions = cloneV2Extensions(t.Extensions)
		v.PartitionKey = cloneV2ColumnList(t.PartitionKey)
		v.ClusteringColumns = cloneV2ColumnList(t.ClusteringColumns)
		v.OrderedColumns = append([]string(nil), t.OrderedColumns...)
		v.Options = cloneV2TableOptions(t.Options)
		r.Views[n] = &v
	}
	r.Indexes = make(map[string]*metadata.Index, len(k.Indexes))
	for n, t := range k.Indexes {
		if t == nil {
			r.Indexes[n] = nil
			continue
		}
		v := *t
		v.Options = maps.Clone(t.Options)
		v.Columns = cloneV2Columns(t.Columns)
		v.PartitionKey = cloneV2ColumnList(t.PartitionKey)
		v.ClusteringColumns = cloneV2ColumnList(t.ClusteringColumns)
		v.OrderedColumns = append([]string(nil), t.OrderedColumns...)
		r.Indexes[n] = &v
	}
	return &r
}

// Schema rows intentionally materialize small metadata values on the cold path.
// Fields are explicitly registered; no reflection or destination inference runs.
type InternalV2SchemaField[R any] struct {
	compile  func(cql.DataType) (func(*R, *cql.RowReader) error, error)
	name     string
	required bool
}

func InternalV2SchemaColumn[R, V any](name string, decoder cql.Decoder[V], field func(*R) *V) InternalV2SchemaField[R] {
	return InternalV2SchemaField[R]{name: name, compile: func(typ cql.DataType) (func(*R, *cql.RowReader) error, error) {
		nullable := cql.NullableDecoder(decoder)
		if err := nullable.CheckType(typ); err != nil {
			return nil, err
		}
		return func(row *R, reader *cql.RowReader) error {
			v, err := reader.ReadColumn(nullable)
			if err != nil {
				return err
			}
			if v.Valid {
				*field(row) = v.Value
			}
			return nil
		}, nil
	}}
}
func InternalV2RequiredSchemaColumn[R, V any](name string, decoder cql.Decoder[V], field func(*R) *V) InternalV2SchemaField[R] {
	return InternalV2SchemaField[R]{name: name, required: true, compile: func(typ cql.DataType) (func(*R, *cql.RowReader) error, error) {
		if err := decoder.CheckType(typ); err != nil {
			return nil, err
		}
		return func(row *R, reader *cql.RowReader) error {
			value, err := reader.ReadColumn(decoder)
			if err != nil {
				return err
			}
			*field(row) = value
			return nil
		}, nil
	}}
}

type InternalV2SchemaRows[R any] struct {
	initialize     func() *R
	InternalFields []InternalV2SchemaField[R]
}
type v2SchemaRowPlan[R any] struct {
	codec.ManagedRowDestination
	initialize func() *R
	steps      []func(*R, *cql.RowReader) error
}

func (d InternalV2SchemaRows[R]) Compile(columns cql.Columns) (cql.CompiledRow[R], error) {
	fields := make(map[string]InternalV2SchemaField[R], len(d.InternalFields))
	for _, f := range d.InternalFields {
		if _, exists := fields[f.name]; exists {
			return nil, cqltype.TypeValidation("schema row", "duplicate application field %q", f.name)
		}
		fields[f.name] = f
	}
	steps := make([]func(*R, *cql.RowReader) error, columns.Len())
	seen := make(map[string]bool, columns.Len())
	for i := range steps {
		c, _ := columns.At(i)
		if seen[c.Name] {
			return nil, cqltype.TypeValidation("schema row", "duplicate column %q", c.Name)
		}
		seen[c.Name] = true
		if f, ok := fields[c.Name]; ok {
			step, err := f.compile(c.Type)
			if err != nil {
				return nil, fmt.Errorf("metadata column %s: %w", c.Name, err)
			}
			steps[i] = step
		} else {
			steps[i] = func(_ *R, r *cql.RowReader) error { return r.SkipColumn() }
		}
	}
	for _, f := range d.InternalFields {
		if f.required && !seen[f.name] {
			return nil, cqltype.TypeValidation("schema row", "missing column %q", f.name)
		}
	}
	return v2SchemaRowPlan[R]{steps: steps, initialize: d.initialize}, nil
}
func (d InternalV2SchemaRows[R]) Clone(v R) R { return v }
func (p v2SchemaRowPlan[R]) Decode(reader *cql.RowReader) (R, error) {
	var result R
	err := p.DecodeInto(reader, &result)
	if err != nil {
		return *new(R), err
	}
	return result, nil
}
func (p v2SchemaRowPlan[R]) DecodeInto(reader *cql.RowReader, dst *R) error {
	if dst == nil {
		return cqltype.TypeValidation("destination", "nil schema row destination")
	}
	var zero R
	*dst = zero
	if p.initialize != nil {
		*dst = *p.initialize()
	}
	for _, step := range p.steps {
		if err := step(dst, reader); err != nil {
			return err
		}
	}
	return nil
}
func (p v2SchemaRowPlan[R]) Clone(v R) R { return v }

type InternalV2MetadataText struct{}

type InternalV2MetadataUUIDString struct{}

type InternalV2MetadataInt struct{}

type v2MetadataKind struct{}

type v2MetadataClusteringOrder struct{}

type v2MetadataStrings struct{}

type v2MetadataStringMap struct{}

type v2MetadataExtensions struct{}

type v2SchemaParams struct{ count int }

func (v2SchemaParams) Compile(columns cql.Columns) (cql.CompiledParams[[]string], error) {
	for i := 0; i < columns.Len(); i++ {
		c, _ := columns.At(i)
		if err := cql.StringCodec.CheckType(c.Type); err != nil {
			return nil, err
		}
	}
	return v2SchemaParams{count: columns.Len()}, nil
}
func (p v2SchemaParams) Encode(w *cql.ParamWriter, values []string) error {
	if len(values) != p.count {
		return cqltype.TypeValidation("schema parameters", "wrong count")
	}
	for _, value := range values {
		if err := w.WriteParam(cql.StringCodec, value); err != nil {
			return err
		}
	}
	return nil
}

func InternalQueryV2SchemaRows[R any](ctx context.Context, s Source, stmt string, args []string, decoder cql.RowDecoder[R]) ([]R, error) {
	settings := s.InternalSchemaSettings()
	if !settings.SystemSchema {
		return nil, &cqlerr.UnsupportedSettingError{Setting: "WithMetadataMode", Reason: "the server does not provide the system_schema tables that schema metadata is read from"}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !settings.Available {
		return nil, InternalErrNoControl
	}
	typ := cql.TextType
	columns := make([]cql.ColumnSpec, len(args))
	for i := range columns {
		columns[i] = cql.ColumnSpec{Type: typ}
	}
	rows, err := ReadRows(ctx, 4, settings.Timeout, s.InternalQuerySystem, Query[[]string, R]{CQL: stmt, Params: v2SchemaParams{}, Rows: decoder}, cql.NewColumns(columns...), args)
	return rows, wrapV2SchemaQueryError(stmt, err)
}

func InternalV2SchemaContext(s Source) context.Context {
	if s != nil {
		return s.InternalSchemaContext()
	}
	return context.Background()
}

func v2DecodeTableMetadata(keyspace string) cql.RowDecoder[metadata.Table] {
	return InternalV2SchemaRows[metadata.Table]{initialize: func() *metadata.Table { return &metadata.Table{Keyspace: keyspace} }, InternalFields: []InternalV2SchemaField[metadata.Table]{
		InternalV2RequiredSchemaColumn("id", cql.UUIDCodec, func(r *metadata.Table) *cql.UUID { return &r.ID }),
		InternalV2RequiredSchemaColumn("table_name", InternalV2MetadataText{}, func(r *metadata.Table) *string { return &r.Name }),
		InternalV2SchemaColumn("flags", v2MetadataStrings{}, func(r *metadata.Table) *[]string { return &r.Flags }),
		InternalV2SchemaColumn("extensions", v2MetadataExtensions{}, func(r *metadata.Table) *map[string][]byte { return &r.Extensions }),
		InternalV2SchemaColumn("bloom_filter_fp_chance", cql.Float64Codec, func(r *metadata.Table) *float64 { return &r.Options.BloomFilterFPChance }),
		InternalV2SchemaColumn("caching", v2MetadataStringMap{}, func(r *metadata.Table) *map[string]string { return &r.Options.Caching }),
		InternalV2SchemaColumn("comment", InternalV2MetadataText{}, func(r *metadata.Table) *string { return &r.Options.Comment }),
		InternalV2SchemaColumn("compaction", v2MetadataStringMap{}, func(r *metadata.Table) *map[string]string { return &r.Options.Compaction }),
		InternalV2SchemaColumn("compression", v2MetadataStringMap{}, func(r *metadata.Table) *map[string]string { return &r.Options.Compression }),
		InternalV2SchemaColumn("crc_check_chance", cql.Float64Codec, func(r *metadata.Table) *float64 { return &r.Options.CRCCheckChance }),
		InternalV2SchemaColumn("default_time_to_live", InternalV2MetadataInt{}, func(r *metadata.Table) *int { return &r.Options.DefaultTimeToLive }),
		InternalV2SchemaColumn("gc_grace_seconds", InternalV2MetadataInt{}, func(r *metadata.Table) *int { return &r.Options.GCGraceSeconds }),
		InternalV2SchemaColumn("max_index_interval", InternalV2MetadataInt{}, func(r *metadata.Table) *int { return &r.Options.MaxIndexInterval }),
		InternalV2SchemaColumn("memtable_flush_period_in_ms", InternalV2MetadataInt{}, func(r *metadata.Table) *int { return &r.Options.MemtableFlushPeriodInMS }),
		InternalV2SchemaColumn("min_index_interval", InternalV2MetadataInt{}, func(r *metadata.Table) *int { return &r.Options.MinIndexInterval }),
		InternalV2SchemaColumn("speculative_retry", InternalV2MetadataText{}, func(r *metadata.Table) *string { return &r.Options.SpeculativeRetry }),
	}}
}

func v2DecodeColumnMetadata(keyspace string) cql.RowDecoder[metadata.Column] {
	return InternalV2SchemaRows[metadata.Column]{initialize: func() *metadata.Column { return &metadata.Column{Keyspace: keyspace} }, InternalFields: []InternalV2SchemaField[metadata.Column]{
		InternalV2RequiredSchemaColumn("table_name", InternalV2MetadataText{}, func(r *metadata.Column) *string { return &r.Table }),
		InternalV2RequiredSchemaColumn("column_name", InternalV2MetadataText{}, func(r *metadata.Column) *string { return &r.Name }),
		InternalV2SchemaColumn("clustering_order", v2MetadataClusteringOrder{}, func(r *metadata.Column) *metadata.ColumnOrder { return &r.ClusteringOrder }),
		InternalV2RequiredSchemaColumn("type", InternalV2MetadataText{}, func(r *metadata.Column) *string { return &r.Type }),
		InternalV2RequiredSchemaColumn("kind", v2MetadataKind{}, func(r *metadata.Column) *metadata.ColumnKind { return &r.Kind }),
		InternalV2RequiredSchemaColumn("position", InternalV2MetadataInt{}, func(r *metadata.Column) *int { return &r.ComponentIndex }),
	}}
}

func InternalV2DecodeTypeMetadata(keyspace string) cql.RowDecoder[metadata.Type] {
	return InternalV2SchemaRows[metadata.Type]{initialize: func() *metadata.Type { return &metadata.Type{Keyspace: keyspace} }, InternalFields: []InternalV2SchemaField[metadata.Type]{
		InternalV2RequiredSchemaColumn("type_name", InternalV2MetadataText{}, func(r *metadata.Type) *string { return &r.Name }),
		InternalV2RequiredSchemaColumn("field_names", v2MetadataStrings{}, func(r *metadata.Type) *[]string { return &r.FieldNames }),
		InternalV2RequiredSchemaColumn("field_types", v2MetadataStrings{}, func(r *metadata.Type) *[]string { return &r.FieldTypes }),
	}}
}

func InternalV2DecodeFunctionMetadata(keyspace string) cql.RowDecoder[metadata.Function] {
	return InternalV2SchemaRows[metadata.Function]{initialize: func() *metadata.Function { return &metadata.Function{Keyspace: keyspace} }, InternalFields: []InternalV2SchemaField[metadata.Function]{
		InternalV2RequiredSchemaColumn("function_name", InternalV2MetadataText{}, func(r *metadata.Function) *string { return &r.Name }),
		InternalV2RequiredSchemaColumn("argument_types", v2MetadataStrings{}, func(r *metadata.Function) *[]string { return &r.ArgumentTypes }),
		InternalV2RequiredSchemaColumn("argument_names", v2MetadataStrings{}, func(r *metadata.Function) *[]string { return &r.ArgumentNames }),
		InternalV2RequiredSchemaColumn("body", InternalV2MetadataText{}, func(r *metadata.Function) *string { return &r.Body }),
		InternalV2RequiredSchemaColumn("called_on_null_input", cql.BoolCodec, func(r *metadata.Function) *bool { return &r.CalledOnNullInput }),
		InternalV2RequiredSchemaColumn("language", InternalV2MetadataText{}, func(r *metadata.Function) *string { return &r.Language }),
		InternalV2RequiredSchemaColumn("return_type", InternalV2MetadataText{}, func(r *metadata.Function) *string { return &r.ReturnType }),
	}}
}

func InternalV2DecodeAggregateMetadata(keyspace string) cql.RowDecoder[metadata.Aggregate] {
	return InternalV2SchemaRows[metadata.Aggregate]{initialize: func() *metadata.Aggregate { return &metadata.Aggregate{Keyspace: keyspace} }, InternalFields: []InternalV2SchemaField[metadata.Aggregate]{
		InternalV2RequiredSchemaColumn("aggregate_name", InternalV2MetadataText{}, func(r *metadata.Aggregate) *string { return &r.Name }),
		InternalV2RequiredSchemaColumn("argument_types", v2MetadataStrings{}, func(r *metadata.Aggregate) *[]string { return &r.ArgumentTypes }),
		InternalV2SchemaColumn("final_func", InternalV2MetadataText{}, func(r *metadata.Aggregate) *string { return &AggregateMetadataData(r).InternalFinalFunc }),
		InternalV2SchemaColumn("initcond", InternalV2MetadataText{}, func(r *metadata.Aggregate) *string { return &r.InitCond }),
		InternalV2RequiredSchemaColumn("return_type", InternalV2MetadataText{}, func(r *metadata.Aggregate) *string { return &r.ReturnType }),
		InternalV2RequiredSchemaColumn("state_func", InternalV2MetadataText{}, func(r *metadata.Aggregate) *string { return &AggregateMetadataData(r).InternalStateFunc }),
		InternalV2RequiredSchemaColumn("state_type", InternalV2MetadataText{}, func(r *metadata.Aggregate) *string { return &r.StateType }),
	}}
}

func v2DecodeIndexMetadata(keyspace string) cql.RowDecoder[metadata.Index] {
	return InternalV2SchemaRows[metadata.Index]{initialize: func() *metadata.Index { return &metadata.Index{Keyspace: keyspace} }, InternalFields: []InternalV2SchemaField[metadata.Index]{
		InternalV2RequiredSchemaColumn("index_name", InternalV2MetadataText{}, func(r *metadata.Index) *string { return &r.Name }),
		InternalV2RequiredSchemaColumn("keyspace_name", InternalV2MetadataText{}, func(r *metadata.Index) *string { return &r.Keyspace }),
		InternalV2RequiredSchemaColumn("table_name", InternalV2MetadataText{}, func(r *metadata.Index) *string { return &r.Table }),
		InternalV2RequiredSchemaColumn("kind", InternalV2MetadataText{}, func(r *metadata.Index) *string { return (*string)(&r.Kind) }),
		InternalV2RequiredSchemaColumn("options", v2MetadataStringMap{}, func(r *metadata.Index) *map[string]string { return &r.Options }),
	}}
}

func v2DecodeViewMetadata(keyspace string) cql.RowDecoder[metadata.View] {
	return InternalV2SchemaRows[metadata.View]{initialize: func() *metadata.View { return &metadata.View{Keyspace: keyspace} }, InternalFields: []InternalV2SchemaField[metadata.View]{
		InternalV2RequiredSchemaColumn("id", cql.UUIDCodec, func(r *metadata.View) *cql.UUID { return &r.ID }),
		InternalV2RequiredSchemaColumn("view_name", InternalV2MetadataText{}, func(r *metadata.View) *string { return &r.Name }),
		InternalV2RequiredSchemaColumn("base_table_id", cql.UUIDCodec, func(r *metadata.View) *cql.UUID { return &r.BaseTableID }),
		InternalV2RequiredSchemaColumn("base_table_name", InternalV2MetadataText{}, func(r *metadata.View) *string { return &r.BaseTable }),
		InternalV2RequiredSchemaColumn("include_all_columns", cql.BoolCodec, func(r *metadata.View) *bool { return &r.IncludeAllColumns }),
		InternalV2RequiredSchemaColumn("where_clause", InternalV2MetadataText{}, func(r *metadata.View) *string { return &r.WhereClause }),
		InternalV2SchemaColumn("extensions", v2MetadataExtensions{}, func(r *metadata.View) *map[string][]byte { return &r.Extensions }),
		InternalV2SchemaColumn("dclocal_read_repair_chance", cql.Float64Codec, func(r *metadata.View) *float64 { return &r.Options.DCLocalReadRepairChance }),
		InternalV2SchemaColumn("read_repair_chance", cql.Float64Codec, func(r *metadata.View) *float64 { return &r.Options.ReadRepairChance }),
		InternalV2SchemaColumn("bloom_filter_fp_chance", cql.Float64Codec, func(r *metadata.View) *float64 { return &r.Options.BloomFilterFPChance }),
		InternalV2SchemaColumn("caching", v2MetadataStringMap{}, func(r *metadata.View) *map[string]string { return &r.Options.Caching }),
		InternalV2SchemaColumn("comment", InternalV2MetadataText{}, func(r *metadata.View) *string { return &r.Options.Comment }),
		InternalV2SchemaColumn("compaction", v2MetadataStringMap{}, func(r *metadata.View) *map[string]string { return &r.Options.Compaction }),
		InternalV2SchemaColumn("compression", v2MetadataStringMap{}, func(r *metadata.View) *map[string]string { return &r.Options.Compression }),
		InternalV2SchemaColumn("crc_check_chance", cql.Float64Codec, func(r *metadata.View) *float64 { return &r.Options.CRCCheckChance }),
		InternalV2SchemaColumn("default_time_to_live", InternalV2MetadataInt{}, func(r *metadata.View) *int { return &r.Options.DefaultTimeToLive }),
		InternalV2SchemaColumn("gc_grace_seconds", InternalV2MetadataInt{}, func(r *metadata.View) *int { return &r.Options.GCGraceSeconds }),
		InternalV2SchemaColumn("max_index_interval", InternalV2MetadataInt{}, func(r *metadata.View) *int { return &r.Options.MaxIndexInterval }),
		InternalV2SchemaColumn("memtable_flush_period_in_ms", InternalV2MetadataInt{}, func(r *metadata.View) *int { return &r.Options.MemtableFlushPeriodInMS }),
		InternalV2SchemaColumn("min_index_interval", InternalV2MetadataInt{}, func(r *metadata.View) *int { return &r.Options.MinIndexInterval }),
		InternalV2SchemaColumn("speculative_retry", InternalV2MetadataText{}, func(r *metadata.View) *string { return &r.Options.SpeculativeRetry }),
	}}
}

type v2ScyllaTableRow struct {
	Name    string
	Options metadata.TableOptions
}

func v2DecodeScyllaTable() cql.RowDecoder[v2ScyllaTableRow] {
	return InternalV2SchemaRows[v2ScyllaTableRow]{InternalFields: []InternalV2SchemaField[v2ScyllaTableRow]{InternalV2RequiredSchemaColumn("table_name", InternalV2MetadataText{}, func(r *v2ScyllaTableRow) *string { return &r.Name }),
		InternalV2SchemaColumn("cdc", v2MetadataStringMap{}, func(r *v2ScyllaTableRow) *map[string]string { return &r.Options.CDC }),
		InternalV2SchemaColumn("in_memory", cql.BoolCodec, func(r *v2ScyllaTableRow) *bool { return &r.Options.InMemory }),
		InternalV2SchemaColumn("partitioner", InternalV2MetadataText{}, func(r *v2ScyllaTableRow) *string { return &r.Options.Partitioner }),
		InternalV2SchemaColumn("version", cql.UUIDCodec, func(r *v2ScyllaTableRow) *cql.UUID { return &r.Options.Version }),
	}}
}

type v2KeyspaceRow struct {
	replication  map[string]string
	InternalName string
	durable      bool
}

func InternalV2DecodeKeyspace() cql.RowDecoder[v2KeyspaceRow] {
	return InternalV2SchemaRows[v2KeyspaceRow]{InternalFields: []InternalV2SchemaField[v2KeyspaceRow]{InternalV2SchemaColumn("keyspace_name", InternalV2MetadataText{}, func(r *v2KeyspaceRow) *string { return &r.InternalName }), InternalV2RequiredSchemaColumn("durable_writes", cql.BoolCodec, func(r *v2KeyspaceRow) *bool { return &r.durable }), InternalV2RequiredSchemaColumn("replication", v2MetadataStringMap{}, func(r *v2KeyspaceRow) *map[string]string { return &r.replication })}}
}
func InternalV2KeyspaceFromRow(name string, row v2KeyspaceRow) *metadata.Keyspace {
	ks := &metadata.Keyspace{Name: name, DurableWrites: row.durable, StrategyClass: row.replication["class"], StrategyOptions: make(map[string]string, len(row.replication))}
	for k, v := range row.replication {
		if k != "class" {
			ks.StrategyOptions[k] = v
		}
	}
	return ks
}
func InternalGetV2KeyspaceMetadata(ctx context.Context, s Source, name string) (*metadata.Keyspace, error) {
	rows, err := InternalQueryV2SchemaRows(ctx, s, `SELECT durable_writes, replication FROM system_schema.keyspaces WHERE keyspace_name = ?`, []string{name}, InternalV2DecodeKeyspace())
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, InternalErrKeyspaceDoesNotExist
	}
	if len(rows) != 1 {
		return nil, cqltype.TypeValidation("keyspace schema", "duplicate identity %q", name)
	}
	ks := InternalV2KeyspaceFromRow(name, rows[0])
	if err := InternalReadV2ScyllaKeyspaces(ctx, s, map[string]*metadata.Keyspace{name: ks}, name); err != nil {
		return nil, err
	}
	return ks, nil
}
func InternalGetV2TableMetadata(ctx context.Context, s Source, keyspace, table string) ([]metadata.Table, error) {
	where := ` WHERE keyspace_name = ?`
	args := []string{keyspace}
	if table != "" {
		where += ` AND table_name = ?`
		args = append(args, table)
	}
	rows, err := InternalQueryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.tables`+where, args, v2DecodeTableMetadata(keyspace))
	if err != nil {
		return nil, err
	}
	if !s.InternalSchemaSettings().Scylla {
		return rows, nil
	}
	extra, err := InternalQueryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.scylla_tables`+where, args, v2DecodeScyllaTable())
	if err != nil {
		return nil, err
	}
	options := make(map[string]metadata.TableOptions, len(extra))
	for _, row := range extra {
		if _, ok := options[row.Name]; ok {
			return nil, cqltype.TypeValidation("scylla table schema", "duplicate identity %q", row.Name)
		}
		options[row.Name] = row.Options
	}
	for i := range rows {
		if extra, ok := options[rows[i].Name]; ok {
			rows[i].Options.CDC = extra.CDC
			rows[i].Options.InMemory = extra.InMemory
			rows[i].Options.Partitioner = extra.Partitioner
			rows[i].Options.Version = extra.Version
		}
	}
	return rows, nil
}
func InternalGetV2ColumnMetadata(ctx context.Context, s Source, keyspace, table string) ([]metadata.Column, error) {
	where := ` WHERE keyspace_name = ?`
	args := []string{keyspace}
	if table != "" {
		where += ` AND table_name = ?`
		args = append(args, table)
	}
	return InternalQueryV2SchemaRows(ctx, s, `SELECT `+InternalColumnMetadataColumns+` FROM system_schema.columns`+where, args, v2DecodeColumnMetadata(keyspace))
}
func InternalGetV2IndexMetadata(ctx context.Context, s Source, keyspace, table string) ([]metadata.Index, error) {
	where := ` WHERE keyspace_name = ?`
	args := []string{keyspace}
	if table != "" {
		where += ` AND table_name = ?`
		args = append(args, table)
	}
	return InternalQueryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.indexes`+where, args, v2DecodeIndexMetadata(keyspace))
}
func InternalGetV2ViewMetadata(ctx context.Context, s Source, keyspace, table string) ([]metadata.View, error) {
	where := ` WHERE keyspace_name = ?`
	args := []string{keyspace}
	if table != "" {
		where += ` AND base_table_name = ? ALLOW FILTERING`
		args = append(args, table)
	}
	return InternalQueryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.views`+where, args, v2DecodeViewMetadata(keyspace))
}
func InternalGetV2CreateStatements(ctx context.Context, s Source, keyspace string) ([]byte, error) {
	decoder := InternalV2SchemaRows[string]{InternalFields: []InternalV2SchemaField[string]{InternalV2RequiredSchemaColumn("create_statement", InternalV2MetadataText{}, func(s *string) *string { return s })}}
	stmt := `DESCRIBE KEYSPACE "` + strings.ReplaceAll(keyspace, `"`, `""`) + `" WITH INTERNALS`
	rows, err := InternalQueryV2SchemaRows(ctx, s, stmt, nil, decoder)
	if err != nil {
		if server, ok := errors.AsType[cqlerr.ServerError](err); ok && server.Code() == cqlerr.CodeSyntax {
			return nil, nil
		}
		return nil, err
	}
	return []byte(strings.Join(rows, "\n")), nil
}

func InternalFetchV2Keyspace(ctx context.Context, s Source, base *metadata.Keyspace) (*metadata.Keyspace, error) {
	var tables []metadata.Table
	var columns []metadata.Column
	var types []metadata.Type
	var functions []metadata.Function
	var aggregates []metadata.Aggregate
	var indexes []metadata.Index
	var views []metadata.View
	var create []byte
	group, ctx := errgroup.WithContext(ctx)
	name := base.Name
	group.Go(func() error { var err error; tables, err = InternalGetV2TableMetadata(ctx, s, name, ""); return err })
	group.Go(func() error { var err error; columns, err = InternalGetV2ColumnMetadata(ctx, s, name, ""); return err })
	group.Go(func() error {
		var err error
		types, err = InternalQueryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.types WHERE keyspace_name = ?`, []string{name}, InternalV2DecodeTypeMetadata(name))
		return err
	})
	if s.InternalSchemaSettings().Aggregates {
		group.Go(func() error {
			var err error
			functions, err = InternalQueryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.functions WHERE keyspace_name = ?`, []string{name}, InternalV2DecodeFunctionMetadata(name))
			return err
		})
		group.Go(func() error {
			var err error
			aggregates, err = InternalQueryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.aggregates WHERE keyspace_name = ?`, []string{name}, InternalV2DecodeAggregateMetadata(name))
			return err
		})
	}
	group.Go(func() error { var err error; indexes, err = InternalGetV2IndexMetadata(ctx, s, name, ""); return err })
	group.Go(func() error { var err error; views, err = InternalGetV2ViewMetadata(ctx, s, name, ""); return err })
	group.Go(func() error { var err error; create, err = InternalGetV2CreateStatements(ctx, s, name); return err })
	if err := group.Wait(); err != nil {
		return nil, err
	}
	if err := InternalValidateV2SchemaParts(base, tables, columns, types, functions, aggregates, indexes, views, s.InternalSchemaSettings().Depth); err != nil {
		return nil, err
	}
	compileMetadata(base, tables, columns, functions, aggregates, types, indexes, views, create)
	return base, nil
}

func InternalValidateV2SchemaParts(ks *metadata.Keyspace, tables []metadata.Table, columns []metadata.Column, types []metadata.Type, functions []metadata.Function, aggregates []metadata.Aggregate, indexes []metadata.Index, views []metadata.View, depthLimit ...int) error {
	maxDepth := 64
	if len(depthLimit) > 0 && depthLimit[0] > 0 {
		maxDepth = depthLimit[0]
	}
	if ks == nil || ks.Name == "" {
		return cqltype.TypeValidation("schema", "missing keyspace identity")
	}
	unique := func(kind string, names []string) error {
		seen := make(map[string]bool, len(names))
		for _, name := range names {
			if name == "" || seen[name] {
				return cqltype.TypeValidation("schema", "empty or duplicate %s identity %q", kind, name)
			}
			seen[name] = true
		}
		return nil
	}
	names := make([]string, 0, len(tables))
	knownTables := make(map[string]bool)
	for _, v := range tables {
		names = append(names, v.Name)
		knownTables[v.Name] = true
	}
	if err := unique("table", names); err != nil {
		return err
	}
	names = nil
	for _, v := range views {
		names = append(names, v.Name)
		if knownTables[v.Name] {
			return cqltype.TypeValidation("schema", "table/view identity collision %q", v.Name)
		}
		knownTables[v.Name] = true
	}
	if err := unique("view", names); err != nil {
		return err
	}
	names = nil
	for _, v := range indexes {
		names = append(names, v.Name)
		knownTables[v.Name+"_index"] = true
	}
	if err := unique("index", names); err != nil {
		return err
	}
	udts := make(map[string]bool, len(types))
	names = nil
	for _, v := range types {
		names = append(names, v.Name)
		udts[v.Name] = true
	}
	if err := unique("UDT", names); err != nil {
		return err
	}
	dependencies := make(map[string][]string, len(types))
	for _, v := range types {
		if len(v.FieldNames) != len(v.FieldTypes) {
			return cqltype.TypeValidation("schema UDT", "field count mismatch for %q", v.Name)
		}
		if err := unique("UDT field", v.FieldNames); err != nil {
			return err
		}
		for _, typ := range v.FieldTypes {
			refs, err := validateV2CQLTypeLimit(ks.Name, typ, udts, maxDepth)
			if err != nil {
				return err
			}
			dependencies[v.Name] = append(dependencies[v.Name], refs...)
		}
	}
	visited := make(map[string]uint8)
	var visit func(string) error
	visit = func(name string) error {
		if visited[name] == 1 {
			return cqltype.TypeValidation("schema UDT", "cycle through %q", name)
		}
		if visited[name] == 2 {
			return nil
		}
		visited[name] = 1
		for _, dep := range dependencies[name] {
			if err := visit(dep); err != nil {
				return err
			}
		}
		visited[name] = 2
		return nil
	}
	for name := range udts {
		if err := visit(name); err != nil {
			return err
		}
	}
	identities := make(map[model.TableKey]bool, len(columns))
	positions := make(map[string]map[metadata.ColumnKind]map[int]bool)
	for _, v := range columns {
		if !knownTables[v.Table] {
			return cqltype.TypeValidation("schema column", "unknown table %q", v.Table)
		}
		identity := model.TableKey{InternalKeyspace: v.Table, InternalTable: v.Name}
		if identities[identity] || v.Name == "" {
			return cqltype.TypeValidation("schema column", "duplicate or empty identity %q.%q", v.Table, v.Name)
		}
		identities[identity] = true
		if _, err := validateV2CQLTypeLimit(ks.Name, v.Type, udts, maxDepth); err != nil {
			return err
		}
		if v.Kind == metadata.ColumnKindPartitionKey || v.Kind == metadata.ColumnKindClusteringKey {
			if v.ComponentIndex < 0 || v.ComponentIndex >= len(columns) {
				return cqltype.TypeValidation("schema key order", "invalid position %d", v.ComponentIndex)
			}
			if positions[v.Table] == nil {
				positions[v.Table] = make(map[metadata.ColumnKind]map[int]bool)
			}
			if positions[v.Table][v.Kind] == nil {
				positions[v.Table][v.Kind] = make(map[int]bool)
			}
			if positions[v.Table][v.Kind][v.ComponentIndex] {
				return cqltype.TypeValidation("schema key order", "duplicate position %d", v.ComponentIndex)
			}
			positions[v.Table][v.Kind][v.ComponentIndex] = true
		}
	}
	for _, table := range tables {
		if len(positions[table.Name][metadata.ColumnKindPartitionKey]) == 0 {
			return cqltype.TypeValidation("schema table", "missing partition key for %q", table.Name)
		}
	}
	for table, kinds := range positions {
		for _, pos := range kinds {
			for i := 0; i < len(pos); i++ {
				if !pos[i] {
					return cqltype.TypeValidation("schema key order", "noncontiguous positions for %q", table)
				}
			}
		}
	}
	names = nil
	for _, v := range functions {
		names = append(names, v2FunctionIdentity(v.Name, v.ArgumentTypes))
		if len(v.ArgumentNames) != len(v.ArgumentTypes) {
			return cqltype.TypeValidation("schema function", "argument count mismatch for %q", v.Name)
		}
		for _, typ := range append(append([]string(nil), v.ArgumentTypes...), v.ReturnType) {
			if _, err := validateV2CQLTypeLimit(ks.Name, typ, udts, maxDepth); err != nil {
				return err
			}
		}
	}
	if err := unique("function", names); err != nil {
		return err
	}
	names = nil
	for _, v := range aggregates {
		names = append(names, v2FunctionIdentity(v.Name, v.ArgumentTypes))
		if resolveV2Function(functions, AggregateMetadataData(&v).InternalStateFunc, append([]string{v.StateType}, v.ArgumentTypes...)) == nil {
			return cqltype.TypeValidation("schema aggregate", "unknown state function %q", AggregateMetadataData(&v).InternalStateFunc)
		}
		if AggregateMetadataData(&v).InternalFinalFunc != "" && resolveV2Function(functions, AggregateMetadataData(&v).InternalFinalFunc, []string{v.StateType}) == nil {
			return cqltype.TypeValidation("schema aggregate", "unknown final function %q", AggregateMetadataData(&v).InternalFinalFunc)
		}
		for _, typ := range append(append([]string(nil), v.ArgumentTypes...), v.StateType, v.ReturnType) {
			if _, err := validateV2CQLTypeLimit(ks.Name, typ, udts, maxDepth); err != nil {
				return err
			}
		}
	}
	return unique("aggregate", names)
}

// v2FunctionIdentity is the key of a function or aggregate in its
// Keyspace map, the signature metadata.Keyspace.Function looks up.
func v2FunctionIdentity(name string, args []string) string {
	return name + "(" + strings.Join(args, ",") + ")"
}

// uniqueV2Function returns the only function named name, or nil when there
// is none or more than one.
func uniqueV2Function(functions []metadata.Function, name string) *metadata.Function {
	var found *metadata.Function
	for i := range functions {
		if functions[i].Name != name {
			continue
		}
		if found != nil {
			return nil
		}
		found = &functions[i]
	}
	return found
}
func resolveV2Function(functions []metadata.Function, name string, args []string) *metadata.Function {
	for i := range functions {
		f := &functions[i]
		if f.Name != name || len(f.ArgumentTypes) != len(args) {
			continue
		}
		same := true
		for i := range args {
			if f.ArgumentTypes[i] != args[i] {
				same = false
				break
			}
		}
		if same {
			return f
		}
	}
	return nil
}

type v2CQLTypeParser struct {
	udts             map[string]bool
	source, keyspace string
	refs             []string
	offset           int
	maxDepth         int
}

func InternalValidateV2CQLType(keyspace, source string, udts map[string]bool) ([]string, error) {
	return validateV2CQLTypeLimit(keyspace, source, udts, 64)
}
func validateV2CQLTypeLimit(keyspace, source string, udts map[string]bool, maxDepth int) ([]string, error) {

	p := v2CQLTypeParser{source: source, keyspace: keyspace, udts: udts, maxDepth: maxDepth}
	if err := p.parse(0, false); err != nil {
		return nil, err
	}
	p.space()
	if p.offset != len(source) {
		return nil, cqltype.TypeValidation("schema type", "trailing syntax in %q", source)
	}
	return p.refs, nil
}
func (p *v2CQLTypeParser) space() {
	for p.offset < len(p.source) {
		switch p.source[p.offset] {
		case ' ', '\t', '\r', '\n':
			p.offset++
		default:
			return
		}
	}
}
func (p *v2CQLTypeParser) identifier() (string, bool, error) {
	p.space()
	start := p.offset
	if start >= len(p.source) {
		return "", false, cqltype.TypeValidation("schema type", "truncated type %q", p.source)
	}
	if p.source[start] == '"' {
		p.offset++
		var out strings.Builder
		for p.offset < len(p.source) {
			c := p.source[p.offset]
			p.offset++
			if c == '"' {
				if p.offset < len(p.source) && p.source[p.offset] == '"' {
					p.offset++
					out.WriteByte('"')
					continue
				}
				if out.Len() == 0 {
					return "", true, cqltype.TypeValidation("schema type", "empty identifier")
				}
				return out.String(), true, nil
			}
			out.WriteByte(c)
		}
		return "", true, cqltype.TypeValidation("schema type", "unterminated identifier")
	}
	for p.offset < len(p.source) {
		c := p.source[p.offset]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' || p.offset > start && c >= '0' && c <= '9' {
			p.offset++
		} else {
			break
		}
	}
	if p.offset == start {
		return "", false, cqltype.TypeValidation("schema type", "invalid identifier in %q", p.source)
	}
	return strings.ToLower(p.source[start:p.offset]), false, nil
}
func (p *v2CQLTypeParser) parse(depth int, frozen bool) error {
	if depth >= p.maxDepth {
		return cqltype.TypeValidation("schema type", "maximum nesting depth exceeded")
	}
	p.space()
	if p.offset < len(p.source) && p.source[p.offset] == '\'' {
		p.offset++
		for p.offset < len(p.source) {
			c := p.source[p.offset]
			p.offset++
			if c == '\'' {
				if p.offset < len(p.source) && p.source[p.offset] == '\'' {
					p.offset++
					continue
				}
				return nil
			}
		}
		return cqltype.TypeValidation("schema type", "unterminated custom type")
	}
	name, quoted, err := p.identifier()
	if err != nil {
		return err
	}
	p.space()
	if p.offset < len(p.source) && p.source[p.offset] == '.' {
		if name != p.keyspace {
			return cqltype.TypeValidation("schema type", "foreign keyspace %q", name)
		}
		p.offset++
		name, quoted, err = p.identifier()
		if err != nil {
			return err
		}
		quoted = true
		p.space()
	}
	if p.offset < len(p.source) && p.source[p.offset] == '<' {
		if quoted {
			return cqltype.TypeValidation("schema type", "UDT cannot have parameters")
		}
		switch name {
		case "list", "set", "map", "tuple", "frozen", "vector":
		default:
			return cqltype.TypeValidation("schema type", "unknown constructor %q", name)
		}
		p.offset++
		count := 0
		for {
			if name == "vector" && count == 1 {
				p.space()
				start := p.offset
				for p.offset < len(p.source) && p.source[p.offset] >= '0' && p.source[p.offset] <= '9' {
					p.offset++
				}
				dimension, err := strconv.ParseUint(p.source[start:p.offset], 10, 16)
				if err != nil || dimension == 0 {
					return cqltype.TypeValidation("schema type", "invalid vector dimension")
				}
			} else {
				if err := p.parse(depth+1, name == "frozen"); err != nil {
					return err
				}
			}
			count++
			p.space()
			if p.offset >= len(p.source) {
				return cqltype.TypeValidation("schema type", "unterminated parameters")
			}
			c := p.source[p.offset]
			p.offset++
			if c == '>' {
				break
			}
			if c != ',' {
				return cqltype.TypeValidation("schema type", "invalid parameter separator")
			}
		}
		expected := 1
		if name == "map" || name == "vector" {
			expected = 2
		}
		if name != "tuple" && count != expected {
			return cqltype.TypeValidation("schema type", "wrong arity for %q", name)
		}
		return nil
	}
	if !quoted && !frozen {
		if _, ok := cqlNativeTypes[name]; ok || name == "empty" {
			return nil
		}
	}
	if !p.udts[name] {
		return cqltype.TypeValidation("schema type", "unknown UDT %q", name)
	}
	p.refs = append(p.refs, name)
	return nil
}

func wrapV2SchemaQueryError(statement string, err error) error {
	if err == nil {
		return nil
	}
	kind := "keyspace"
	switch {
	case strings.Contains(statement, "system_schema.scylla_tables"):
		kind = "scylla table"
	case strings.Contains(statement, "system_schema.tables"):
		kind = "table"
	case strings.Contains(statement, "system_schema.columns"):
		kind = "column"
	case strings.Contains(statement, "system_schema.views"):
		kind = "view"
	case strings.Contains(statement, "system_schema.types"):
		kind = "type"
	case strings.Contains(statement, "system_schema.indexes"):
		kind = "index"
	case strings.Contains(statement, "system_schema.functions"):
		kind = "function"
	case strings.Contains(statement, "system_schema.aggregates"):
		kind = "aggregate"
	}
	return fmt.Errorf("error querying %s schema: %w", kind, err)
}
