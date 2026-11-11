// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

func cloneV2Any(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return cloneV2AnyMap(x)
	case map[string]string:
		return maps.Clone(x)
	case []byte:
		return append([]byte(nil), x...)
	case []string:
		return append([]string(nil), x...)
	case []any:
		r := make([]any, len(x))
		for i, v := range x {
			r[i] = cloneV2Any(v)
		}
		return r
	default:
		return v
	}
}

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
func cloneV2AnyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	r := make(map[string]any, len(m))
	for k, v := range m {
		r[k] = cloneV2Any(v)
	}
	return r
}
func cloneV2Column(c *ColumnMetadata) *ColumnMetadata {
	if c == nil {
		return nil
	}
	r := *c
	r.Index.Options = cloneV2AnyMap(c.Index.Options)
	return &r
}
func cloneV2Columns(c map[string]*ColumnMetadata) map[string]*ColumnMetadata {
	if c == nil {
		return nil
	}
	r := make(map[string]*ColumnMetadata, len(c))
	for k, v := range c {
		r[k] = cloneV2Column(v)
	}
	return r
}
func cloneV2ColumnList(c []*ColumnMetadata) []*ColumnMetadata {
	if c == nil {
		return nil
	}
	r := make([]*ColumnMetadata, len(c))
	for i, v := range c {
		r[i] = cloneV2Column(v)
	}
	return r
}
func cloneV2TableOptions(o TableMetadataOptions) TableMetadataOptions {
	o.Caching = maps.Clone(o.Caching)
	o.Compaction = maps.Clone(o.Compaction)
	o.Compression = maps.Clone(o.Compression)
	o.CDC = maps.Clone(o.CDC)
	return o
}
func cloneV2Function(f FunctionMetadata) FunctionMetadata {
	f.ArgumentTypes = append([]string(nil), f.ArgumentTypes...)
	f.ArgumentNames = append([]string(nil), f.ArgumentNames...)
	return f
}
func InternalCloneV2Keyspace(k *KeyspaceMetadata) *KeyspaceMetadata {
	if k == nil {
		return nil
	}
	r := *k
	r.StrategyOptions = maps.Clone(k.StrategyOptions)
	KeyspaceData(&r).InternalTablesInvalidated = maps.Clone(KeyspaceData(k).InternalTablesInvalidated)
	r.Tables = make(map[string]*TableMetadata, len(k.Tables))
	for n, t := range k.Tables {
		if t == nil {
			r.Tables[n] = nil
			continue
		}
		v := *t
		v.Columns = cloneV2Columns(t.Columns)
		v.Extensions = cloneV2Extensions(t.Extensions)
		v.PartitionKey = cloneV2ColumnList(t.PartitionKey)
		v.ClusteringColumns = cloneV2ColumnList(t.ClusteringColumns)
		v.OrderedColumns = append([]string(nil), t.OrderedColumns...)
		v.Flags = append([]string(nil), t.Flags...)
		v.Options = cloneV2TableOptions(t.Options)
		r.Tables[n] = &v
	}
	r.Functions = make(map[string]*FunctionMetadata, len(k.Functions))
	for n, f := range k.Functions {
		if f == nil {
			r.Functions[n] = nil
			continue
		}
		v := cloneV2Function(*f)
		r.Functions[n] = &v
	}
	r.Aggregates = make(map[string]*AggregateMetadata, len(k.Aggregates))
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
	r.Types = make(map[string]*TypeMetadata, len(k.Types))
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
	r.Views = make(map[string]*ViewMetadata, len(k.Views))
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
	r.Indexes = make(map[string]*IndexMetadata, len(k.Indexes))
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

// PartialRefreshError reports keyspaces whose previous valid schema was kept.
type PartialRefreshError struct {
	Topology     error
	ClientRoutes error
	Keyspaces    map[string]error
	Generation   uint64
}

func (e *PartialRefreshError) Error() string {
	names := make([]string, 0, len(e.Keyspaces))
	for name := range e.Keyspaces {
		names = append(names, name)
	}
	sort.Strings(names)
	var details []string
	for _, name := range names {
		details = append(details, fmt.Sprintf("%s: %v", name, e.Keyspaces[name]))
	}
	if e.Topology != nil {
		details = append(details, "topology: "+e.Topology.Error())
	}
	if e.ClientRoutes != nil {
		details = append(details, "client routes: "+e.ClientRoutes.Error())
	}
	return fmt.Sprintf("gocql: metadata generation %d retained stale data (%s)", e.Generation, strings.Join(details, "; "))
}
func (e *PartialRefreshError) Unwrap() []error {
	names := make([]string, 0, len(e.Keyspaces))
	for name := range e.Keyspaces {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]error, 0, len(names))
	for _, name := range names {
		out = append(out, e.Keyspaces[name])
	}
	if e.Topology != nil {
		out = append(out, e.Topology)
	}
	if e.ClientRoutes != nil {
		out = append(out, e.ClientRoutes)
	}
	return out
}

// NoHostsError distinguishes an empty eligible peer scope from agreement.
type NoHostsError struct {
	Operation string
	Causes    []error
}

func (e *NoHostsError) Error() string   { return "gocql: no eligible hosts for " + e.Operation }
func (e *NoHostsError) Unwrap() []error { return append([]error(nil), e.Causes...) }

// Schema rows intentionally materialize small metadata values on the cold path.
// Fields are explicitly registered; no reflection or destination inference runs.
type InternalV2SchemaField[R any] struct {
	compile  func(cqltype.DataType) (func(*R, *codec.RowReader) error, error)
	name     string
	required bool
}

func InternalV2SchemaColumn[R, V any](name string, decoder codec.Decoder[V], field func(*R) *V) InternalV2SchemaField[R] {
	return InternalV2SchemaField[R]{name: name, compile: func(typ cqltype.DataType) (func(*R, *codec.RowReader) error, error) {
		nullable := codec.NullableDecoder(decoder)
		if err := nullable.CheckType(typ); err != nil {
			return nil, err
		}
		return func(row *R, reader *codec.RowReader) error {
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
func InternalV2RequiredSchemaColumn[R, V any](name string, decoder codec.Decoder[V], field func(*R) *V) InternalV2SchemaField[R] {
	return InternalV2SchemaField[R]{name: name, required: true, compile: func(typ cqltype.DataType) (func(*R, *codec.RowReader) error, error) {
		if err := decoder.CheckType(typ); err != nil {
			return nil, err
		}
		return func(row *R, reader *codec.RowReader) error {
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
	initialize func() *R
	steps      []func(*R, *codec.RowReader) error
}

func (d InternalV2SchemaRows[R]) Compile(columns cqltype.Columns) (codec.RowPlan[R], error) {
	fields := make(map[string]InternalV2SchemaField[R], len(d.InternalFields))
	for _, f := range d.InternalFields {
		if _, exists := fields[f.name]; exists {
			return nil, cqltype.TypeValidation("schema row", "duplicate application field %q", f.name)
		}
		fields[f.name] = f
	}
	steps := make([]func(*R, *codec.RowReader) error, columns.Len())
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
			steps[i] = func(_ *R, r *codec.RowReader) error { return r.SkipColumn() }
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
func (p v2SchemaRowPlan[R]) Decode(reader *codec.RowReader) (R, error) {
	var result R
	err := p.DecodeInto(reader, &result)
	if err != nil {
		return *new(R), err
	}
	return result, nil
}
func (p v2SchemaRowPlan[R]) DecodeInto(reader *codec.RowReader, dst *R) error {
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

type v2MetadataStrings struct{}

type v2MetadataStringMap struct{}

type v2MetadataExtensions struct{}

type v2SchemaParams struct{ count int }

func (v2SchemaParams) Compile(columns cqltype.Columns) (codec.ParamPlan[[]string], error) {
	for i := 0; i < columns.Len(); i++ {
		c, _ := columns.At(i)
		if err := codec.StringEncoder.CheckType(c.Type); err != nil {
			return nil, err
		}
	}
	return v2SchemaParams{count: columns.Len()}, nil
}
func (p v2SchemaParams) Encode(w *codec.ParamWriter, values []string) error {
	if len(values) != p.count {
		return cqltype.TypeValidation("schema parameters", "wrong count")
	}
	for _, value := range values {
		if err := w.WriteParam(codec.StringEncoder, value); err != nil {
			return err
		}
	}
	return nil
}

func InternalQueryV2SchemaRows[R any](ctx context.Context, s Source, stmt string, args []string, decoder codec.RowDecoder[R]) ([]R, error) {
	settings := s.InternalSchemaSettings()
	if !settings.SystemSchema {
		return nil, &runtimeerrors.UnsupportedSettingError{Setting: "schema metadata", Reason: "server does not provide system_schema tables"}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !settings.Available {
		return nil, InternalErrNoControl
	}
	typ := cqltype.VarcharType
	columns := make([]cqltype.ColumnSpec, len(args))
	for i := range columns {
		columns[i] = cqltype.ColumnSpec{Type: typ}
	}
	rows, err := ReadRows(ctx, 4, settings.Timeout, s.InternalQuerySystem, Query[[]string, R]{CQL: stmt, Params: v2SchemaParams{}, Rows: decoder}, cqltype.NewColumns(columns...), args)
	return rows, wrapV2SchemaQueryError(stmt, err)
}

func InternalV2SchemaContext(s Source) context.Context {
	if s != nil {
		return s.InternalSchemaContext()
	}
	return context.Background()
}

func v2DecodeTableMetadata(keyspace string) codec.RowDecoder[TableMetadata] {
	return InternalV2SchemaRows[TableMetadata]{initialize: func() *TableMetadata { return &TableMetadata{Keyspace: keyspace} }, InternalFields: []InternalV2SchemaField[TableMetadata]{
		InternalV2RequiredSchemaColumn("id", InternalV2MetadataUUIDString{}, func(r *TableMetadata) *string { return &r.ID }),
		InternalV2RequiredSchemaColumn("table_name", InternalV2MetadataText{}, func(r *TableMetadata) *string { return &r.Name }),
		InternalV2SchemaColumn("flags", v2MetadataStrings{}, func(r *TableMetadata) *[]string { return &r.Flags }),
		InternalV2SchemaColumn("extensions", v2MetadataExtensions{}, func(r *TableMetadata) *map[string][]byte { return &r.Extensions }),
		InternalV2SchemaColumn("bloom_filter_fp_chance", codec.Float64, func(r *TableMetadata) *float64 { return &r.Options.BloomFilterFpChance }),
		InternalV2SchemaColumn("caching", v2MetadataStringMap{}, func(r *TableMetadata) *map[string]string { return &r.Options.Caching }),
		InternalV2SchemaColumn("comment", InternalV2MetadataText{}, func(r *TableMetadata) *string { return &r.Options.Comment }),
		InternalV2SchemaColumn("compaction", v2MetadataStringMap{}, func(r *TableMetadata) *map[string]string { return &r.Options.Compaction }),
		InternalV2SchemaColumn("compression", v2MetadataStringMap{}, func(r *TableMetadata) *map[string]string { return &r.Options.Compression }),
		InternalV2SchemaColumn("crc_check_chance", codec.Float64, func(r *TableMetadata) *float64 { return &r.Options.CrcCheckChance }),
		InternalV2SchemaColumn("default_time_to_live", InternalV2MetadataInt{}, func(r *TableMetadata) *int { return &r.Options.DefaultTimeToLive }),
		InternalV2SchemaColumn("gc_grace_seconds", InternalV2MetadataInt{}, func(r *TableMetadata) *int { return &r.Options.GcGraceSeconds }),
		InternalV2SchemaColumn("max_index_interval", InternalV2MetadataInt{}, func(r *TableMetadata) *int { return &r.Options.MaxIndexInterval }),
		InternalV2SchemaColumn("memtable_flush_period_in_ms", InternalV2MetadataInt{}, func(r *TableMetadata) *int { return &r.Options.MemtableFlushPeriodInMs }),
		InternalV2SchemaColumn("min_index_interval", InternalV2MetadataInt{}, func(r *TableMetadata) *int { return &r.Options.MinIndexInterval }),
		InternalV2SchemaColumn("speculative_retry", InternalV2MetadataText{}, func(r *TableMetadata) *string { return &r.Options.SpeculativeRetry }),
	}}
}

func v2DecodeColumnMetadata(keyspace string) codec.RowDecoder[ColumnMetadata] {
	return InternalV2SchemaRows[ColumnMetadata]{initialize: func() *ColumnMetadata { return &ColumnMetadata{Keyspace: keyspace} }, InternalFields: []InternalV2SchemaField[ColumnMetadata]{
		InternalV2RequiredSchemaColumn("table_name", InternalV2MetadataText{}, func(r *ColumnMetadata) *string { return &r.Table }),
		InternalV2RequiredSchemaColumn("column_name", InternalV2MetadataText{}, func(r *ColumnMetadata) *string { return &r.Name }),
		InternalV2SchemaColumn("clustering_order", InternalV2MetadataText{}, func(r *ColumnMetadata) *string { return &r.ClusteringOrder }),
		InternalV2RequiredSchemaColumn("type", InternalV2MetadataText{}, func(r *ColumnMetadata) *string { return &r.Type }),
		InternalV2RequiredSchemaColumn("kind", v2MetadataKind{}, func(r *ColumnMetadata) *ColumnKind { return &r.Kind }),
		InternalV2RequiredSchemaColumn("position", InternalV2MetadataInt{}, func(r *ColumnMetadata) *int { return &r.ComponentIndex }),
	}}
}

func InternalV2DecodeTypeMetadata(keyspace string) codec.RowDecoder[TypeMetadata] {
	return InternalV2SchemaRows[TypeMetadata]{initialize: func() *TypeMetadata { return &TypeMetadata{Keyspace: keyspace} }, InternalFields: []InternalV2SchemaField[TypeMetadata]{
		InternalV2RequiredSchemaColumn("type_name", InternalV2MetadataText{}, func(r *TypeMetadata) *string { return &r.Name }),
		InternalV2RequiredSchemaColumn("field_names", v2MetadataStrings{}, func(r *TypeMetadata) *[]string { return &r.FieldNames }),
		InternalV2RequiredSchemaColumn("field_types", v2MetadataStrings{}, func(r *TypeMetadata) *[]string { return &r.FieldTypes }),
	}}
}

func InternalV2DecodeFunctionMetadata(keyspace string) codec.RowDecoder[FunctionMetadata] {
	return InternalV2SchemaRows[FunctionMetadata]{initialize: func() *FunctionMetadata { return &FunctionMetadata{Keyspace: keyspace} }, InternalFields: []InternalV2SchemaField[FunctionMetadata]{
		InternalV2RequiredSchemaColumn("function_name", InternalV2MetadataText{}, func(r *FunctionMetadata) *string { return &r.Name }),
		InternalV2RequiredSchemaColumn("argument_types", v2MetadataStrings{}, func(r *FunctionMetadata) *[]string { return &r.ArgumentTypes }),
		InternalV2RequiredSchemaColumn("argument_names", v2MetadataStrings{}, func(r *FunctionMetadata) *[]string { return &r.ArgumentNames }),
		InternalV2RequiredSchemaColumn("body", InternalV2MetadataText{}, func(r *FunctionMetadata) *string { return &r.Body }),
		InternalV2RequiredSchemaColumn("called_on_null_input", codec.Bool, func(r *FunctionMetadata) *bool { return &r.CalledOnNullInput }),
		InternalV2RequiredSchemaColumn("language", InternalV2MetadataText{}, func(r *FunctionMetadata) *string { return &r.Language }),
		InternalV2RequiredSchemaColumn("return_type", InternalV2MetadataText{}, func(r *FunctionMetadata) *string { return &r.ReturnType }),
	}}
}

func InternalV2DecodeAggregateMetadata(keyspace string) codec.RowDecoder[AggregateMetadata] {
	return InternalV2SchemaRows[AggregateMetadata]{initialize: func() *AggregateMetadata { return &AggregateMetadata{Keyspace: keyspace} }, InternalFields: []InternalV2SchemaField[AggregateMetadata]{
		InternalV2RequiredSchemaColumn("aggregate_name", InternalV2MetadataText{}, func(r *AggregateMetadata) *string { return &r.Name }),
		InternalV2RequiredSchemaColumn("argument_types", v2MetadataStrings{}, func(r *AggregateMetadata) *[]string { return &r.ArgumentTypes }),
		InternalV2SchemaColumn("final_func", InternalV2MetadataText{}, func(r *AggregateMetadata) *string { return &r.privateStorage.InternalFinalFunc }),
		InternalV2SchemaColumn("initcond", InternalV2MetadataText{}, func(r *AggregateMetadata) *string { return &r.InitCond }),
		InternalV2RequiredSchemaColumn("return_type", InternalV2MetadataText{}, func(r *AggregateMetadata) *string { return &r.ReturnType }),
		InternalV2RequiredSchemaColumn("state_func", InternalV2MetadataText{}, func(r *AggregateMetadata) *string { return &r.privateStorage.InternalStateFunc }),
		InternalV2RequiredSchemaColumn("state_type", InternalV2MetadataText{}, func(r *AggregateMetadata) *string { return &r.StateType }),
	}}
}

func v2DecodeIndexMetadata(keyspace string) codec.RowDecoder[IndexMetadata] {
	return InternalV2SchemaRows[IndexMetadata]{initialize: func() *IndexMetadata { return &IndexMetadata{KeyspaceName: keyspace} }, InternalFields: []InternalV2SchemaField[IndexMetadata]{
		InternalV2RequiredSchemaColumn("index_name", InternalV2MetadataText{}, func(r *IndexMetadata) *string { return &r.Name }),
		InternalV2RequiredSchemaColumn("keyspace_name", InternalV2MetadataText{}, func(r *IndexMetadata) *string { return &r.KeyspaceName }),
		InternalV2RequiredSchemaColumn("table_name", InternalV2MetadataText{}, func(r *IndexMetadata) *string { return &r.TableName }),
		InternalV2RequiredSchemaColumn("kind", InternalV2MetadataText{}, func(r *IndexMetadata) *string { return (*string)(&r.Kind) }),
		InternalV2RequiredSchemaColumn("options", v2MetadataStringMap{}, func(r *IndexMetadata) *map[string]string { return &r.Options }),
	}}
}

func v2DecodeViewMetadata(keyspace string) codec.RowDecoder[ViewMetadata] {
	return InternalV2SchemaRows[ViewMetadata]{initialize: func() *ViewMetadata { return &ViewMetadata{KeyspaceName: keyspace} }, InternalFields: []InternalV2SchemaField[ViewMetadata]{
		InternalV2RequiredSchemaColumn("id", InternalV2MetadataUUIDString{}, func(r *ViewMetadata) *string { return &r.ID }),
		InternalV2RequiredSchemaColumn("view_name", InternalV2MetadataText{}, func(r *ViewMetadata) *string { return &r.ViewName }),
		InternalV2RequiredSchemaColumn("base_table_id", InternalV2MetadataUUIDString{}, func(r *ViewMetadata) *string { return &r.BaseTableID }),
		InternalV2RequiredSchemaColumn("base_table_name", InternalV2MetadataText{}, func(r *ViewMetadata) *string { return &r.BaseTableName }),
		InternalV2RequiredSchemaColumn("include_all_columns", codec.Bool, func(r *ViewMetadata) *bool { return &r.IncludeAllColumns }),
		InternalV2RequiredSchemaColumn("where_clause", InternalV2MetadataText{}, func(r *ViewMetadata) *string { return &r.WhereClause }),
		InternalV2SchemaColumn("extensions", v2MetadataExtensions{}, func(r *ViewMetadata) *map[string][]byte { return &r.Extensions }),
		InternalV2SchemaColumn("dclocal_read_repair_chance", codec.Float64, func(r *ViewMetadata) *float64 { return &r.DcLocalReadRepairChance }),
		InternalV2SchemaColumn("read_repair_chance", codec.Float64, func(r *ViewMetadata) *float64 { return &r.ReadRepairChance }),
		InternalV2SchemaColumn("bloom_filter_fp_chance", codec.Float64, func(r *ViewMetadata) *float64 { return &r.Options.BloomFilterFpChance }),
		InternalV2SchemaColumn("caching", v2MetadataStringMap{}, func(r *ViewMetadata) *map[string]string { return &r.Options.Caching }),
		InternalV2SchemaColumn("comment", InternalV2MetadataText{}, func(r *ViewMetadata) *string { return &r.Options.Comment }),
		InternalV2SchemaColumn("compaction", v2MetadataStringMap{}, func(r *ViewMetadata) *map[string]string { return &r.Options.Compaction }),
		InternalV2SchemaColumn("compression", v2MetadataStringMap{}, func(r *ViewMetadata) *map[string]string { return &r.Options.Compression }),
		InternalV2SchemaColumn("crc_check_chance", codec.Float64, func(r *ViewMetadata) *float64 { return &r.Options.CrcCheckChance }),
		InternalV2SchemaColumn("default_time_to_live", InternalV2MetadataInt{}, func(r *ViewMetadata) *int { return &r.Options.DefaultTimeToLive }),
		InternalV2SchemaColumn("gc_grace_seconds", InternalV2MetadataInt{}, func(r *ViewMetadata) *int { return &r.Options.GcGraceSeconds }),
		InternalV2SchemaColumn("max_index_interval", InternalV2MetadataInt{}, func(r *ViewMetadata) *int { return &r.Options.MaxIndexInterval }),
		InternalV2SchemaColumn("memtable_flush_period_in_ms", InternalV2MetadataInt{}, func(r *ViewMetadata) *int { return &r.Options.MemtableFlushPeriodInMs }),
		InternalV2SchemaColumn("min_index_interval", InternalV2MetadataInt{}, func(r *ViewMetadata) *int { return &r.Options.MinIndexInterval }),
		InternalV2SchemaColumn("speculative_retry", InternalV2MetadataText{}, func(r *ViewMetadata) *string { return &r.Options.SpeculativeRetry }),
	}}
}

type v2ScyllaTableRow struct {
	Name    string
	Options TableMetadataOptions
}

func v2DecodeScyllaTable() codec.RowDecoder[v2ScyllaTableRow] {
	return InternalV2SchemaRows[v2ScyllaTableRow]{InternalFields: []InternalV2SchemaField[v2ScyllaTableRow]{InternalV2RequiredSchemaColumn("table_name", InternalV2MetadataText{}, func(r *v2ScyllaTableRow) *string { return &r.Name }),
		InternalV2SchemaColumn("cdc", v2MetadataStringMap{}, func(r *v2ScyllaTableRow) *map[string]string { return &r.Options.CDC }),
		InternalV2SchemaColumn("in_memory", codec.Bool, func(r *v2ScyllaTableRow) *bool { return &r.Options.InMemory }),
		InternalV2SchemaColumn("partitioner", InternalV2MetadataText{}, func(r *v2ScyllaTableRow) *string { return &r.Options.Partitioner }),
		InternalV2SchemaColumn("version", InternalV2MetadataUUIDString{}, func(r *v2ScyllaTableRow) *string { return &r.Options.Version }),
	}}
}

type v2KeyspaceRow struct {
	replication  map[string]string
	InternalName string
	durable      bool
}

func InternalV2DecodeKeyspace() codec.RowDecoder[v2KeyspaceRow] {
	return InternalV2SchemaRows[v2KeyspaceRow]{InternalFields: []InternalV2SchemaField[v2KeyspaceRow]{InternalV2SchemaColumn("keyspace_name", InternalV2MetadataText{}, func(r *v2KeyspaceRow) *string { return &r.InternalName }), InternalV2RequiredSchemaColumn("durable_writes", codec.Bool, func(r *v2KeyspaceRow) *bool { return &r.durable }), InternalV2RequiredSchemaColumn("replication", v2MetadataStringMap{}, func(r *v2KeyspaceRow) *map[string]string { return &r.replication })}}
}
func InternalV2KeyspaceFromRow(name string, row v2KeyspaceRow) *KeyspaceMetadata {
	ks := &KeyspaceMetadata{Name: name, DurableWrites: row.durable, StrategyClass: row.replication["class"], StrategyOptions: make(map[string]string, len(row.replication))}
	for k, v := range row.replication {
		if k != "class" {
			ks.StrategyOptions[k] = v
		}
	}
	return ks
}
func InternalGetV2KeyspaceMetadata(ctx context.Context, s Source, name string) (*KeyspaceMetadata, error) {
	rows, err := InternalQueryV2SchemaRows(ctx, s, `SELECT durable_writes, replication FROM system_schema.keyspaces WHERE keyspace_name = ?`, []string{name}, InternalV2DecodeKeyspace())
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, runtimeerrors.ErrKeyspaceDoesNotExist
	}
	if len(rows) != 1 {
		return nil, cqltype.TypeValidation("keyspace schema", "duplicate identity %q", name)
	}
	ks := InternalV2KeyspaceFromRow(name, rows[0])
	if err := InternalReadV2ScyllaKeyspaces(ctx, s, map[string]*KeyspaceMetadata{name: ks}, name); err != nil {
		return nil, err
	}
	return ks, nil
}
func InternalGetV2TableMetadata(ctx context.Context, s Source, keyspace, table string) ([]TableMetadata, error) {
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
	options := make(map[string]TableMetadataOptions, len(extra))
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
func InternalGetV2ColumnMetadata(ctx context.Context, s Source, keyspace, table string) ([]ColumnMetadata, error) {
	where := ` WHERE keyspace_name = ?`
	args := []string{keyspace}
	if table != "" {
		where += ` AND table_name = ?`
		args = append(args, table)
	}
	return InternalQueryV2SchemaRows(ctx, s, `SELECT `+InternalColumnMetadataColumns+` FROM system_schema.columns`+where, args, v2DecodeColumnMetadata(keyspace))
}
func InternalGetV2IndexMetadata(ctx context.Context, s Source, keyspace, table string) ([]IndexMetadata, error) {
	where := ` WHERE keyspace_name = ?`
	args := []string{keyspace}
	if table != "" {
		where += ` AND table_name = ?`
		args = append(args, table)
	}
	return InternalQueryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.indexes`+where, args, v2DecodeIndexMetadata(keyspace))
}
func InternalGetV2ViewMetadata(ctx context.Context, s Source, keyspace, table string) ([]ViewMetadata, error) {
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
		if server, ok := errors.AsType[protocol.RequestError](err); ok && server.GetCode() == protocol.ErrCodeSyntax {
			return nil, nil
		}
		return nil, err
	}
	return []byte(strings.Join(rows, "\n")), nil
}

func InternalFetchV2Keyspace(ctx context.Context, s Source, base *KeyspaceMetadata) (*KeyspaceMetadata, error) {
	var tables []TableMetadata
	var columns []ColumnMetadata
	var types []TypeMetadata
	var functions []FunctionMetadata
	var aggregates []AggregateMetadata
	var indexes []IndexMetadata
	var views []ViewMetadata
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

func InternalValidateV2SchemaParts(ks *KeyspaceMetadata, tables []TableMetadata, columns []ColumnMetadata, types []TypeMetadata, functions []FunctionMetadata, aggregates []AggregateMetadata, indexes []IndexMetadata, views []ViewMetadata, depthLimit ...int) error {
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
		names = append(names, v.ViewName)
		if knownTables[v.ViewName] {
			return cqltype.TypeValidation("schema", "table/view identity collision %q", v.ViewName)
		}
		knownTables[v.ViewName] = true
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
	positions := make(map[string]map[ColumnKind]map[int]bool)
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
		if v.Kind == ColumnPartitionKey || v.Kind == ColumnClusteringKey {
			if v.ComponentIndex < 0 || v.ComponentIndex >= len(columns) {
				return cqltype.TypeValidation("schema key order", "invalid position %d", v.ComponentIndex)
			}
			if positions[v.Table] == nil {
				positions[v.Table] = make(map[ColumnKind]map[int]bool)
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
		if len(positions[table.Name][ColumnPartitionKey]) == 0 {
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
		if resolveV2Function(functions, v.privateStorage.InternalStateFunc, append([]string{v.StateType}, v.ArgumentTypes...)) == nil {
			return cqltype.TypeValidation("schema aggregate", "unknown state function %q", v.privateStorage.InternalStateFunc)
		}
		if v.privateStorage.InternalFinalFunc != "" && resolveV2Function(functions, v.privateStorage.InternalFinalFunc, []string{v.StateType}) == nil {
			return cqltype.TypeValidation("schema aggregate", "unknown final function %q", v.privateStorage.InternalFinalFunc)
		}
		for _, typ := range append(append([]string(nil), v.ArgumentTypes...), v.StateType, v.ReturnType) {
			if _, err := validateV2CQLTypeLimit(ks.Name, typ, udts, maxDepth); err != nil {
				return err
			}
		}
	}
	return unique("aggregate", names)
}
func v2FunctionIdentity(name string, args []string) string {
	return name + "(" + strings.Join(args, ",") + ")"
}
func resolveV2Function(functions []FunctionMetadata, name string, args []string) *FunctionMetadata {
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
