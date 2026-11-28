// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"github.com/scylladb/gocql/v2/internal/syncx"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// udtOptions names the UDT a UDT codec expects and relaxes field matching:
// UDTName, IgnoreUnknownFields and AllowMissingFields.
type udtOptions struct {
	keyspace, name string
	allowExtra     bool // UDT fields that no struct field maps
	allowMissing   bool // struct fields the UDT does not have
}

// udtOptionsOf applies opts for UDTEncoder, UDTDecoder or StreamUDT.
func udtOptionsOf(opts []StructOption) (udtOptions, error) {
	set, err := applyOptions(targetUDTValue, "a UDT", opts)
	return set.udt, err
}

type recordEncoder[T any] struct {
	codec   ParamEncoder[T]
	plan    CompiledParams[T]
	typ     DataType
	options udtOptions
	optErr  error
	columns Columns
	kind    Kind
}

// TupleEncoder encodes its parameters as members of one physical tuple cell.
func TupleEncoder[T any](codec ParamEncoder[T]) Encoder[T] {
	return recordEncoder[T]{kind: KindTuple, codec: codec}
}

// UDTEncoder encodes a UDT's fields with codec, matched by name against the
// UDT's metadata, as named by UDTName and relaxed by IgnoreUnknownFields and
// AllowMissingFields; it rejects other options when it is compiled.
func UDTEncoder[T any](codec ParamEncoder[T], opts ...StructOption) Encoder[T] {
	options, err := udtOptionsOf(opts)
	return recordEncoder[T]{kind: KindUDT, codec: codec, options: options, optErr: err}
}
func recordColumns(typ DataType, kind Kind) (Columns, error) {
	if err := validateDataType(typ); err != nil {
		return Columns{}, err
	}
	if typ.Kind() != kind {
		return Columns{}, typeValidation("type", "expected %s, got %s", kind, typ)
	}
	// The descriptor is immutable, so a compiled plan can retain its columns
	// without rebuilding the public defensive copy on every value.
	return descriptorOf(typ).Fields, nil
}
func (c recordEncoder[T]) CheckType(typ DataType) error {
	perfCheckType()

	_, err := c.compileValueEncoder(typ)
	return err
}
func (c recordEncoder[T]) compileValueEncoder(typ DataType) (Encoder[T], error) {
	if c.optErr != nil {
		return nil, c.optErr
	}
	if c.kind == KindUDT {
		if err := checkUDTIdentity(typ, c.options); err != nil {
			return nil, err
		}
	}
	columns, err := recordColumns(typ, c.kind)
	if err != nil {
		return nil, err
	}
	if c.codec == nil {
		return nil, typeValidation("encoder", "nil tuple/UDT parameter encoder")
	}
	var plan CompiledParams[T]
	if c.kind == KindUDT {
		plan, err = compileUDTParamEncoder(c.codec, columns, c.options)
	} else {
		plan, err = compileParams(c.codec, columns)
	}
	if err != nil {
		return nil, recordMemberError(c.kind, err)
	}
	c.typ, c.columns, c.plan = typ, columns, plan
	return c, nil
}
func (c recordEncoder[T]) Encode(w *ValueWriter, value T) error {
	if c.plan == nil {
		bound, err := c.compileValueEncoder(w.Type())
		if err != nil {
			return err
		}
		return bound.Encode(w, value)
	}
	if !c.typ.Equal(w.Type()) {
		return typeValidation("type", "tuple/UDT identity or fields changed after compilation")
	}
	if err := w.payload(); err != nil {
		return err
	}
	params := newParamWriter(w.builder, c.columns, false)
	params.ownBorrowed = w.ownBorrowed
	if err := c.plan.Encode(params, value); err != nil {
		return w.fail(recordMemberError(c.kind, err))
	}
	return w.fail(finishParamWriter(params))
}

type recordDecoder[T any] struct {
	codec   RowDecoder[T]
	plan    CompiledRow[T]
	typ     DataType
	readers *syncx.Pool[*recordReaderState]
	values  *syncx.Pool[*T]
	options udtOptions
	optErr  error
	columns Columns
	kind    Kind
}

type recordReaderState struct {
	reader RowReader
	cursor wire.Cursor
}

// TupleDecoder consumes tuple members inside one cell without expanding result
// columns. A missing serialized suffix is passed to member decoders as null.
func TupleDecoder[T any](codec RowDecoder[T]) Decoder[T] {
	return recordDecoder[T]{kind: KindTuple, codec: codec}
}

// UDTDecoder decodes a UDT's fields with codec, matched by name against the
// UDT's metadata, as named by UDTName and relaxed by IgnoreUnknownFields and
// AllowMissingFields; it rejects other options when it is compiled. With a
// Row or RowOf codec, a NULL field, and a trailing field the server left
// out because it was added by ALTER TYPE ADD after the value was written,
// decodes as the field's zero value.
func UDTDecoder[T any](codec RowDecoder[T], opts ...StructOption) Decoder[T] {
	options, err := udtOptionsOf(opts)
	return recordDecoder[T]{kind: KindUDT, codec: codec, options: options, optErr: err}
}
func (c recordDecoder[T]) CheckType(typ DataType) error {
	perfCheckType()

	_, err := c.compileValueDecoder(typ)
	return err
}
func (c recordDecoder[T]) compileValueDecoder(typ DataType) (Decoder[T], error) {
	if c.optErr != nil {
		return nil, c.optErr
	}
	if c.kind == KindUDT {
		if err := checkUDTIdentity(typ, c.options); err != nil {
			return nil, err
		}
	}
	columns, err := recordColumns(typ, c.kind)
	if err != nil {
		return nil, err
	}
	if c.codec == nil {
		return nil, typeValidation("decoder", "nil tuple/UDT row decoder")
	}
	var plan CompiledRow[T]
	if c.kind == KindUDT {
		plan, err = compileUDTRowDecoder(c.codec, columns, c.options)
	} else {
		plan, err = compileRow(c.codec, columns)
	}
	if err != nil {
		return nil, recordMemberError(c.kind, err)
	}
	c.typ, c.columns, c.plan = typ, columns, plan
	c.readers = syncx.NewPool(func() *recordReaderState { return new(recordReaderState) })
	c.values = syncx.NewPool(func() *T { return new(T) })
	return c, nil
}
func (c recordDecoder[T]) Decode(r ValueReader) (T, error) {
	if c.plan == nil {
		bound, err := c.compileValueDecoder(r.Type())
		if err != nil {
			return *new(T), err
		}
		return bound.Decode(r)
	}
	slot := c.values.Get()
	err := c.DecodeInto(r, slot)
	value := *slot
	var zero T
	*slot = zero
	c.values.Put(slot)
	if err != nil {
		return zero, err
	}
	return value, nil
}
func (c recordDecoder[T]) DecodeInto(r ValueReader, dst *T) error {
	if dst == nil {
		return typeValidation("destination", "nil record destination")
	}
	if c.plan == nil {
		bound, err := c.compileValueDecoder(r.Type())
		if err != nil {
			return err
		}
		return bound.(valueIntoDecoder[T]).DecodeInto(r, dst)
	}
	if !c.typ.Equal(r.Type()) {
		return typeValidation("type", "tuple/UDT identity or fields changed after compilation")
	}
	if err := requirePresent(r); err != nil {
		return err
	}
	if err := validateRecordBody(r, c.columns); err != nil {
		return recordMemberError(c.kind, err)
	}
	state := c.readers.Get()
	state.cursor = *wire.NewCursor(r.body)
	state.reader = RowReader{columns: c.columns, cursor: &state.cursor, lease: r.lease, allowMissing: true}
	err := c.plan.DecodeInto(&state.reader, dst)
	if err != nil {
		err = recordMemberError(c.kind, err)
	} else {
		err = finishRowReader(&state.reader)
	}
	if err == nil {
		err = state.cursor.RequireEnd()
	}
	*state = recordReaderState{}
	c.readers.Put(state)
	return err
}
func (c recordDecoder[T]) Clone(value T) T {
	return c.codec.Clone(value)
}

func validateRecordBody(r ValueReader, columns Columns) error {
	cursor := r.cursorValue()
	for index, column := range columnSlice(columns) {
		if cursor.Remaining() == 0 {
			break
		}
		if _, err := cursor.Child(column.Type); err != nil {
			return columnError(CodecStageDecode, index, column.Name, &ProtocolError{Stage: "cell", Err: err})
		}
	}
	if err := cursor.RequireEnd(); err != nil {
		return &ProtocolError{Stage: "cell", Err: err}
	}
	return nil
}

// udtFieldMapping resolves source fields once while distinguishing missing
// application fields from unmatched database fields.
func udtFieldMapping(columns Columns, locations []fieldLocation, options udtOptions) ([]int, error) {
	mapping := make([]int, columns.Len())
	for i := range mapping {
		mapping[i] = -1
	}
	names := make(map[string]int, columns.Len())
	for index, column := range columnSlice(columns) {
		if _, duplicate := names[column.Name]; duplicate {
			return nil, typeValidation("field", "duplicate database UDT field %q", column.Name)
		}
		names[column.Name] = index
	}
	for field, location := range locations {
		if !location.positional && location.name == "" {
			return nil, typeValidation("field", "empty UDT field name")
		}
		index, found := names[location.name]
		if location.positional {
			index, found = location.index, location.index >= 0 && location.index < columns.Len()
		}
		if !found {
			if !options.allowMissing {
				return nil, typeValidation("field", "application field %q at index %d is absent from UDT", location.name, location.index)
			}
			continue
		}
		if mapping[index] != -1 {
			return nil, typeValidation("field", "multiple accessors for UDT field %q", columnSlice(columns)[index].Name)
		}
		mapping[index] = field
	}
	for index, field := range mapping {
		if field == -1 && !options.allowExtra {
			return nil, typeValidation("field", "no accessor for UDT field %q", columnSlice(columns)[index].Name)
		}
	}
	return mapping, nil
}

type nullUDTField[T any] struct{}

func (f nullUDTField[T]) location() fieldLocation                 { return fieldLocation{} }
func (f nullUDTField[T]) compile(DataType) (InputField[T], error) { return f, nil }
func (f nullUDTField[T]) encode(w *ParamWriter, _ T) error {
	column, ok := w.columns.At(w.index)
	if !ok {
		return typeValidation("field", "UDT has no field at index %d", w.index)
	}
	child := newValueWriter(w.builder, column.Type, false)
	if err := child.Null(); err != nil {
		abortValueWriter(child)
		return err
	}
	if err := finishValueWriter(child); err != nil {
		return err
	}
	w.index++
	return nil
}
func compileUDTParams[T any](codec paramsCodec[T], columns Columns, options udtOptions) (CompiledParams[T], error) {
	locations := make([]fieldLocation, len(codec.fields))
	for i, field := range codec.fields {
		if field == nil {
			return nil, typeValidation("field", "nil UDT input field")
		}
		locations[i] = field.location()
	}
	mapping, err := udtFieldMapping(columns, locations, options)
	if err != nil {
		return nil, err
	}
	plan := compiledParams[T]{ordered: make([]InputField[T], len(mapping))}
	for index, field := range mapping {
		if field == -1 {
			plan.ordered[index] = nullUDTField[T]{}
			continue
		}
		compiled, err := codec.fields[field].compile(columnSlice(columns)[index].Type)
		if err != nil {
			return nil, columnError(CodecStageCompile, index, columnSlice(columns)[index].Name, err)
		}
		plan.ordered[index] = compiled
	}
	plan.ops = paramOps(plan.ordered)
	return plan, nil
}
func compileUDTRows[T any](codec rowCodec[T], columns Columns, options udtOptions) (CompiledRow[T], error) {
	locations := make([]fieldLocation, len(codec.fields))
	for i, field := range codec.fields {
		if field == nil {
			return nil, typeValidation("field", "nil UDT output field")
		}
		locations[i] = field.location()
	}
	mapping, err := udtFieldMapping(columns, locations, options)
	if err != nil {
		return nil, err
	}
	plan := compiledRow[T]{ordered: make([]OutputField[T], len(mapping))}
	for index, field := range mapping {
		if field == -1 {
			plan.ordered[index] = ignoredField[T]{}
			continue
		}
		compiled, err := codec.fields[field].compile(columnSlice(columns)[index].Type)
		if err != nil {
			return nil, columnError(CodecStageCompile, index, columnSlice(columns)[index].Name, err)
		}
		plan.ordered[index] = compiled
	}
	plan.ops = rowOps(plan.ordered)
	// A UDT field decodes NULL, and a trailing field the value predates, as
	// its zero value; see NullAsZero.
	for i := range plan.ops {
		plan.ops[i].nullZero = true
	}
	return plan, nil
}

// Preserve UDT matching policy through the public row/parameter composition
// adapters. Arbitrary custom compilers retain their own metadata contract.
type udtParamCompiler[T any] interface {
	compileUDT(Columns, udtOptions) (CompiledParams[T], error)
}
type udtRowCompiler[T any] interface {
	compileUDT(Columns, udtOptions) (CompiledRow[T], error)
}

func compileUDTParamEncoder[T any](codec ParamEncoder[T], columns Columns, options udtOptions) (CompiledParams[T], error) {
	if codec == nil {
		return nil, typeValidation("encoder", "nil UDT parameter encoder")
	}
	if compiler, ok := codec.(udtParamCompiler[T]); ok {
		return compiler.compileUDT(columns, options)
	}
	return compileParams(codec, columns)
}
func compileUDTRowDecoder[T any](codec RowDecoder[T], columns Columns, options udtOptions) (CompiledRow[T], error) {
	if codec == nil {
		return nil, typeValidation("decoder", "nil UDT row decoder")
	}
	if compiler, ok := codec.(udtRowCompiler[T]); ok {
		return compiler.compileUDT(columns, options)
	}
	return compileRow(codec, columns)
}
func (c paramsCodec[T]) compileUDT(columns Columns, options udtOptions) (CompiledParams[T], error) {
	return compileUDTParams(c, columns, options)
}
func (c rowCodec[T]) compileUDT(columns Columns, options udtOptions) (CompiledRow[T], error) {
	return compileUDTRows(c, columns, options)
}
func (c singleColumnRows[T]) compileUDT(columns Columns, options udtOptions) (CompiledRow[T], error) {
	if columns.Len() == 1 {
		return c.Compile(columns)
	}
	codec := rowCodec[T]{fields: []OutputField[T]{ColumnAt(0, c.decoder, func(value *T) *T { return value })}}
	return compileUDTRows(codec, columns, options)
}
func (c mappedParams[A, B]) compileUDT(columns Columns, options udtOptions) (CompiledParams[A], error) {
	if c.mapValue == nil {
		return nil, typeValidation("encoder", "nil UDT parameter mapping")
	}
	plan, err := compileUDTParamEncoder(c.codec, columns, options)
	if err != nil {
		return nil, err
	}
	return compiledMappedParams[A, B]{plan: plan, mapValue: c.mapValue}, nil
}
func (c mappedRows[A, B]) compileUDT(columns Columns, options udtOptions) (CompiledRow[B], error) {
	if c.mapValue == nil || c.cloneValue == nil {
		return nil, typeValidation("decoder", "nil UDT row mapping")
	}
	plan, err := compileUDTRowDecoder(c.codec, columns, options)
	if err != nil {
		return nil, err
	}
	return compiledMappedRow[A, B]{plan: plan, mapValue: c.mapValue, cloneValue: c.cloneValue}, nil
}
func (c pairedParams[A, B]) compileUDT(columns Columns, options udtOptions) (CompiledParams[Pair[A, B]], error) {
	left, right, err := pairSplit(c.a, columns)
	if err != nil {
		return nil, err
	}
	a, err := compileUDTParamEncoder(c.a, left, options)
	if err != nil {
		return nil, err
	}
	b, err := compileUDTParamEncoder(c.b, right, options)
	if err != nil {
		return nil, rebaseColumnError(err, left.Len())
	}
	return compiledPairedParams[A, B]{left: left, right: right, a: a, b: b}, nil
}
func (c pairedRows[A, B]) compileUDT(columns Columns, options udtOptions) (CompiledRow[Pair[A, B]], error) {
	left, right, err := pairSplit(c.a, columns)
	if err != nil {
		return nil, err
	}
	a, err := compileUDTRowDecoder(c.a, left, options)
	if err != nil {
		return nil, err
	}
	b, err := compileUDTRowDecoder(c.b, right, options)
	if err != nil {
		return nil, rebaseColumnError(err, left.Len())
	}
	return compiledPairedRow[A, B]{left: left, right: right, a: a, b: b, decoders: c}, nil
}

func checkUDTIdentity(typ DataType, options udtOptions) error {
	ks, name, ok := typ.UDTName()
	if !ok {
		return typeValidation("UDT identity", "expected a UDT, got %s", typ)
	}
	if options.name != "" && name != options.name || options.keyspace != "" && ks != options.keyspace {
		want := options.name
		if want == "" {
			want = "*"
		}
		if options.keyspace != "" {
			want = options.keyspace + "." + want
		}
		return typeValidation("UDT identity", "expected %s, got %s.%s", want, ks, name)
	}
	return nil
}

// recordCodec is a tuple or UDT encoder and decoder of the same T.
type recordCodec[T any] struct {
	recordEncoder[T]
	recordDecoder[T]
}

// TupleCodec encodes a tuple's members with enc and decodes them with dec,
// as TupleEncoder and TupleDecoder do, so that one codec serves a tuple
// field of a struct mapping (see WithCodec).
func TupleCodec[T any](enc ParamEncoder[T], dec RowDecoder[T]) Codec[T] {
	return recordCodec[T]{
		recordEncoder: recordEncoder[T]{kind: KindTuple, codec: enc},
		recordDecoder: recordDecoder[T]{kind: KindTuple, codec: dec},
	}
}

// TupleCodecOf maps a tuple's members to the exported fields of struct T by
// position: the first mapped field is the first member, and so on, in
// declaration order with the fields of a struct embedded by value in its
// place. Field names and column tags do not matter, but a field's
// `codec=` and nullzero tags and `cql:"-"` apply as in RowOf. Every member
// needs a field and every field a member. TupleCodecOf applies WithCodec, for
// T's own fields, and NullAsZero, and rejects other options.
func TupleCodecOf[T any](opts ...StructOption) (Codec[T], error) {
	fields := generatedFields[T]()
	params, err := generatedParams(fields, targetTuple, opts...)
	if err != nil {
		return nil, err
	}
	rows, err := generatedRow(fields, targetTuple, opts...)
	if err != nil {
		return nil, err
	}
	p, r := params.(paramsCodec[T]), rows
	positionalParams := make([]InputField[T], len(p.fields))
	for i, f := range p.fields {
		positionalParams[i] = positionalInput[T]{InputField: f, index: i}
	}
	positionalRows := make([]OutputField[T], len(r.fields))
	for i, f := range r.fields {
		positionalRows[i] = positionalOutput[T]{OutputField: f, index: i}
	}
	return TupleCodec[T](paramsCodec[T]{fields: positionalParams}, rowCodec[T]{fields: positionalRows, reset: r.reset, nullZero: r.nullZero}), nil
}

// MustTupleCodecOf is TupleCodecOf that panics on error, for package-level
// variables.
func MustTupleCodecOf[T any](opts ...StructOption) Codec[T] {
	return must(TupleCodecOf[T](opts...))
}

// positionalInput and positionalOutput place a struct field at a tuple
// member's index; compiling them yields the field's own compiled form.
type positionalInput[P any] struct {
	InputField[P]
	index int
}

func (f positionalInput[P]) location() fieldLocation {
	return fieldLocation{index: f.index, positional: true}
}

type positionalOutput[R any] struct {
	OutputField[R]
	index int
}

func (f positionalOutput[R]) location() fieldLocation {
	return fieldLocation{index: f.index, positional: true}
}

// PairCodec maps a tuple of two members to a Pair: a maps the first and b
// the second.
func PairCodec[A, B any](a Codec[A], b Codec[B]) Codec[Pair[A, B]] {
	return TupleCodec(PairParams(OneParam[A](a), OneParam[B](b)), PairRows(OneColumn[A](a), OneColumn[B](b)))
}

func (c recordCodec[T]) CheckType(typ DataType) error {
	if err := c.recordEncoder.CheckType(typ); err != nil {
		return err
	}
	return c.recordDecoder.CheckType(typ)
}
