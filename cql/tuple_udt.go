// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"github.com/scylladb/gocql/v2/internal/syncx"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// UDTOptions names the UDT a UDTEncoder or UDTDecoder expects and explicitly
// relaxes field matching. AllowMissingFields concerns fields absent from
// metadata, not null or absent serialized field values. The zero UDTOptions
// names no UDT, which those codecs reject with a ValidationError.
type UDTOptions struct {
	Keyspace, Name           string
	AllowExtraDatabaseFields bool
	AllowMissingFields       bool
}

// UDTOptionsFor returns options naming typ's UDT, with every relaxation off.
// It fails when typ is not a UDT.
func UDTOptionsFor(typ DataType) (UDTOptions, error) {
	keyspace, name, ok := typ.UDTName()
	if !ok {
		return UDTOptions{}, typeValidation("type", "%s is not a UDT", typ)
	}
	return UDTOptions{Keyspace: keyspace, Name: name}, nil
}

type recordEncoder[T any] struct {
	codec   ParamEncoder[T]
	plan    CompiledParams[T]
	typ     DataType
	options UDTOptions
	columns Columns
	kind    Type
}

// TupleEncoder encodes its parameters as members of one physical tuple cell.
func TupleEncoder[T any](codec ParamEncoder[T]) Encoder[T] {
	return recordEncoder[T]{kind: TypeTuple, codec: codec}
}

// UDTEncoder matches fields against a complete UDT descriptor.
func UDTEncoder[T any](codec ParamEncoder[T], options UDTOptions) Encoder[T] {
	return recordEncoder[T]{kind: TypeUDT, codec: codec, options: options}
}
func recordColumns(typ DataType, kind Type) (Columns, error) {
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
	if c.kind == TypeUDT {
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
	if c.kind == TypeUDT {
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
	options UDTOptions
	columns Columns
	kind    Type
}

type recordReaderState struct {
	reader RowReader
	cursor wire.Cursor
}

// TupleDecoder consumes tuple members inside one cell without expanding result
// columns. A missing serialized suffix is passed to member decoders as null.
func TupleDecoder[T any](codec RowDecoder[T]) Decoder[T] {
	return recordDecoder[T]{kind: TypeTuple, codec: codec}
}

// UDTDecoder decodes UDT fields with valueCodec and explicit identity and matching options.
func UDTDecoder[T any](codec RowDecoder[T], options UDTOptions) Decoder[T] {
	return recordDecoder[T]{kind: TypeUDT, codec: codec, options: options}
}
func (c recordDecoder[T]) CheckType(typ DataType) error {
	perfCheckType()

	_, err := c.compileValueDecoder(typ)
	return err
}
func (c recordDecoder[T]) compileValueDecoder(typ DataType) (Decoder[T], error) {
	if c.kind == TypeUDT {
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
	if c.kind == TypeUDT {
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
			return columnError("decode", index, column.Name, &ProtocolError{Stage: "cell", Err: err})
		}
	}
	if err := cursor.RequireEnd(); err != nil {
		return &ProtocolError{Stage: "cell", Err: err}
	}
	return nil
}

// udtFieldMapping resolves source fields once while distinguishing missing
// application fields from unmatched database fields.
func udtFieldMapping(columns Columns, locations []fieldLocation, options UDTOptions) ([]int, error) {
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
			if !options.AllowMissingFields {
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
		if field == -1 && !options.AllowExtraDatabaseFields {
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
func compileUDTParams[T any](codec paramsCodec[T], columns Columns, options UDTOptions) (CompiledParams[T], error) {
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
			return nil, columnError("compile", index, columnSlice(columns)[index].Name, err)
		}
		plan.ordered[index] = compiled
	}
	plan.ops = paramOps(plan.ordered)
	return plan, nil
}
func compileUDTRows[T any](codec rowCodec[T], columns Columns, options UDTOptions) (CompiledRow[T], error) {
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
	plan := compiledRow[T]{ordered: make([]OutputField[T], len(mapping)), scratch: syncx.NewPool(func() *T { return new(T) })}
	for index, field := range mapping {
		if field == -1 {
			plan.ordered[index] = ignoredField[T]{}
			continue
		}
		compiled, err := codec.fields[field].compile(columnSlice(columns)[index].Type)
		if err != nil {
			return nil, columnError("compile", index, columnSlice(columns)[index].Name, err)
		}
		plan.ordered[index] = compiled
	}
	plan.ops = rowOps(plan.ordered)
	return plan, nil
}

// Preserve UDT matching policy through the public row/parameter composition
// adapters. Arbitrary custom compilers retain their own metadata contract.
type udtParamCompiler[T any] interface {
	compileUDT(Columns, UDTOptions) (CompiledParams[T], error)
}
type udtRowCompiler[T any] interface {
	compileUDT(Columns, UDTOptions) (CompiledRow[T], error)
}

func compileUDTParamEncoder[T any](codec ParamEncoder[T], columns Columns, options UDTOptions) (CompiledParams[T], error) {
	if codec == nil {
		return nil, typeValidation("encoder", "nil UDT parameter encoder")
	}
	if compiler, ok := codec.(udtParamCompiler[T]); ok {
		return compiler.compileUDT(columns, options)
	}
	return compileParams(codec, columns)
}
func compileUDTRowDecoder[T any](codec RowDecoder[T], columns Columns, options UDTOptions) (CompiledRow[T], error) {
	if codec == nil {
		return nil, typeValidation("decoder", "nil UDT row decoder")
	}
	if compiler, ok := codec.(udtRowCompiler[T]); ok {
		return compiler.compileUDT(columns, options)
	}
	return compileRow(codec, columns)
}
func (c paramsCodec[T]) compileUDT(columns Columns, options UDTOptions) (CompiledParams[T], error) {
	return compileUDTParams(c, columns, options)
}
func (c rowCodec[T]) compileUDT(columns Columns, options UDTOptions) (CompiledRow[T], error) {
	return compileUDTRows(c, columns, options)
}
func (c singleColumnRows[T]) compileUDT(columns Columns, options UDTOptions) (CompiledRow[T], error) {
	if columns.Len() == 1 {
		return c.Compile(columns)
	}
	codec := rowCodec[T]{fields: []OutputField[T]{ColumnAt(0, c.decoder, func(value *T) *T { return value })}}
	return compileUDTRows(codec, columns, options)
}
func (c mappedParams[A, B]) compileUDT(columns Columns, options UDTOptions) (CompiledParams[A], error) {
	if c.mapValue == nil {
		return nil, typeValidation("encoder", "nil UDT parameter mapping")
	}
	plan, err := compileUDTParamEncoder(c.codec, columns, options)
	if err != nil {
		return nil, err
	}
	return compiledMappedParams[A, B]{plan: plan, mapValue: c.mapValue}, nil
}
func (c mappedRows[A, B]) compileUDT(columns Columns, options UDTOptions) (CompiledRow[B], error) {
	if c.mapValue == nil || c.cloneValue == nil {
		return nil, typeValidation("decoder", "nil UDT row mapping")
	}
	plan, err := compileUDTRowDecoder(c.codec, columns, options)
	if err != nil {
		return nil, err
	}
	return compiledMappedRow[A, B]{plan: plan, mapValue: c.mapValue, cloneValue: c.cloneValue}, nil
}
func (c pairedParams[A, B]) compileUDT(columns Columns, options UDTOptions) (CompiledParams[Pair[A, B]], error) {
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
func (c pairedRows[A, B]) compileUDT(columns Columns, options UDTOptions) (CompiledRow[Pair[A, B]], error) {
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
	return compiledPairedRow[A, B]{left: left, right: right, a: a, b: b}, nil
}

func checkUDTIdentity(typ DataType, options UDTOptions) error {
	ks, name, ok := typ.UDTName()
	if !ok || options.Keyspace == "" || options.Name == "" {
		return typeValidation("UDT identity", "expected keyspace and name are required")
	}
	if ks != options.Keyspace || name != options.Name {
		return typeValidation("UDT identity", "expected %s.%s, got %s.%s", options.Keyspace, options.Name, ks, name)
	}
	return nil
}
