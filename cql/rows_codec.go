// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"fmt"

	"github.com/scylladb/gocql/v2/internal/syncx"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// EmptyRow is the row type of a statement without result columns.
//
// It has no fields, so its zero value is its only value.
type EmptyRow struct{}

// Pair composes two independently typed parameter or row values.
//
// Its zero value holds the zero A and B.
type Pair[A, B any] struct {
	First  A
	Second B
}

// CompiledParams encodes typed parameters in server order using ParamWriter.
type CompiledParams[P any] interface {
	Encode(*ParamWriter, P) error
}

// CompiledRow decodes checked rows into caller destinations. Clone must recursively detach
// all borrowed response storage; custom plans are cloned before One returns.
type CompiledRow[R any] interface {
	Decode(*RowReader) (R, error)
	DecodeInto(*RowReader, *R) error
	Clone(R) R
}

// ParamWriter writes parameters in server order. It is confined to one binding.
//
// The driver passes one to a parameter encoder; a zero ParamWriter rejects
// writes with a ValidationError.
type ParamWriter struct {
	builder     *wire.Builder
	columns     Columns
	capture     []paramCell
	cell        ValueWriter // reused for each parameter; never retained by encoders
	index       int
	allowUnset  bool
	ownBorrowed bool
}

// ParamCell records framing while encoding, avoiding a second parse of the
// request. Offsets refer to the final contiguous owned parameter buffer.
type paramCell struct {
	Start, End int64
	State      CellState
}

// CaptureParamCells enables framing capture into exactly one slot per column.
func captureParamCells(w *ParamWriter, cells []paramCell) { w.capture = cells }

func newParamWriter(builder *wire.Builder, columns Columns, allowUnset bool) *ParamWriter {
	return &ParamWriter{builder: builder, columns: columns, allowUnset: allowUnset}
}

// ResetParamWriter prepares caller-owned writer storage for a new binding, so a
// reusable binding allocates no writer.
func resetParamWriter(w *ParamWriter, builder *wire.Builder, columns Columns, allowUnset bool) {
	*w = ParamWriter{builder: builder, columns: columns, allowUnset: allowUnset}
}

func finishParamWriter(w *ParamWriter) error {
	if w.index != w.columns.Len() {
		return fmt.Errorf("gocql: encoded %d parameters, expected %d", w.index, w.columns.Len())
	}
	return nil
}

// WriteParam encodes one typed parameter with transactional cell framing.
func (w *ParamWriter) WriteParam[T any](encoder Encoder[T], value T) error {
	if w == nil || w.builder == nil || encoder == nil {
		return typeValidation("writer", "nil or zero parameter writer, or nil encoder")
	}
	column, ok := w.columns.At(w.index)
	if !ok {
		return fmt.Errorf("gocql: too many parameters: index %d", w.index)
	}
	// The cell lives in the writer: passing it to an interface encoder would
	// otherwise move a fresh ValueWriter to the heap for every parameter.
	cell := &w.cell
	initValueWriter(cell, w.builder, column.Type, w.allowUnset)
	cell.ownBorrowed = w.ownBorrowed
	if err := encoder.Encode(cell, value); err != nil {
		abortValueWriter(cell)
		return fmt.Errorf("gocql: parameter %d (%s): %w", w.index, column.Name, err)
	}
	if err := finishValueWriter(cell); err != nil {
		abortValueWriter(cell)
		return fmt.Errorf("gocql: parameter %d (%s): %w", w.index, column.Name, err)
	}
	if w.capture != nil {
		if w.index >= len(w.capture) {
			return fmt.Errorf("gocql: missing parameter capture slot")
		}
		state := cell.state
		if state == EmptyCell {
			state = Present
		} // empty has a zero-length present wire body
		w.capture[w.index] = paramCell{Start: cell.start + 4, End: w.builder.Len(), State: state}
	}
	w.index++
	return nil
}

// RowReader consumes physical columns without tuple expansion or reflection.
//
// The driver passes one to a row decoder; a zero RowReader rejects reads with a
// ValidationError.
type RowReader struct {
	cursor       *wire.Cursor
	lease        wire.Ref
	columns      Columns
	index        int
	allowMissing bool
}

func newRowReader(columns Columns, body wire.Chain, lease *wire.Lease) *RowReader {
	return &RowReader{columns: columns, cursor: wire.NewCursor(body), lease: wire.RefOf(lease)}
}

func finishRowReader(r *RowReader) error {
	if r.index != r.columns.Len() {
		return fmt.Errorf("gocql: decoded %d columns, expected %d", r.index, r.columns.Len())
	}
	return nil
}

func (r *RowReader) nextValue() (ValueReader, error) {
	if r == nil || r.cursor == nil {
		return ValueReader{}, typeValidation("reader", "nil or zero row reader")
	}
	column, ok := r.columns.At(r.index)
	if !ok {
		return ValueReader{}, fmt.Errorf("gocql: too many columns: index %d", r.index)
	}
	if r.allowMissing && r.cursor.Remaining() == 0 {
		r.index++
		return newValueReader(column.Type, Null, wire.Chain{}, r.lease), nil
	}
	v, err := readFramedValue(r.cursor, column.Type, r.lease, false, false)
	if err != nil {
		return ValueReader{}, fmt.Errorf("gocql: column %d (%s): %w", r.index, column.Name, err)
	}
	r.index++
	return v, nil
}

// ReadColumn decodes the next column with decoder.
func (r *RowReader) ReadColumn[T any](decoder Decoder[T]) (T, error) {
	var zero T
	if decoder == nil {
		return zero, fmt.Errorf("gocql: nil column decoder")
	}
	value, err := r.nextValue()
	if err != nil {
		return zero, err
	}
	result, err := decoder.Decode(value)
	if err != nil {
		column, _ := r.columns.At(r.index - 1)
		return zero, fmt.Errorf("gocql: column %d (%s): %w", r.index-1, column.Name, err)
	}
	return result, nil
}

func (r *RowReader) SkipColumn() error {
	_, err := r.nextValue()
	return err
}

type fieldLocation struct {
	name       string
	index      int
	positional bool
}

// InputField is constructed with Param or ParamAt; it never boxes a field value.
type InputField[P any] interface {
	location() fieldLocation
	compile(DataType) (InputField[P], error)
	encode(*ParamWriter, P) error
}

type inputField[P, V any] struct {
	codec Encoder[V]
	get   func(P) V
	loc   fieldLocation
}

// Param maps a named bind marker through valueCodec from the value returned by get.
func Param[P, V any](name string, codec Encoder[V], get func(P) V) InputField[P] {
	return inputField[P, V]{loc: fieldLocation{name: name}, codec: codec, get: get}
}

// ParamAt maps a zero-based bind marker through valueCodec from get.
func ParamAt[P, V any](index int, codec Encoder[V], get func(P) V) InputField[P] {
	return inputField[P, V]{loc: fieldLocation{index: index, positional: true}, codec: codec, get: get}
}

func (f inputField[P, V]) location() fieldLocation { return f.loc }
func (f inputField[P, V]) compile(typ DataType) (InputField[P], error) {
	if f.codec == nil || f.get == nil {
		return nil, fmt.Errorf("gocql: nil input codec or accessor")
	}
	codec, err := compileValueEncoder(f.codec, typ)
	if err != nil {
		return nil, err
	}
	f.codec = codec
	return f, nil
}
func (f inputField[P, V]) encode(w *ParamWriter, value P) error {
	return w.WriteParam(f.codec, f.get(value))
}

type paramsCodec[P any] struct {
	fields   []InputField[P]
	optional bool // fields without a column are skipped (struct mappings)
}
type compiledParams[P any] struct {
	ordered []InputField[P]
	ops     []paramOp[P] // ordered, with built-in codecs as direct ops
}

// Params builds a typed parameter encoder from field mappings. Every bind marker must be mapped.
func Params[P any](fields ...InputField[P]) ParamEncoder[P] {
	return paramsCodec[P]{fields: append([]InputField[P](nil), fields...)}
}

// mapFields resolves names/positions once. Parameters may repeat one named input;
// result columns require an explicit positional accessor for duplicate aliases.
// With optional, a field without a column is skipped instead of rejected, so
// one struct mapping serves statements that use a subset of its fields; every
// column still needs a field.
func mapFields(columns Columns, locations []fieldLocation, repeatedNames, optional bool) ([]int, error) {
	return mapColumnFields(columns, locations, repeatedNames, optional, false)
}

// mapColumnFields is shared by buffered and streaming row/record compilation.
// Missing application fields and extra database columns are independent policies.
func mapColumnFields(columns Columns, locations []fieldLocation, repeatedNames, optional, extra bool) ([]int, error) {
	perfNameMap()
	ordered := make([]int, columns.Len())
	for i := range ordered {
		ordered[i] = -1
	}
	for i, location := range locations {
		matches := 0
		if !location.positional && location.name == "" {
			return nil, fmt.Errorf("gocql: empty field name")
		}
		for j := 0; j < columns.Len(); j++ {
			column, _ := columns.At(j)
			match := location.positional && location.index == j || !location.positional && location.name == column.Name
			if !match {
				continue
			}
			if ordered[j] != -1 {
				return nil, fmt.Errorf("gocql: multiple accessors for column %d (%s)", j, column.Name)
			}
			ordered[j] = i
			matches++
		}
		if matches == 0 {
			if optional {
				continue
			}
			return nil, fmt.Errorf("gocql: field %q at index %d has no database column", location.name, location.index)
		}
		if matches > 1 && !repeatedNames {
			return nil, fmt.Errorf("gocql: duplicate result name %q requires positional mapping", location.name)
		}
	}
	for i, field := range ordered {
		if field < 0 && !extra {
			column, _ := columns.At(i)
			return nil, fmt.Errorf("gocql: no accessor for column %d (%s)", i, column.Name)
		}
	}
	return ordered, nil
}

func (c paramsCodec[P]) Compile(columns Columns) (CompiledParams[P], error) {
	locations := make([]fieldLocation, len(c.fields))
	for i, field := range c.fields {
		if field == nil {
			return nil, fmt.Errorf("gocql: nil parameter field at index %d", i)
		}
		locations[i] = field.location()
	}
	mapping, err := mapFields(columns, locations, true, c.optional)
	if err != nil {
		return nil, err
	}
	plan := compiledParams[P]{ordered: make([]InputField[P], len(mapping))}
	for index, field := range mapping {
		column, _ := columns.At(index)
		compiled, err := c.fields[field].compile(column.Type)
		if err != nil {
			return nil, fmt.Errorf("gocql: parameter %d (%s): %w", index, column.Name, err)
		}
		plan.ordered[index] = compiled
	}
	plan.ops = paramOps(plan.ordered)
	return plan, nil
}

func (p compiledParams[P]) Encode(w *ParamWriter, value P) error {
	mark := w.builder.Mark()
	start := w.index
	for i := range p.ops {
		op := &p.ops[i]
		var err error
		if op.kind == directNone {
			err = op.field.encode(w, value)
		} else {
			err = op.encodeDirect(w, value)
		}
		if err != nil {
			w.builder.Rollback(mark)
			w.index = start
			return err
		}
	}
	return nil
}

// OutputField contains a typed field address accessor used for decoding and Clone.
type OutputField[R any] interface {
	location() fieldLocation
	compile(DataType) (OutputField[R], error)
	decode(*RowReader, *R) error
	clone(dst, source *R)
	detachField(*R)
}

type outputField[R, V any] struct {
	codec Decoder[V]
	field func(*R) *V
	loc   fieldLocation
}

// Column maps the named result column through valueCodec into the address returned by field.
// The accessor must return a non-nil address within the destination and must not retain it.
func Column[R, V any](name string, codec Decoder[V], field func(*R) *V) OutputField[R] {
	return outputField[R, V]{loc: fieldLocation{name: name}, codec: codec, field: field}
}

// ColumnAt maps the zero-based result column through valueCodec into field.
func ColumnAt[R, V any](index int, codec Decoder[V], field func(*R) *V) OutputField[R] {
	return outputField[R, V]{loc: fieldLocation{index: index, positional: true}, codec: codec, field: field}
}
func (f outputField[R, V]) location() fieldLocation { return f.loc }
func (f outputField[R, V]) compile(typ DataType) (OutputField[R], error) {
	if f.codec == nil || f.field == nil {
		return nil, fmt.Errorf("gocql: nil output codec or accessor")
	}
	codec, err := compileValueDecoder(f.codec, typ)
	if err != nil {
		return nil, err
	}
	f.codec = codec
	return f, nil
}
func (f outputField[R, V]) decode(reader *RowReader, row *R) error {
	field := f.field(row)
	if field == nil {
		return fmt.Errorf("gocql: output field accessor returned nil")
	}
	return readColumnInto(reader, f.codec, field)
}
func (f outputField[R, V]) clone(dst, source *R) {
	*f.field(dst) = f.codec.Clone(*f.field(source))
}

type ignoredField[R any] struct{ loc fieldLocation }

// IgnoreColumn consumes the named result column without storing it.
func IgnoreColumn[R any](name string) OutputField[R] {
	return ignoredField[R]{loc: fieldLocation{name: name}}
}
func (f ignoredField[R]) location() fieldLocation                  { return f.loc }
func (f ignoredField[R]) compile(DataType) (OutputField[R], error) { return f, nil }
func (f ignoredField[R]) decode(r *RowReader, _ *R) error          { return r.SkipColumn() }
func (f ignoredField[R]) clone(dst, source *R)                     {}

type rowCodec[R any] struct {
	reset    func(*R)
	fields   []OutputField[R]
	optional bool // fields without a column are skipped (struct mappings)
}
type compiledRow[R any] struct {
	reset func(*R)
	// Scratch addresses are confined to Decode; accessors must not retain them.
	// The returned R is copied before scratch is cleared and recycled.
	scratch *syncx.Pool[*R]
	ordered []OutputField[R]
	ops     []rowOp[R] // ordered, with built-in codecs as direct ops
}

// Row builds a row decoder from field mappings. Borrowed fields follow the execution lifetime;
// One detaches them, Each lends them during its callback, and cursors lend them until advancement.
func Row[R any](fields ...OutputField[R]) RowDecoder[R] {
	return rowCodec[R]{fields: append([]OutputField[R](nil), fields...)}
}

// RowInto maps columns into the actual destination after calling reset. The
// callback clears application state and may preserve owned slices for reuse.
// Borrowed fields follow the execution's normal lifetime; One detaches them.
func RowInto[R any](reset func(*R), fields ...OutputField[R]) RowDecoder[R] {
	return rowCodec[R]{fields: append([]OutputField[R](nil), fields...), reset: reset}
}
func (p compiledRow[R]) preserveDestination() bool { return p.reset != nil }

// rowDestinationManager marks a built-in row plan whose DecodeInto zeroes, or
// resets, the whole destination before it decodes, so the caller need not
// clear the destination before each row. The method is unexported: an
// application plan cannot claim it and is always handed a cleared row.
type rowDestinationManager interface{ managesRowDestination() }

// ManagedRowDestination gives a built-in row plan outside this package the
// rowDestinationManager marker when embedded. Embed it only in a plan whose
// DecodeInto overwrites the whole destination.
type managedRowDestination struct{}

func (managedRowDestination) managesRowDestination() {}

func (compiledRow[R]) managesRowDestination()          {}
func (singleColumnPlan[T]) managesRowDestination()     {}
func (compiledMappedRow[A, B]) managesRowDestination() {}
func (compiledPairedRow[A, B]) managesRowDestination() {}
func (compiledCASRow[R]) managesRowDestination()       {}
func (dynamicRowsPlan) managesRowDestination()         {}
func (c rowCodec[R]) Clone(source R) R {
	dst := source
	for _, field := range c.fields {
		// Compile rejects a nil field, so such a decoder never produced
		// source; there is nothing of the field's to detach.
		if field == nil {
			continue
		}
		field.clone(&dst, &source)
	}
	return dst
}
func (c rowCodec[R]) Compile(columns Columns) (CompiledRow[R], error) {
	return c.compile(columns, c.optional, false)
}

// compileLenient compiles the previous-row plan of a conditional result,
// whose columns differ by server and outcome: a field without a column is
// left at its zero value and a column without a field is skipped.
func (c rowCodec[R]) compileLenient(columns Columns) (CompiledRow[R], error) {
	return c.compile(columns, true, true)
}

func (c rowCodec[R]) compile(columns Columns, optional, extra bool) (CompiledRow[R], error) {
	locations := make([]fieldLocation, len(c.fields))
	for i, field := range c.fields {
		if field == nil {
			return nil, fmt.Errorf("gocql: nil result field at index %d", i)
		}
		locations[i] = field.location()
	}
	mapping, err := mapColumnFields(columns, locations, false, optional, extra)
	if err != nil {
		return nil, err
	}
	plan := compiledRow[R]{reset: c.reset, ordered: make([]OutputField[R], len(mapping)), scratch: syncx.NewPool(func() *R { return new(R) })}
	for index, field := range mapping {
		column, _ := columns.At(index)
		if field < 0 {
			plan.ordered[index] = ignoredField[R]{loc: fieldLocation{name: column.Name}}
			continue
		}
		compiled, err := c.fields[field].compile(column.Type)
		if err != nil {
			return nil, fmt.Errorf("gocql: result column %d (%s): %w", index, column.Name, err)
		}
		plan.ordered[index] = compiled
	}
	plan.ops = rowOps(plan.ordered)
	return plan, nil
}
func (p compiledRow[R]) Decode(reader *RowReader) (R, error) {
	row := p.scratch.Get()
	err := p.DecodeInto(reader, row)
	result := *row
	var zero R
	*row = zero
	p.scratch.Put(row)
	if err != nil {
		return zero, err
	}
	return result, nil
}
func (p compiledRow[R]) DecodeInto(reader *RowReader, dst *R) error {
	if dst == nil {
		return typeValidation("destination", "nil row destination")
	}
	if p.reset != nil {
		p.reset(dst)
	} else {
		var zero R
		*dst = zero
	}
	for i := range p.ops {
		op := &p.ops[i]
		var err error
		if op.kind == directNone {
			err = op.field.decode(reader, dst)
		} else {
			err = op.decodeDirect(reader, dst)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
func (p compiledRow[R]) Clone(source R) R {
	dst := source
	for _, field := range p.ordered {
		field.clone(&dst, &source)
	}
	return dst
}

// OneParam encodes one positional bind marker using encoder.
func OneParam[T any](encoder Encoder[T]) ParamEncoder[T] {
	return Params(ParamAt(0, encoder, func(value T) T { return value }))
}

// OneColumn decodes exactly one result column using decoder.
func OneColumn[T any](decoder Decoder[T]) RowDecoder[T] {
	return singleColumnRows[T]{decoder: decoder}
}

type singleColumnRows[T any] struct{ decoder Decoder[T] }
type singleColumnPlan[T any] struct{ decoder Decoder[T] }

func (c singleColumnRows[T]) Compile(columns Columns) (CompiledRow[T], error) {
	if columns.Len() != 1 {
		return nil, fmt.Errorf("gocql: single column decoder expected 1 column, got %d", columns.Len())
	}
	column, _ := columns.At(0)
	decoder, err := compileValueDecoder(c.decoder, column.Type)
	if err != nil {
		return nil, err
	}
	return singleColumnPlan[T]{decoder: decoder}, nil
}
func (c singleColumnRows[T]) Clone(value T) T { return c.decoder.Clone(value) }
func (p singleColumnPlan[T]) Decode(reader *RowReader) (T, error) {
	return reader.ReadColumn(p.decoder)
}
func (p singleColumnPlan[T]) DecodeInto(reader *RowReader, dst *T) error {
	if dst == nil {
		return typeValidation("destination", "nil row destination")
	}
	var zero T
	*dst = zero
	return readColumnInto(reader, p.decoder, dst)
}
func (p singleColumnPlan[T]) Clone(value T) T { return p.decoder.Clone(value) }

var EmptyParamsEncoder ParamEncoder[EmptyRow] = Params[EmptyRow]()
var EmptyRowDecoder RowDecoder[EmptyRow] = Row[EmptyRow]()

type mappedParams[A, B any] struct {
	codec    ParamEncoder[B]
	mapValue func(A) B
}
type compiledMappedParams[A, B any] struct {
	plan     CompiledParams[B]
	mapValue func(A) B
}

// ConvertParams adapts an encoder for B to input A by converting each A to a B
// with convert before encoding it.
func ConvertParams[A, B any](codec ParamEncoder[B], mapValue func(A) B) ParamEncoder[A] {
	return mappedParams[A, B]{codec: codec, mapValue: mapValue}
}
func (c mappedParams[A, B]) Compile(columns Columns) (CompiledParams[A], error) {
	if c.codec == nil || c.mapValue == nil {
		return nil, fmt.Errorf("gocql: nil parameter mapping")
	}
	plan, err := compileParams(c.codec, columns)
	if err != nil {
		return nil, err
	}
	return compiledMappedParams[A, B]{plan: plan, mapValue: c.mapValue}, nil
}
func (p compiledMappedParams[A, B]) Encode(w *ParamWriter, value A) error {
	return p.plan.Encode(w, p.mapValue(value))
}

type mappedRows[A, B any] struct {
	codec      RowDecoder[A]
	mapValue   func(A) B
	cloneValue func(B) B
}
type compiledMappedRow[A, B any] struct {
	plan       CompiledRow[A]
	mapValue   func(A) B
	cloneValue func(B) B
}

// ConvertRows decodes A rows with valueCodec and converts each to a B with
// convert; cloneValue must recursively detach borrowed data in B.
func ConvertRows[A, B any](codec RowDecoder[A], mapValue func(A) B, cloneValue func(B) B) RowDecoder[B] {
	return mappedRows[A, B]{codec: codec, mapValue: mapValue, cloneValue: cloneValue}
}
func (c mappedRows[A, B]) Clone(value B) B {
	// Compile rejects a nil cloneValue, so such a decoder never produced
	// value; it is returned as it is.
	if c.cloneValue == nil {
		return value
	}
	return c.cloneValue(value)
}
func (c mappedRows[A, B]) Compile(columns Columns) (CompiledRow[B], error) {
	if c.codec == nil || c.mapValue == nil || c.cloneValue == nil {
		return nil, fmt.Errorf("gocql: nil row mapping")
	}
	plan, err := compileRow(c.codec, columns)
	if err != nil {
		return nil, err
	}
	return compiledMappedRow[A, B]{plan: plan, mapValue: c.mapValue, cloneValue: c.cloneValue}, nil
}
func (p compiledMappedRow[A, B]) Decode(reader *RowReader) (B, error) {
	v, err := p.plan.Decode(reader)
	if err != nil {
		var zero B
		return zero, err
	}
	return p.mapValue(v), nil
}
func (p compiledMappedRow[A, B]) DecodeInto(reader *RowReader, dst *B) error {
	if dst == nil {
		return typeValidation("destination", "nil row destination")
	}
	var zero B
	*dst = zero
	value, err := p.plan.Decode(reader)
	if err != nil {
		return err
	}
	*dst = p.mapValue(value) // Explicit value-returning application mapping.
	return nil
}
func (p compiledMappedRow[A, B]) Clone(value B) B { return p.cloneValue(value) }

func splitColumns(columns Columns, split int) (Columns, Columns, error) {
	if split < 0 || split > columns.Len() {
		return Columns{}, Columns{}, fmt.Errorf("gocql: invalid column split %d/%d", split, columns.Len())
	}
	left, right := make([]ColumnSpec, split), make([]ColumnSpec, columns.Len()-split)
	for i := range left {
		left[i], _ = columns.At(i)
	}
	for i := range right {
		right[i], _ = columns.At(split + i)
	}
	return NewColumns(left...), NewColumns(right...), nil
}

// fixedArity reports how many columns codec, a ParamEncoder or RowDecoder,
// covers whatever the metadata: 1 for OneParam and OneColumn, the highest
// index plus one for Params, Row and RowInto built only from ParamAt or
// ColumnAt, and the sum of both sides for a pair. Any other codec, such as a
// named field or a struct mapping, has none.
func fixedArity(codec any) (int, bool) {
	c, ok := codec.(interface{ fixedArity() (int, bool) })
	if !ok {
		return 0, false
	}
	return c.fixedArity()
}

func positionalArity[F interface{ location() fieldLocation }](fields []F) (int, bool) {
	n := 0
	for _, field := range fields {
		loc := field.location()
		if !loc.positional || loc.index < 0 {
			return 0, false
		}
		n = max(n, loc.index+1)
	}
	return n, true
}

func (c paramsCodec[P]) fixedArity() (int, bool) {
	if c.optional {
		return 0, false
	}
	return positionalArity(c.fields)
}
func (c rowCodec[R]) fixedArity() (int, bool)        { return positionalArity(c.fields) }
func (singleColumnRows[T]) fixedArity() (int, bool)  { return 1, true }
func (c pairedParams[A, B]) fixedArity() (int, bool) { return pairArity(c.a, c.b) }
func (c pairedRows[A, B]) fixedArity() (int, bool)   { return pairArity(c.a, c.b) }

func pairArity(a, b any) (int, bool) {
	left, ok := fixedArity(a)
	if !ok {
		return 0, false
	}
	right, ok := fixedArity(b)
	return left + right, ok
}

// pairSplit returns the columns of a pair's left side, a, and its right side.
func pairSplit(a any, columns Columns) (Columns, Columns, error) {
	split, ok := fixedArity(a)
	if !ok {
		return Columns{}, Columns{}, fmt.Errorf("gocql: pair left side has no fixed arity; use OneParam, OneColumn, ParamAt or ColumnAt fields, or a pair of them")
	}
	return splitColumns(columns, split)
}

type pairedParams[A, B any] struct {
	a ParamEncoder[A]
	b ParamEncoder[B]
}
type compiledPairedParams[A, B any] struct {
	a           CompiledParams[A]
	b           CompiledParams[B]
	left, right Columns
}

// PairParams combines two encoders: a binds the leading parameters and b the
// rest. a must cover a fixed number of parameters: OneParam covers one, Params
// built only from ParamAt fields the highest index plus one, and a PairParams
// the sum of its sides. Compile rejects any other a, such as named fields or
// ParamsOf.
func PairParams[A, B any](a ParamEncoder[A], b ParamEncoder[B]) ParamEncoder[Pair[A, B]] {
	return pairedParams[A, B]{a: a, b: b}
}
func (c pairedParams[A, B]) Compile(columns Columns) (CompiledParams[Pair[A, B]], error) {
	if c.a == nil || c.b == nil {
		return nil, fmt.Errorf("gocql: nil paired parameter codec")
	}
	left, right, err := pairSplit(c.a, columns)
	if err != nil {
		return nil, err
	}
	a, err := compileParams(c.a, left)
	if err != nil {
		return nil, err
	}
	b, err := compileParams(c.b, right)
	if err != nil {
		return nil, err
	}
	return compiledPairedParams[A, B]{left: left, right: right, a: a, b: b}, nil
}
func (p compiledPairedParams[A, B]) Encode(w *ParamWriter, value Pair[A, B]) error {
	mark := w.builder.Mark()
	columns, index := w.columns, w.index
	completed := false
	defer func() {
		w.columns, w.index = columns, index
		if completed {
			w.index += p.left.Len() + p.right.Len()
		}
	}()
	w.columns, w.index = p.left, 0
	err := p.a.Encode(w, value.First)
	if err == nil {
		err = finishParamWriter(w)
	}
	w.columns, w.index = p.right, 0
	if err == nil {
		err = p.b.Encode(w, value.Second)
	}
	if err == nil {
		err = finishParamWriter(w)
	}
	if err != nil {
		w.builder.Rollback(mark)
		return err
	}
	completed = true
	return nil
}

type pairedRows[A, B any] struct {
	a RowDecoder[A]
	b RowDecoder[B]
}
type compiledPairedRow[A, B any] struct {
	a           CompiledRow[A]
	b           CompiledRow[B]
	left, right Columns
}

// PairRows combines two decoders: a decodes the leading columns and b the
// rest. a must cover a fixed number of columns: OneColumn covers one, Row or
// RowInto built only from ColumnAt fields the highest index plus one, and a
// PairRows the sum of its sides. Compile rejects any other a, such as named
// fields or RowOf.
func PairRows[A, B any](a RowDecoder[A], b RowDecoder[B]) RowDecoder[Pair[A, B]] {
	return pairedRows[A, B]{a: a, b: b}
}
func (c pairedRows[A, B]) Clone(value Pair[A, B]) Pair[A, B] {
	return Pair[A, B]{First: c.a.Clone(value.First), Second: c.b.Clone(value.Second)}
}
func (c pairedRows[A, B]) Compile(columns Columns) (CompiledRow[Pair[A, B]], error) {
	if c.a == nil || c.b == nil {
		return nil, fmt.Errorf("gocql: nil paired row codec")
	}
	left, right, err := pairSplit(c.a, columns)
	if err != nil {
		return nil, err
	}
	a, err := compileRow(c.a, left)
	if err != nil {
		return nil, err
	}
	b, err := compileRow(c.b, right)
	if err != nil {
		return nil, err
	}
	return compiledPairedRow[A, B]{left: left, right: right, a: a, b: b}, nil
}
func (p compiledPairedRow[A, B]) Decode(r *RowReader) (Pair[A, B], error) {
	var result Pair[A, B]
	err := p.DecodeInto(r, &result)
	if err != nil {
		return Pair[A, B]{}, err
	}
	return result, nil
}
func (p compiledPairedRow[A, B]) DecodeInto(r *RowReader, dst *Pair[A, B]) error {
	if dst == nil {
		return typeValidation("destination", "nil row destination")
	}
	*dst = Pair[A, B]{}
	columns, index := r.columns, r.index
	completed := false
	defer func() {
		r.columns, r.index = columns, index
		if completed {
			r.index += p.left.Len() + p.right.Len()
		}
	}()
	r.columns, r.index = p.left, 0
	err := p.a.DecodeInto(r, &dst.First)
	if err == nil {
		err = finishRowReader(r)
	}
	if err != nil {
		return err
	}
	r.columns, r.index = p.right, 0
	err = p.b.DecodeInto(r, &dst.Second)
	if err == nil {
		err = finishRowReader(r)
	}
	if err != nil {
		return err
	}
	completed = true
	return nil
}
func (p compiledPairedRow[A, B]) Clone(value Pair[A, B]) Pair[A, B] {
	return Pair[A, B]{First: p.a.Clone(value.First), Second: p.b.Clone(value.Second)}
}

// Composite codecs can bind nested field plans once with the enclosing plan.
// The returned codec must be immutable and safe for concurrent use.
type compiledValueDecoder[T any] interface {
	compileValueDecoder(DataType) (Decoder[T], error)
}
type compiledValueEncoder[T any] interface {
	compileValueEncoder(DataType) (Encoder[T], error)
}

func compileValueDecoder[T any](decoder Decoder[T], typ DataType) (Decoder[T], error) {
	perfCompile()
	if decoder == nil {
		return nil, fmt.Errorf("gocql: nil value decoder")
	}
	if compiled, ok := decoder.(compiledValueDecoder[T]); ok {
		value, err := compiled.compileValueDecoder(typ)
		if err == nil && value == nil {
			err = typeValidation("decoder", "Compile returned a nil value decoder")
		}
		return value, err
	}
	if err := decoder.CheckType(typ); err != nil {
		return nil, err
	}
	return decoder, nil
}
func compileValueEncoder[T any](encoder Encoder[T], typ DataType) (Encoder[T], error) {
	perfCompile()
	if encoder == nil {
		return nil, fmt.Errorf("gocql: nil value encoder")
	}
	if compiled, ok := encoder.(compiledValueEncoder[T]); ok {
		value, err := compiled.compileValueEncoder(typ)
		if err == nil && value == nil {
			err = typeValidation("encoder", "Compile returned a nil value encoder")
		}
		return value, err
	}
	if err := encoder.CheckType(typ); err != nil {
		return nil, err
	}
	return encoder, nil
}

// Aggregate codecs propagate an actual field address; scalar decoders return
// only their scalar value. This is deliberately not an entire-row adapter.
type valueIntoDecoder[T any] interface{ DecodeInto(ValueReader, *T) error }

func decodeValueInto[T any](decoder Decoder[T], reader ValueReader, dst *T) error {
	if dst == nil {
		return typeValidation("destination", "nil value destination")
	}
	if direct, ok := decoder.(valueIntoDecoder[T]); ok {
		return direct.DecodeInto(reader, dst)
	}
	value, err := decoder.Decode(reader)
	if err == nil {
		*dst = value
	}
	return err
}
func readColumnInto[T any](r *RowReader, decoder Decoder[T], dst *T) error {
	if decoder == nil {
		return typeValidation("decoder", "nil column decoder")
	}
	if direct, ok := decoder.(rowValueIntoDecoder[T]); ok && dst != nil {
		return direct.decodeColumnInto(r, dst)
	}
	value, err := r.nextValue()
	if err == nil {
		err = decodeValueInto(decoder, value, dst)
	}
	return err
}

func compileRow[R any](decoder RowDecoder[R], columns Columns) (CompiledRow[R], error) {
	if decoder == nil {
		return nil, typeValidation("decoder", "nil row decoder")
	}
	plan, err := decoder.Compile(columns)
	if err == nil && plan == nil {
		err = typeValidation("decoder", "Compile returned a nil row plan")
	}
	return plan, err
}
func compileParams[P any](encoder ParamEncoder[P], columns Columns) (CompiledParams[P], error) {
	if encoder == nil {
		return nil, typeValidation("encoder", "nil parameter encoder")
	}
	plan, err := encoder.Compile(columns)
	if err == nil && plan == nil {
		err = typeValidation("encoder", "Compile returned a nil parameter plan")
	}
	return plan, err
}
