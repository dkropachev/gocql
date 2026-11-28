// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/structmap"
)

// Struct mappings build the same Param and Column fields an application writes
// by hand, from a struct's exported fields and `cql:"name,codec=X"` tags.
// Reflection runs once, when the mapping is built; the field accessors read at
// a fixed offset, so encoding and decoding stay reflection-free and keep the
// built-in direct ops. cmd/cqlgen generates the same fields from source, using
// the tag grammar and the derivation rules of derive.go, as a CQLFields method
// of the struct.

// builtinFieldTypes returns the built-in leaf types, which struct mappings
// derive every other field type from.
func builtinFieldTypes() []structmap.BuiltinFieldType {
	leaves := leafTypes()
	types := make([]structmap.BuiltinFieldType, len(leaves))
	for i, l := range leaves {
		types[i] = l.BuiltinFieldType
	}
	return types
}

// StructOption configures a struct mapping: ParamsOf, RowOf, StreamRowOf,
// UDTCodecOf or TupleCodecOf, or the UDT a UDTEncoder, UDTDecoder or
// StreamUDT expects. Each option applies to some of them, and the others
// reject it with a ValidationError rather than ignore it:
//
//   - WithCodec: ParamsOf, RowOf, UDTCodecOf and TupleCodecOf.
//   - WithEncoder and NilAsUnset: ParamsOf.
//   - ReuseBuffers: RowOf.
//   - NullAsZero: RowOf, StreamRowOf, UDTCodecOf and TupleCodecOf.
//   - IgnoreExtraFields: RowOf, StreamRowOf, UDTCodecOf and the UDT
//     constructors.
//   - UDTName and AllowMissingFields: UDTCodecOf and the UDT constructors.
//   - WithStreamDecoder: StreamRowOf.
//
// Each call builds an independent mapping; options never change a generated
// mapping in place.
type StructOption interface {
	// option names the option and the mappings it applies to.
	option() (string, structTarget)
	apply(*structOptionSet)
}

// structTarget is a set of struct mapping constructors.
type structTarget uint8

const (
	targetParams structTarget = 1 << iota
	targetRows
	targetStream
	targetUDT
	targetTuple
	targetUDTValue // UDTEncoder, UDTDecoder and StreamUDT
)

func (t structTarget) String() string {
	switch t {
	case targetParams:
		return "ParamsOf"
	case targetRows:
		return "RowOf"
	case targetStream:
		return "StreamRowOf"
	case targetTuple:
		return "TupleCodecOf"
	case targetUDTValue:
		return "UDTEncoder, UDTDecoder or StreamUDT"
	default:
		return "UDTCodecOf"
	}
}

// structOptionSet is the options of one mapping, before they are bound to
// its struct type.
type structOptionSet struct {
	codecs       map[reflect.Type]derived
	streams      map[reflect.Type]erasedStreamEntry
	udt          udtOptions
	reuseBuffers bool
	nullZero     bool
	nilUnset     bool
}

type structConfig[P any] struct {
	codecs       map[reflect.Type]derived
	streams      map[reflect.Type]streamBuiltinField[P]
	udt          udtOptions
	reuseBuffers bool
	nullZero     bool
	nilUnset     bool
}

// Built-in field types and options name a field type V but not the struct,
// and building their accessors for each struct would instantiate every
// field type's code once per mapped struct. They build them for erasedBase
// instead, a stand-in whose pointer is the struct's address, and
// erasedParam, erasedColumn and erasedStreamColumn adapt them to the struct.
//
// unsafe: an erased accessor adds the field's reflect offset to the
// struct's address; the struct is P or R, whose field at that offset has
// type exactly V, or a type with V's layout that names a type where V has
// the type's underlying type (deriveField, sameLayout). eachStructField
// sums the offsets of flattened value embeds and rejects pointer embeds, so
// the offset stays inside the struct and the result is a typed pointer the
// GC sees.
type erasedBase struct{}

// erasedEntry builds the fields of one Go type for an option: param binds a
// parameter, and column, nil for an encoder-only option, decodes a column.
type erasedEntry struct {
	param  func(name string, offset uintptr) InputField[*erasedBase]
	column func(name string, offset uintptr) OutputField[erasedBase]
}

type erasedStreamEntry struct {
	column func(name string, offset uintptr) StreamOutputField[erasedBase]
}

func erasedParamOf[V any](enc Encoder[V]) func(string, uintptr) InputField[*erasedBase] {
	return func(name string, offset uintptr) InputField[*erasedBase] {
		return Param(name, enc, func(base *erasedBase) V { return *(*V)(unsafe.Add(unsafe.Pointer(base), offset)) })
	}
}

func erasedColumnOf[V any](dec Decoder[V]) func(string, uintptr) OutputField[erasedBase] {
	return func(name string, offset uintptr) OutputField[erasedBase] {
		return Column(name, dec, func(base *erasedBase) *V { return (*V)(unsafe.Add(unsafe.Pointer(base), offset)) })
	}
}

func erasedStreamColumnOf[V any](dec StreamDecoder[V]) func(string, uintptr) StreamOutputField[erasedBase] {
	return func(name string, offset uintptr) StreamOutputField[erasedBase] {
		return StreamColumn(name, dec, func(base *erasedBase) *V { return (*V)(unsafe.Add(unsafe.Pointer(base), offset)) })
	}
}

// erasedParam binds a parameter of P with a field built for erasedBase,
// passing the field the address of the parameter struct, which arrives by
// value, without moving it to the heap (structAddr).
type erasedParam[P any] struct{ inner InputField[*erasedBase] }

func (f erasedParam[P]) location() fieldLocation { return f.inner.location() }
func (f erasedParam[P]) compile(typ DataType) (InputField[P], error) {
	inner, err := f.inner.compile(typ)
	if err != nil {
		return nil, err
	}
	f.inner = inner
	return f, nil
}
func (f erasedParam[P]) encode(w *ParamWriter, value P) error {
	return f.inner.encode(w, structAddr(&value))
}

// directParamOp is the inner field's direct op, which calls its getter with
// the struct's address (paramOp.erased), with f as its fallback field.
func (f erasedParam[P]) directParamOp() (paramOp[P], bool) {
	d, ok := f.inner.(directParamField[*erasedBase])
	if !ok {
		return paramOp[P]{}, false
	}
	op, ok := d.directParamOp()
	if !ok {
		return paramOp[P]{}, false
	}
	return paramOp[P]{field: f, get: op.get, width: op.width, colWidth: op.colWidth, columnKind: op.columnKind, alt: op.alt, kind: op.kind, erased: true}, true
}

// structAddr returns p as the erasedBase pointer of an erased field, hiding
// it from escape analysis: erasedParam and paramOp.encodeDirect take the
// address of a parameter struct that arrives by value, and passing it to an
// erased field, an interface or a func value, would otherwise move the
// struct to the heap on every bind.
//
// unsafe: an erased field only reads the field at its offset from the
// address, during the call, and never retains the address, so the struct
// outlives every use. The address is loaded back as a pointer, so the GC
// and stack copying see it; only escape analysis loses track of it through
// the uintptr.
func structAddr[P any](p *P) *erasedBase {
	x := uintptr(unsafe.Pointer(p))
	return *(**erasedBase)(unsafe.Pointer(&x))
}

// erasedColumn decodes a column of R with a field built for erasedBase.
type erasedColumn[R any] struct{ inner OutputField[erasedBase] }

func erased[R any](row *R) *erasedBase { return (*erasedBase)(unsafe.Pointer(row)) }

func (f erasedColumn[R]) location() fieldLocation { return f.inner.location() }
func (f erasedColumn[R]) compile(typ DataType) (OutputField[R], error) {
	inner, err := f.inner.compile(typ)
	if err != nil {
		return nil, err
	}
	f.inner = inner
	return f, nil
}
func (f erasedColumn[R]) decode(r *RowReader, row *R) error { return f.inner.decode(r, erased(row)) }
func (f erasedColumn[R]) clone(dst, source *R)              { f.inner.clone(erased(dst), erased(source)) }
func (f erasedColumn[R]) detachField(row *R)                { f.inner.detachField(erased(row)) }

// directRowOp is the inner field's direct op. Its accessor, a
// func(*erasedBase) *V, is called as a func(*R) unsafe.Pointer: both take
// and return one pointer, so they are called the same way (funcPointer),
// and the row's address is the erasedBase address the accessor expects.
func (f erasedColumn[R]) directRowOp() (rowOp[R], bool) {
	d, ok := f.inner.(directRowField[erasedBase])
	if !ok {
		return rowOp[R]{}, false
	}
	op, ok := d.directRowOp()
	if !ok {
		return rowOp[R]{}, false
	}
	out := rowOp[R]{access: op.access, width: op.width, colWidth: op.colWidth, columnKind: op.columnKind, alt: op.alt, kind: op.kind, compiled: op.compiled}
	if op.field != nil {
		out.field = erasedColumn[R]{inner: op.field}
	}
	return out, true
}

// erasedStreamColumn decodes a streamed column of R with a field built for
// erasedBase. Its funcs take a *erasedBase where R's take a *R, so they are
// called the same way (funcPointer) and are used as R's, with the row's
// address as the erasedBase address the field expects.
func erasedStreamColumn[R any](field StreamOutputField[erasedBase]) StreamOutputField[R] {
	inner := field.streamDeclaration()
	return &streamOutputField[R]{loc: inner.loc,
		resetMissing: *(*func(*R))(unsafe.Pointer(&inner.resetMissing)),
		compile: func(typ DataType) (streamFieldPlan[R], error) {
			plan, err := inner.compile(typ)
			if err != nil {
				return streamFieldPlan[R]{}, err
			}
			return streamFieldPlan[R]{
				reset:  *(*func(*R))(unsafe.Pointer(&plan.reset)),
				decode: *(*func(*StreamRowReader, *R) error)(unsafe.Pointer(&plan.decode)),
			}, nil
		},
	}
}

// boundField is a field type's builders for struct P.
type boundField[P any] struct {
	param  func(name string, offset uintptr) InputField[P]
	column func(name string, offset uintptr) OutputField[P]
}

// bindEntry returns e's fields for struct P.
func bindEntry[P any](e erasedEntry) boundField[P] {
	var entry boundField[P]
	if e.param != nil {
		entry.param = func(name string, offset uintptr) InputField[P] {
			return erasedParam[P]{inner: e.param(name, offset)}
		}
	}
	if e.column != nil {
		entry.column = func(name string, offset uintptr) OutputField[P] {
			return erasedColumn[P]{inner: e.column(name, offset)}
		}
	}
	return entry
}

type codecOption[V any] struct {
	enc   Encoder[V]
	codec Codec[V] // nil for WithEncoder
	name  string
}

func (o codecOption[V]) option() (string, structTarget) {
	if o.codec == nil {
		return o.name, targetParams
	}
	return o.name, targetParams | targetRows | targetUDT | targetTuple
}

func (o codecOption[V]) apply(c *structOptionSet) {
	if o.codec != nil {
		c.codecs[reflect.TypeFor[V]()] = deriveCodec(o.codec)
		return
	}
	c.codecs[reflect.TypeFor[V]()] = deriveEncoder(o.enc)
}

// WithCodec maps the struct's fields of type V with codec, and V wherever a
// field's type is derived from it: []V and [N]V (a list, set or vector), *V,
// Nullable[V], BindValue[V] (a parameter only), Pair members, map keys and
// values, and nested forms such as [][]V or map[string][]V. Use it for a type
// with no built-in codec and no CQLCodec method, such as a UDT from another
// package, or to replace V's default. V is inferred from codec, as in
// WithCodec(statusCodec). Recognized built-in codec tags take precedence. The
// option also applies to generated mappings; other generated
// custom-expression codecs remain defaults. ParamsOf, RowOf, UDTCodecOf and
// TupleCodecOf apply it; StreamRowOf decodes with stream decoders, so it
// rejects it: use WithStreamDecoder.
func WithCodec[V any](codec Codec[V]) StructOption {
	return codecOption[V]{enc: codec, codec: codec, name: "WithCodec"}
}

// WithEncoder maps the parameter fields whose type is V, or derived from V,
// with encoder, for a type that is only ever bound, as WithCodec does for
// both directions. Only ParamsOf applies it.
func WithEncoder[V any](encoder Encoder[V]) StructOption {
	return codecOption[V]{enc: encoder, name: "WithEncoder"}
}

// structFields is a struct's buffered mapping as cmd/cqlgen generates it
// (StructFields), or as cql.StreamRowOf generates it (StreamColumns).
type structFields[P any] struct {
	Params        []InputField[P]
	Columns       []OutputField[P]
	StreamColumns []StreamOutputField[P]
}

// GeneratedParams binds parameters from the generated mapping fields, with
// opts applied to it, or reflects P when fields has no buffered mapping.
func generatedParams[P any](fields structFields[P], target structTarget, opts ...StructOption) (ParamEncoder[P], error) {
	config, err := structOptions[P](target, opts)
	if err != nil {
		return nil, err
	}
	if fields.Params == nil {
		return reflectParams(config)
	}
	fields, err = generatedStructOptions(fields, config)
	if err != nil {
		return nil, err
	}
	return structParams(fields.Params), nil
}

// GeneratedRow decodes rows with the generated mapping fields, with opts
// applied to it, or reflects R when fields has no buffered mapping.
func generatedRow[R any](fields structFields[R], target structTarget, opts ...StructOption) (rowCodec[R], error) {
	config, err := structOptions[R](target, opts)
	if err != nil {
		return rowCodec[R]{}, err
	}
	if fields.Columns == nil {
		return reflectRow(config)
	}
	fields, err = generatedStructOptions(fields, config)
	if err != nil {
		return rowCodec[R]{}, err
	}
	reset, err := bufferReset[R](config.reuseBuffers)
	if err != nil {
		return rowCodec[R]{}, err
	}
	return structRow(fields.Columns, reset, config), nil
}

// generatedStructOptions applies options and per-field tags to a generated
// mapping. It replaces only the fields whose type a type option derives:
// the generated defaults may contain Go codec expressions that reflection
// cannot evaluate, so rebuilding the entire mapping reflectively would lose
// them. Field discovery and option resolution happen here, never during
// execution.
func generatedStructOptions[P any](fields structFields[P], config structConfig[P]) (structFields[P], error) {
	fields.Params = append([]InputField[P](nil), fields.Params...)
	fields.Columns = append([]OutputField[P](nil), fields.Columns...)
	fields.StreamColumns = append([]StreamOutputField[P](nil), fields.StreamColumns...)
	params, columns, streams := map[string]int{}, map[string]int{}, map[string]int{}
	for i, field := range fields.Params {
		if field != nil && !field.location().positional {
			params[field.location().name] = i
		}
	}
	for i, field := range fields.Columns {
		if field != nil && !field.location().positional {
			columns[field.location().name] = i
		}
	}
	for i, field := range fields.StreamColumns {
		if field != nil && !field.streamDeclaration().loc.positional {
			streams[field.streamDeclaration().loc.name] = i
		}
	}
	var streamBuiltins []streamBuiltinField[P]
	if len(config.streams) != 0 {
		streamBuiltins = streamBuiltinFields[P]()
	}
	err := eachStructField(reflect.TypeFor[P](), func(sf reflect.StructField, tag structmap.FieldTag) error {
		// An option replaces the generated codec of a field whose type is
		// derived from the option's, unless a built-in tag selects the
		// codec; a custom expression is replaced by the derived codec.
		if len(config.codecs) != 0 && derivesFrom(sf.Type, config.codecs) {
			d, err := deriveField(sf.Type, tag.Codec, config.codecs)
			if err != nil {
				return err
			}
			bound := bindEntry[P](d.fieldEntry())
			if i, ok := params[tag.Column]; ok && bound.param != nil {
				fields.Params[i] = bound.param(tag.Column, sf.Offset)
			}
			if i, ok := columns[tag.Column]; ok && bound.column != nil {
				fields.Columns[i] = bound.column(tag.Column, sf.Offset)
			}
		}
		if entry, ok := config.streams[sf.Type]; ok && !tagSelectsBuiltin(streamBuiltins, sf.Type, tag.Stream) {
			if i, ok := streams[tag.Column]; ok {
				fields.StreamColumns[i] = entry.column(tag.Column, sf.Offset)
			}
		}
		if i, ok := params[tag.Column]; ok {
			field, err := nilUnsetParam(fields.Params[i], sf, tag, config.nilUnset)
			if err != nil {
				return err
			}
			fields.Params[i] = field
		}
		if i, ok := columns[tag.Column]; ok && tag.NullZero {
			fields.Columns[i] = nullZeroField[P]{fields.Columns[i]}
		}
		if i, ok := streams[tag.Column]; ok && tag.NullZero {
			fields.StreamColumns[i] = nullZeroStream(fields.StreamColumns[i])
		}
		return nil
	})
	return fields, err
}

// derivesFrom reports whether a field of type t is derived from a type that
// options map: t itself, an element, a pointer target, a map key or value,
// or a member of a Nullable, BindValue or Pair.
func derivesFrom(t reflect.Type, options map[reflect.Type]derived) bool {
	if _, ok := options[t]; ok {
		return true
	}
	switch t.Kind() {
	case reflect.Slice, reflect.Array, reflect.Pointer:
		return derivesFrom(t.Elem(), options)
	case reflect.Map:
		return derivesFrom(t.Key(), options) || derivesFrom(t.Elem(), options)
	case reflect.Struct:
		if genericOf(t, nullableName) || genericOf(t, bindName) || genericOf(t, pairName) {
			for i := range t.NumField() {
				if derivesFrom(t.Field(i).Type, options) {
					return true
				}
			}
		}
	}
	return false
}

func tagSelectsBuiltin[E interface {
	meta() structmap.BuiltinFieldType
}](builtins []E, typ reflect.Type, name string) bool {
	if name == "" {
		return false
	}
	for _, entry := range builtinsFor(builtins, typ) {
		for _, candidate := range entry.meta().Names {
			if candidate == name {
				return true
			}
		}
	}
	return false
}

// builtinsFor returns the entries of builtins for field type typ: those of
// typ itself or, when it has none, those of the type it maps like because it
// has that type's layout (sameLayout).
func builtinsFor[E interface {
	meta() structmap.BuiltinFieldType
}](builtins []E, typ reflect.Type) []E {
	var entries []E
	for _, b := range builtins {
		if b.meta().Type == typ {
			entries = append(entries, b)
		}
	}
	if len(entries) != 0 {
		return entries
	}
	for _, b := range builtins {
		if sameLayout(typ, b.meta().Type) {
			entries = append(entries, b)
		}
	}
	return entries
}

// ReflectParams maps the fields of struct P by reflection, once, here.
func reflectParams[P any](config structConfig[P]) (ParamEncoder[P], error) {
	fields, err := reflectFields(config)
	if err != nil {
		return nil, err
	}
	params := make([]InputField[P], len(fields))
	for i, f := range fields {
		param, err := nilUnsetParam(f.entry.param(f.tag.Column, f.sf.Offset), f.sf, f.tag, config.nilUnset)
		if err != nil {
			return nil, fmt.Errorf("gocql: struct mapping of %s: field %s: %w", reflect.TypeFor[P](), f.sf.Name, err)
		}
		params[i] = param
	}
	return structParams(params), nil
}

// ReflectRow maps the fields of struct R by reflection, once, here.
func reflectRow[R any](config structConfig[R]) (rowCodec[R], error) {
	fields, err := reflectFields(config)
	if err != nil {
		return rowCodec[R]{}, err
	}
	columns := make([]OutputField[R], len(fields))
	for i, f := range fields {
		if f.entry.column == nil {
			return rowCodec[R]{}, fmt.Errorf("gocql: struct mapping of %s: field %s: type %s only binds parameters", reflect.TypeFor[R](), f.sf.Name, f.sf.Type)
		}
		columns[i] = f.entry.column(f.tag.Column, f.sf.Offset)
		if f.tag.NullZero {
			columns[i] = nullZeroField[R]{columns[i]}
		}
	}
	reset, err := bufferReset[R](config.reuseBuffers)
	if err != nil {
		return rowCodec[R]{}, err
	}
	return structRow(columns, reset, config), nil
}

func structParams[P any](fields []InputField[P]) ParamEncoder[P] {
	return paramsCodec[P]{fields: append([]InputField[P](nil), fields...), optional: true}
}

func structRow[R any](fields []OutputField[R], reset func(*R), config structConfig[R]) rowCodec[R] {
	return rowCodec[R]{fields: append([]OutputField[R](nil), fields...), optional: true, extra: config.udt.allowExtra, reset: reset, nullZero: config.nullZero}
}

// nilUnsetParam returns field, which binds struct field sf, binding UNSET
// for a nil value when its tag has nilunset or when all is set (NilAsUnset).
// A nilunset tag on a field that cannot be nil is an error; NilAsUnset skips
// such fields.
func nilUnsetParam[P any](field InputField[P], sf reflect.StructField, tag structmap.FieldTag, all bool) (InputField[P], error) {
	switch sf.Type.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice:
	default:
		if tag.NilUnset {
			return nil, fmt.Errorf("nilunset on a field of type %s, which cannot be nil", sf.Type)
		}
		return field, nil
	}
	if !tag.NilUnset && !all {
		return field, nil
	}
	return nilUnsetField[P]{InputField: field, offset: sf.Offset, slice: sf.Type.Kind() == reflect.Slice}, nil
}

// nilUnsetField binds UNSET for a nil pointer, map or slice field, and the
// field's value otherwise.
type nilUnsetField[P any] struct {
	InputField[P]
	offset uintptr
	slice  bool
}

func (f nilUnsetField[P]) compile(typ DataType) (InputField[P], error) {
	inner, err := f.InputField.compile(typ)
	if err != nil {
		return nil, err
	}
	f.InputField = inner
	return f, nil
}

func (f nilUnsetField[P]) encode(w *ParamWriter, value P) error {
	// unsafe: offset is the reflect offset of a pointer, map or slice field
	// of P (eachStructField); a pointer and a map are one pointer word, and
	// every slice header has the same layout (see bufferReset).
	p := unsafe.Add(unsafe.Pointer(structAddr(&value)), f.offset)
	if f.slice && *(*[]byte)(p) == nil || !f.slice && *(*unsafe.Pointer)(p) == nil {
		return w.WriteParam(Encoder[struct{}](unsetEncoder{}), struct{}{})
	}
	return f.InputField.encode(w, value)
}

// unsetEncoder binds UNSET for any column type.
type unsetEncoder struct{}

func (unsetEncoder) CheckType(DataType) error                { return nil }
func (unsetEncoder) Encode(w *ValueWriter, _ struct{}) error { return w.Unset() }

// nullZeroField decodes its field, leaving it zero for a NULL or missing
// cell instead of failing (NullAsZero, the nullzero tag). The plans that
// run it skip the cell themselves (rowOps).
type nullZeroField[R any] struct{ OutputField[R] }

func (f nullZeroField[R]) compile(typ DataType) (OutputField[R], error) {
	inner, err := f.OutputField.compile(typ)
	if err != nil {
		return nil, err
	}
	return nullZeroField[R]{inner}, nil
}

func (f nullZeroField[R]) decode(r *RowReader, row *R) error {
	if r.skipNullCell() {
		return nil
	}
	return f.OutputField.decode(r, row)
}

type reuseBuffersOption struct{}

func (reuseBuffersOption) option() (string, structTarget) { return "ReuseBuffers", targetRows }
func (reuseBuffersOption) apply(c *structOptionSet)       { c.reuseBuffers = true }

// ReuseBuffers makes RowOf keep the storage of the struct's slice fields,
// such as []byte and lists, between rows decoded into the same destination,
// as RowInto does. The next row overwrites that storage, so copy a field that
// must outlive its row. Other fields, maps included, are zeroed as usual.
// Only RowOf applies it: ParamsOf has no destination, and StreamRowOf always
// reuses slice storage.
func ReuseBuffers() StructOption { return reuseBuffersOption{} }

type ignoreExtraFieldsOption struct{}

func (ignoreExtraFieldsOption) option() (string, structTarget) {
	return "IgnoreExtraFields", targetRows | targetStream | targetUDT | targetUDTValue
}
func (ignoreExtraFieldsOption) apply(c *structOptionSet) { c.udt.allowExtra = true }

// IgnoreExtraFields skips database fields that no struct field maps: a
// result column in RowOf and StreamRowOf, so that SELECT * keeps working
// after ALTER TABLE ADD, and a UDT field in UDTCodecOf, UDTEncoder,
// UDTDecoder and StreamUDT, so that a UDT keeps decoding after ALTER TYPE
// ADD; such a UDT field is bound as NULL. Without it every database field
// needs a struct field, which catches a misspelled tag.
func IgnoreExtraFields() StructOption { return ignoreExtraFieldsOption{} }

type allowMissingFieldsOption struct{}

func (allowMissingFieldsOption) option() (string, structTarget) {
	return "AllowMissingFields", targetUDT | targetUDTValue
}
func (allowMissingFieldsOption) apply(c *structOptionSet) { c.udt.allowMissing = true }

// AllowMissingFields accepts struct fields that the UDT does not have, in
// UDTCodecOf, UDTEncoder, UDTDecoder and StreamUDT. It concerns fields absent
// from the UDT's metadata, not NULL or absent serialized values, which decode
// as zero.
func AllowMissingFields() StructOption { return allowMissingFieldsOption{} }

type udtNameOption struct{ keyspace, name string }

func (udtNameOption) option() (string, structTarget) { return "UDTName", targetUDT | targetUDTValue }
func (o udtNameOption) apply(c *structOptionSet)     { c.udt.keyspace, c.udt.name = o.keyspace, o.name }

// UDTName names the UDT that UDTCodecOf, UDTEncoder, UDTDecoder or StreamUDT
// expects. An empty keyspace matches the UDT in any keyspace, and an empty
// name a UDT of any name; without the option the UDT matches by fields
// alone.
func UDTName(keyspace, name string) StructOption {
	return udtNameOption{keyspace: keyspace, name: name}
}

// UDTNameOf returns the UDTName of typ's UDT. It fails when typ is not a
// UDT.
func UDTNameOf(typ DataType) (StructOption, error) {
	keyspace, name, ok := typ.UDTName()
	if !ok {
		return nil, typeValidation("type", "%s is not a UDT", typ)
	}
	return UDTName(keyspace, name), nil
}

type nullAsZeroOption struct{}

func (nullAsZeroOption) option() (string, structTarget) {
	return "NullAsZero", targetRows | targetStream | targetUDT | targetTuple
}
func (nullAsZeroOption) apply(c *structOptionSet) { c.nullZero = true }

// NullAsZero makes RowOf, StreamRowOf and TupleCodecOf leave a field at its
// zero value for a NULL cell, or a tuple member the server left out, instead
// of failing with an error matching ErrNull. The `cql:"name,nullzero"` tag
// does the same for one field. A UDT's fields always decode so: the server
// sends NULL for every field added by ALTER TYPE ADD since the value was
// written, so UDTCodecOf accepts the option but needs none. A Nullable or
// pointer field already decodes NULL, as invalid or nil.
func NullAsZero() StructOption { return nullAsZeroOption{} }

type nilAsUnsetOption struct{}

func (nilAsUnsetOption) option() (string, structTarget) { return "NilAsUnset", targetParams }
func (nilAsUnsetOption) apply(c *structOptionSet)       { c.nilUnset = true }

// NilAsUnset makes ParamsOf bind UNSET, which leaves the column unchanged,
// for every nil pointer, slice or map field, instead of NULL or an empty
// collection. The `cql:"name,nilunset"` tag does the same for one field. A
// BindValue field binds UNSET per value; see Bind, BindNull and BindUnset.
func NilAsUnset() StructOption { return nilAsUnsetOption{} }

// bufferReset returns the RowInto reset that keeps the storage of R's mapped
// slice fields, or nil, which zeroes the row, when reuse is off or R has none.
func bufferReset[R any](reuse bool) (func(*R), error) {
	if !reuse {
		return nil, nil
	}
	var offsets []uintptr
	err := eachStructField(reflect.TypeFor[R](), func(sf reflect.StructField, _ structmap.FieldTag) error {
		if sf.Type.Kind() == reflect.Slice {
			offsets = append(offsets, sf.Offset)
		}
		return nil
	})
	if err != nil || len(offsets) == 0 {
		return nil, err
	}
	return func(r *R) {
		old := *r
		var zero R
		*r = zero
		// unsafe: offsets are the reflect offsets of R's slice fields
		// (see erasedBase), and old keeps the previous backing arrays alive.
		base, prev := unsafe.Pointer(r), unsafe.Pointer(&old)
		for _, off := range offsets {
			// Every slice header has the same layout, so truncating it as a
			// []byte keeps any slice's pointer and capacity.
			*(*[]byte)(unsafe.Add(base, off)) = (*(*[]byte)(unsafe.Add(prev, off)))[:0]
		}
	}, nil
}

type reflectedField[P any] struct {
	entry boundField[P]
	tag   structmap.FieldTag
	sf    reflect.StructField
}

func reflectFields[P any](config structConfig[P]) ([]reflectedField[P], error) {
	var fields []reflectedField[P]
	err := eachStructField(reflect.TypeFor[P](), func(sf reflect.StructField, tag structmap.FieldTag) error {
		d, err := deriveField(sf.Type, tag.Codec, config.codecs)
		if err == nil {
			fields = append(fields, reflectedField[P]{sf: sf, tag: tag, entry: bindEntry[P](d.fieldEntry())})
		}
		return err
	})
	return fields, err
}

// structOptions binds opts to struct P for the target constructor, which
// rejects an option it does not apply.
func structOptions[P any](target structTarget, opts []StructOption) (structConfig[P], error) {
	t := reflect.TypeFor[P]()
	config := structConfig[P]{codecs: map[reflect.Type]derived{}, streams: map[reflect.Type]streamBuiltinField[P]{}}
	if t.Kind() != reflect.Struct {
		return config, typeValidation("struct", "%s is not a struct", t)
	}
	set, err := applyOptions(target, t.String(), opts)
	if err != nil {
		return config, err
	}
	config.codecs = set.codecs
	for typ, e := range set.streams {
		column := e.column
		config.streams[typ] = streamBuiltinField[P]{column: func(name string, offset uintptr) StreamOutputField[P] {
			return erasedStreamColumn[P](column(name, offset))
		}}
	}
	config.udt, config.reuseBuffers, config.nullZero, config.nilUnset = set.udt, set.reuseBuffers, set.nullZero, set.nilUnset
	if target == targetUDT {
		// A UDT's fields decode NULL as zero; see NullAsZero.
		config.nullZero = true
	}
	return config, nil
}

// applyOptions applies opts for the target constructor of what, which
// rejects an option it does not apply.
func applyOptions(target structTarget, what string, opts []StructOption) (structOptionSet, error) {
	set := structOptionSet{codecs: map[reflect.Type]derived{}, streams: map[reflect.Type]erasedStreamEntry{}}
	for _, opt := range opts {
		if opt == nil {
			return set, typeValidation("StructOption", "nil option for %s of %s", target, what)
		}
		if name, targets := opt.option(); targets&target == 0 {
			return set, typeValidation(name, "%s does not apply it", target)
		}
		opt.apply(&set)
	}
	return set, nil
}

// eachStructField calls fn for each mapped field of struct t, in order. The
// fields of a struct embedded by value are mapped as if declared in t, with
// their offset from t and a dotted Name; see ResolveFieldConflicts.
func eachStructField(t reflect.Type, fn func(reflect.StructField, structmap.FieldTag) error) error {
	var fields []reflect.StructField
	var candidates []structmap.FieldCandidate
	var tags []structmap.FieldTag
	var walk func(t reflect.Type, prefix string, offset uintptr, depth int) error
	walk = func(t reflect.Type, prefix string, offset uintptr, depth int) error {
		for i := range t.NumField() {
			sf := t.Field(i)
			path := prefix + sf.Name
			if sf.Anonymous {
				embed, err := structmap.EmbeddedField(sf.Name, sf.Tag, sf.Type.Kind() == reflect.Pointer, sf.Type.Kind() == reflect.Struct)
				if err != nil {
					return fmt.Errorf("gocql: struct mapping of %s: %w", t, err)
				}
				if embed == structmap.EmbedSkip {
					continue
				}
				if embed == structmap.EmbedFlatten {
					if err := walk(sf.Type, path+".", offset+sf.Offset, depth+1); err != nil {
						return err
					}
					continue
				}
			}
			tag, ok, err := structmap.ParseFieldTag(sf.Name, sf.IsExported(), sf.Tag)
			if err != nil {
				return fmt.Errorf("gocql: struct mapping of %s: %w", t, err)
			}
			if !ok {
				continue
			}
			sf.Name, sf.Offset = path, offset+sf.Offset
			fields = append(fields, sf)
			tags = append(tags, tag)
			candidates = append(candidates, structmap.FieldCandidate{Column: tag.Column, Path: path, Depth: depth})
		}
		return nil
	}
	if err := walk(t, "", 0, 0); err != nil {
		return err
	}
	keep, err := structmap.ResolveFieldConflicts(candidates)
	if err != nil {
		return fmt.Errorf("gocql: struct mapping of %s: %w", t, err)
	}
	for _, i := range keep {
		if err := fn(fields[i], tags[i]); err != nil {
			return fmt.Errorf("gocql: struct mapping of %s: field %s: %w", t, fields[i].Name, err)
		}
	}
	return nil
}

// resolveStreamField picks a streamed field's decoder: a built-in named by
// its tag, then an option for its type, then the built-in default.
func resolveStreamField[E interface {
	meta() structmap.BuiltinFieldType
}](builtins []E, options map[reflect.Type]E, typ reflect.Type, codec, option string) (E, error) {
	var zero E
	// A named type maps like its underlying type when that has a built-in
	// decoder, and so do slices and Nullable wrappers of it. Their fields
	// read the field as the built-in type, whose memory layout is the same.
	entries := builtinsFor(builtins, typ)
	candidates := make([]structmap.BuiltinFieldType, len(entries))
	for i, e := range entries {
		candidates[i] = e.meta()
	}
	if codec != "" {
		if i, ok := structmap.SelectBuiltin(candidates, codec); ok {
			return entries[i], nil
		}
		if entry, ok := options[typ]; ok {
			return entry, nil
		}
		return zero, fmt.Errorf("codec %q is not a built-in codec for %s; pass %s for %s", codec, typ, option, typ)
	}
	if entry, ok := options[typ]; ok {
		return entry, nil
	}
	if i, ok := structmap.SelectBuiltin(candidates, ""); ok {
		return entries[i], nil
	}
	return zero, fmt.Errorf("no codec for type %s; pass %s or use a supported type", typ, option)
}

// sameLayout reports whether field type t maps like the built-in field type
// u: t is u with named types in place of the predeclared or unnamed types
// they are defined from, as a whole or as an element, a key, a value or the
// type argument of a cql.Nullable or cql.BindValue. Status, []Status,
// *Status, Nullable[Status] and map[Status]int64 map like string, []string,
// *string, Nullable[string] and map[string]int64 for type Status string. A
// type defined from another named type, such as cql.UUID, does not.
//
// Such a t has u's memory layout, so its field is read and written as u.
// For a map it has u's runtime representation too: a named key or value
// hashes, compares and is stored as the type it is defined from, so a map
// built as map[string]V is a valid map[Status]V.
func sameLayout(t, u reflect.Type) bool {
	if t == u {
		return true
	}
	if t.Kind() != u.Kind() {
		return false
	}
	if u.Name() != "" {
		if u.PkgPath() == "" {
			// Predeclared, such as string: t, of its kind, is defined from it.
			return true
		}
		return sameInstance(t, u)
	}
	switch u.Kind() {
	case reflect.Slice, reflect.Pointer:
		return sameLayout(t.Elem(), u.Elem())
	case reflect.Array:
		return t.Len() == u.Len() && sameLayout(t.Elem(), u.Elem())
	case reflect.Map:
		return sameLayout(t.Key(), u.Key()) && sameLayout(t.Elem(), u.Elem())
	}
	return false
}

// sameInstance reports whether the struct types t and u instantiate one
// generic type, such as Nullable, with type arguments of the same layout:
// their fields match by name and offset and have the same layout.
func sameInstance(t, u reflect.Type) bool {
	base := func(name string) (string, bool) {
		i := strings.IndexByte(name, '[')
		return name[:max(i, 0)], i > 0
	}
	tb, tok := base(t.Name())
	ub, uok := base(u.Name())
	if !tok || !uok || tb != ub || t.PkgPath() != u.PkgPath() || t.Kind() != reflect.Struct || t.NumField() != u.NumField() {
		return false
	}
	for i := range u.NumField() {
		tf, uf := t.Field(i), u.Field(i)
		if tf.Name != uf.Name || tf.Offset != uf.Offset || !sameLayout(tf.Type, uf.Type) {
			return false
		}
	}
	return true
}

// Streaming rows decode with StreamDecoders, which use their own Go types: text
// and blob columns decode into reusable []byte storage, and there is no string
// decoder. The `stream=` tag key picks among them like `codec=` does for
// buffered rows; a field's `codec=` tag does not apply to streaming.

type streamBuiltinField[P any] struct {
	column func(name string, offset uintptr) StreamOutputField[P]
	structmap.BuiltinFieldType
}

func (f streamBuiltinField[P]) meta() structmap.BuiltinFieldType { return f.BuiltinFieldType }

// streamBuiltinFields returns the built-in streaming field types, each with
// its Nullable form and its list form, and for a type's default decoder its
// set and vector forms.
func streamBuiltinFields[P any]() []streamBuiltinField[P] {
	erased := erasedStreamBuiltins()
	fields := make([]streamBuiltinField[P], len(erased))
	for i, b := range erased {
		column := b.column
		fields[i] = streamBuiltinField[P]{BuiltinFieldType: b.BuiltinFieldType, column: func(name string, offset uintptr) StreamOutputField[P] {
			return erasedStreamColumn[P](column(name, offset))
		}}
	}
	return fields
}

// erasedStreamBuiltin is a built-in streaming field type with its field
// built for erasedBase, as erasedBuiltin is for buffered rows.
type erasedStreamBuiltin struct {
	erasedStreamEntry
	structmap.BuiltinFieldType
}

// erasedStreamBuiltins returns the built-in streaming field types with
// their fields built for erasedBase, once per field type rather than once per
// struct type, as erasedBuiltins does.
func erasedStreamBuiltins() []erasedStreamBuiltin {
	var fields []erasedStreamBuiltin
	add := func(entries ...erasedStreamBuiltin) { fields = append(fields, entries...) }
	add(streamBuiltin(StreamBool, "bool", "Bool", true)...)
	add(streamBuiltin(StreamInt8, "int8", "Int8", true)...)
	add(streamBuiltin(StreamInt16, "int16", "Int16", true)...)
	add(streamBuiltin(StreamInt32, "int32", "Int32", true)...)
	add(streamBuiltin(StreamInt64, "int64", "Int64", true)...)
	add(streamBuiltin(StreamFloat32, "float32", "Float32", true)...)
	add(streamBuiltin(StreamFloat64, "float64", "Float64", true)...)
	add(streamBuiltin(StreamBlob, "[]byte", "Blob", true)...)
	// ASCII names StreamText, which decodes ascii too, in a tag.
	add(streamBuiltin(StreamText, "[]byte", "Text", false, "ASCII")...)
	add(streamBuiltin(StreamVarint, "[]byte", "Varint", false)...)
	add(streamBuiltin(StreamUUID, "cql.UUID", "UUID", true)...)
	add(streamBuiltin(StreamTimeUUID, "cql.TimeUUID", "TimeUUID", true)...)
	add(streamBuiltin(StreamInet, "netip.Addr", "Inet", true)...)
	add(streamBuiltin(StreamDecimal, "cql.DecimalParts", "Decimal", true)...)
	add(streamBuiltin(StreamDate, "cql.Date", "Date", true)...)
	add(streamBuiltin(StreamTime, "cql.TimeOfDay", "Time", true)...)
	add(streamBuiltin(StreamTimestamp, "cql.Timestamp", "Timestamp", true)...)
	add(streamBuiltin(StreamDuration, "cql.Duration", "Duration", true)...)
	// The allocating decoders, for the types the seeded ones do not cover.
	add(streamBuiltin(StreamString, "string", "String", true)...)
	add(streamBuiltin(StreamTimestampTime, "time.Time", "TimestampTime", true)...)
	add(streamBuiltin(StreamTimeDuration, "time.Duration", "TimeDuration", true)...)
	add(streamBuiltin(StreamDecimalValue, "cql.Decimal", "DecimalValue", true)...)
	return fields
}

// streamBuiltin returns the entries for V, Nullable[V], and []V as whichever
// of a list, a set or a vector the column is (the default), or as a list
// (`stream=List`), a set (`stream=Set`) or a vector (`stream=Vector`). For a
// decoder that is not its type's default, as Text is not for []byte, the tag
// that names it, or one of aliases, also selects the slice of it, and there
// is no list, set or vector entry: those tags already select the default
// decoder's.
func streamBuiltin[V any](dec StreamDecoder[V], typeSrc, name string, def bool, aliases ...string) []erasedStreamBuiltin {
	names := append([]string{name}, aliases...)
	entry := func(typ reflect.Type, typeSrc, decSrc string, names []string, def bool, column func(string, uintptr) StreamOutputField[erasedBase]) erasedStreamBuiltin {
		return erasedStreamBuiltin{erasedStreamEntry: erasedStreamEntry{column: column}, BuiltinFieldType: structmap.BuiltinFieldType{
			Type: typ, TypeSrc: typeSrc, DecoderSrc: decSrc, Names: names, Default: def,
		}}
	}
	src := "cql.Stream" + name
	plain := []erasedStreamBuiltin{
		entry(reflect.TypeFor[V](), typeSrc, src, names, def, erasedStreamColumnOf(dec)),
		entry(reflect.TypeFor[Nullable[V]](), "cql.Nullable["+typeSrc+"]", "cql.StreamNullable("+src+")",
			names, def, erasedStreamColumnOf(StreamNullable(dec))),
	}
	if !def {
		return append(plain, entry(reflect.TypeFor[[]V](), "[]"+typeSrc, "cql.StreamSlice("+src+")",
			names, false, erasedStreamColumnOf(StreamSlice(dec))))
	}
	// A pointer allocates its value, which a decoder of seeded storage
	// cannot fill.
	if t := reflect.TypeFor[V](); t.Kind() != reflect.Slice && t != reflect.TypeFor[DecimalParts]() {
		plain = append(plain, entry(reflect.TypeFor[*V](), "*"+typeSrc, "cql.StreamPointer("+src+")",
			names, def, erasedStreamColumnOf(StreamPointer(dec))))
	}
	return append(plain, []erasedStreamBuiltin{
		// A slice takes its element's default decoder.
		entry(reflect.TypeFor[[]V](), "[]"+typeSrc, "cql.StreamSlice("+src+")",
			nil, def, erasedStreamColumnOf(StreamSlice(dec))),
		entry(reflect.TypeFor[[]V](), "[]"+typeSrc, "cql.StreamList("+src+")",
			[]string{"List"}, false, erasedStreamColumnOf(StreamList(dec))),
		entry(reflect.TypeFor[[]V](), "[]"+typeSrc, "cql.StreamSet("+src+")",
			[]string{"Set"}, false, erasedStreamColumnOf(StreamSet(dec))),
		entry(reflect.TypeFor[[]V](), "[]"+typeSrc, "cql.StreamVector("+src+")",
			[]string{"Vector"}, false, erasedStreamColumnOf(StreamVector(dec))),
	}...)
}

// StreamBuiltinFieldTypes returns the Go field types StreamRowOf maps without
// configuration. EncoderSrc is empty: streaming only decodes.
func streamBuiltinFieldTypes() []structmap.BuiltinFieldType {
	fields := streamBuiltinFields[struct{}]()
	types := make([]structmap.BuiltinFieldType, len(fields))
	for i, f := range fields {
		types[i] = f.BuiltinFieldType
	}
	return types
}

type streamCodecOption[V any] struct{ decoder StreamDecoder[V] }

func (streamCodecOption[V]) option() (string, structTarget) { return "WithStreamDecoder", targetStream }
func (o streamCodecOption[V]) apply(c *structOptionSet) {
	c.streams[reflect.TypeFor[V]()] = erasedStreamEntry{column: erasedStreamColumnOf(o.decoder)}
	c.streams[reflect.TypeFor[Nullable[V]]()] = erasedStreamEntry{column: erasedStreamColumnOf(StreamNullable(o.decoder))}
	c.streams[reflect.TypeFor[[]V]()] = erasedStreamEntry{column: erasedStreamColumnOf(StreamSlice(o.decoder))}
}

// WithStreamDecoder maps the struct's fields of type V, Nullable[V] and []V
// (a list, set or vector, whichever the column is) with decoder in
// StreamRowOf. V is inferred from decoder.
// Recognized built-in stream tags take precedence, including in generated
// mappings. Only StreamRowOf applies it.
func WithStreamDecoder[V any](decoder StreamDecoder[V]) StructOption {
	return streamCodecOption[V]{decoder: decoder}
}

// GeneratedStreamRow decodes streamed rows with the generated stream
// columns, with opts applied to them, or reflects R when columns is nil.
func generatedStreamRow[R any](columns []StreamOutputField[R], opts ...StructOption) (StreamRowDecoder[R], error) {
	config, err := structOptions[R](targetStream, opts)
	if err != nil {
		return nil, err
	}
	fields := structFields[R]{StreamColumns: columns}
	if fields.StreamColumns == nil {
		return reflectStreamRow(config)
	}
	fields, err = generatedStructOptions(fields, config)
	if err != nil {
		return nil, err
	}
	return structStreamRow(fields.StreamColumns, config), nil
}

// ReflectStreamRow maps the fields of struct R by reflection with the
// streaming decoders: every column needs a field, and a field whose column is
// not selected is reset by its decoder and otherwise left alone. Decoders
// reuse the storage of []byte and slice fields across rows.
func reflectStreamRow[R any](config structConfig[R]) (StreamRowDecoder[R], error) {
	builtins := streamBuiltinFields[R]()
	var columns []StreamOutputField[R]
	err := eachStructField(reflect.TypeFor[R](), func(sf reflect.StructField, tag structmap.FieldTag) error {
		entry, err := resolveStreamField(builtins, config.streams, sf.Type, tag.Stream, "cql.WithStreamDecoder")
		if err == nil {
			column := entry.column(tag.Column, sf.Offset)
			if tag.NullZero {
				column = nullZeroStream(column)
			}
			columns = append(columns, column)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return structStreamRow(columns, config), nil
}

func structStreamRow[R any](fields []StreamOutputField[R], config structConfig[R]) StreamRowDecoder[R] {
	return streamRowsCodec[R]{fields: streamDeclarations(fields), structMapping: true, ignoreExtra: config.udt.allowExtra, nullZero: config.nullZero}
}
