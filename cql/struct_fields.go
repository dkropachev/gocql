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
// the tag grammar and type table below, as a CQLFields method of the struct.

type builtinField[P any] struct {
	param  func(name string, offset uintptr) InputField[P]
	column func(name string, offset uintptr) OutputField[P]
	structmap.BuiltinFieldType
}

// builtinFields returns the built-in field types, each with its Nullable form.
func builtinFields[P any]() []builtinField[P] {
	erased := erasedBuiltins()
	fields := make([]builtinField[P], 0, len(erased)+len(goMapBuiltins()))
	for _, b := range erased {
		entry := bindEntry[P](b.erasedEntry)
		entry.BuiltinFieldType = b.BuiltinFieldType
		fields = append(fields, entry)
	}
	return append(fields, goMapFields[P]()...)
}

// erasedBuiltin is a built-in field type with its fields built for
// erasedBase, as an option's are.
type erasedBuiltin struct {
	erasedEntry
	structmap.BuiltinFieldType
}

// erasedBuiltins returns the built-in field types other than Go maps, with
// their fields built for erasedBase. The table does not depend on the mapped
// struct type, so each field type's codecs and accessors are instantiated
// once, not once per struct type: under coverage and race instrumentation,
// instantiating them per struct made compiling a package with many mapped
// structs take tens of gigabytes.
func erasedBuiltins() []erasedBuiltin {
	var fields []erasedBuiltin
	add := func(entries ...erasedBuiltin) { fields = append(fields, entries...) }
	add(builtin(BoolCodec, "bool", "BoolCodec", true)...)
	add(builtin(Int8Codec, "int8", "Int8Codec", true)...)
	add(builtin(Int16Codec, "int16", "Int16Codec", true)...)
	add(builtin(Int32Codec, "int32", "Int32Codec", true)...)
	add(builtin(Int64Codec, "int64", "Int64Codec", true)...)
	add(builtin(IntCodec, "int", "IntCodec", true)...)
	add(builtin(Float32Codec, "float32", "Float32Codec", true)...)
	add(builtin(Float64Codec, "float64", "Float64Codec", true)...)
	// ASCIIStringCodec names StringCodec, which maps ascii too, in a tag.
	add(builtin(StringCodec, "string", "StringCodec", true, "ASCIIStringCodec")...)
	add(builtin(TextCodec, "cql.Text", "TextCodec", true)...)
	add(builtin(ByteSliceCodec, "[]byte", "ByteSliceCodec", true)...)
	add(builtin(BytesCodec, "cql.Bytes", "BytesCodec", true)...)
	add(builtin(UUIDCodec, "cql.UUID", "UUIDCodec", true)...)
	add(builtin(TimeUUIDCodec, "cql.TimeUUID", "TimeUUIDCodec", true)...)
	add(builtin(InetCodec, "netip.Addr", "InetCodec", true)...)
	add(builtin(VarintCodec, "cql.Varint", "VarintCodec", true)...)
	add(builtin(VarintBigIntCodec, "*big.Int", "VarintBigIntCodec", true)...)
	add(builtin(DecimalCodec, "cql.Decimal", "DecimalCodec", true)...)
	add(builtin(DateCodec, "cql.Date", "DateCodec", true)...)
	add(builtin(TimeOfDayCodec, "cql.TimeOfDay", "TimeOfDayCodec", true)...)
	add(builtin(TimestampCodec, "cql.Timestamp", "TimestampCodec", true)...)
	add(builtin(DurationCodec, "cql.Duration", "DurationCodec", true)...)
	add(builtin(TimestampTimeCodec, "time.Time", "TimestampTimeCodec", true)...)
	return fields
}

// builtin returns the entries for V, Nullable[V], *V (unless V is a
// pointer), BindValue[V] (parameters only), and []V as whichever of a list,
// a set or a vector the column is (the default), or as a list
// (`codec=ListCodec`), a set (`codec=SetCodec`) or a vector
// (`codec=VectorCodec`) of c. name is c's exported name, which a `codec=`
// tag selects it by, as it does each of aliases.
func builtin[V any](c Codec[V], typeSrc, name string, def bool, aliases ...string) []erasedBuiltin {
	names := append([]string{name}, aliases...)
	plain := erasedBuiltin{
		BuiltinFieldType: structmap.BuiltinFieldType{
			Type: reflect.TypeFor[V](), TypeSrc: typeSrc,
			EncoderSrc: "cql." + name, DecoderSrc: "cql." + name,
			Names: names, Default: def,
		},
		erasedEntry: erasedEntry{param: erasedParamOf(Encoder[V](c)), column: erasedColumnOf(Decoder[V](c))},
	}
	nullable := erasedBuiltin{
		BuiltinFieldType: structmap.BuiltinFieldType{
			Type: reflect.TypeFor[Nullable[V]](), TypeSrc: "cql.Nullable[" + typeSrc + "]",
			EncoderSrc: "cql.NullableEncoder(cql." + name + ")", DecoderSrc: "cql.NullableDecoder(cql." + name + ")",
			Names: names, Default: def,
		},
		erasedEntry: erasedEntry{param: erasedParamOf(NullableEncoder[V](c)), column: erasedColumnOf(NullableDecoder[V](c))},
	}
	bind := erasedBuiltin{
		BuiltinFieldType: structmap.BuiltinFieldType{
			Type: reflect.TypeFor[BindValue[V]](), TypeSrc: "cql.BindValue[" + typeSrc + "]",
			EncoderSrc: "cql.BindEncoder(cql." + name + ")",
			Names:      names, Default: def,
		},
		erasedEntry: erasedEntry{param: erasedParamOf(BindEncoder[V](c))},
	}
	collection := func(codec Codec[[]V], src string, names []string, def bool) erasedBuiltin {
		return erasedBuiltin{
			BuiltinFieldType: structmap.BuiltinFieldType{
				Type: reflect.TypeFor[[]V](), TypeSrc: "[]" + typeSrc,
				EncoderSrc: src, DecoderSrc: src, Names: names, Default: def,
			},
			erasedEntry: erasedEntry{param: erasedParamOf(Encoder[[]V](codec)), column: erasedColumnOf(Decoder[[]V](codec))},
		}
	}
	fields := []erasedBuiltin{plain, nullable, bind,
		collection(SliceCodec(c), "cql.SliceCodec(cql."+name+")", names, def)}
	if reflect.TypeFor[V]().Kind() != reflect.Pointer {
		fields = append(fields, erasedBuiltin{
			BuiltinFieldType: structmap.BuiltinFieldType{
				Type: reflect.TypeFor[*V](), TypeSrc: "*" + typeSrc,
				EncoderSrc: "cql.PointerCodec(cql." + name + ")", DecoderSrc: "cql.PointerCodec(cql." + name + ")",
				Names: names, Default: def,
			},
			erasedEntry: erasedEntry{param: erasedParamOf(Encoder[*V](PointerCodec(c))), column: erasedColumnOf(Decoder[*V](PointerCodec(c)))},
		})
	}
	if def {
		fields = append(fields, collection(ListCodec(c), "cql.ListCodec(cql."+name+")", []string{"ListCodec"}, false))
		fields = append(fields, collection(SetCodec(c), "cql.SetCodec(cql."+name+")", []string{"SetCodec"}, false))
		fields = append(fields, collection(VectorCodec(c), "cql.VectorCodec(cql."+name+")", []string{"VectorCodec"}, false))
	}
	return fields
}

// BuiltinFieldTypes returns the Go field types mapped without configuration.
func builtinFieldTypes() []structmap.BuiltinFieldType {
	fields := builtinFields[struct{}]()
	types := make([]structmap.BuiltinFieldType, len(fields))
	for i, f := range fields {
		types[i] = f.BuiltinFieldType
	}
	return types
}

// StructOption configures a struct mapping: ParamsOf, RowOf, StreamRowOf,
// UDTCodecOf or TupleCodecOf. Each option applies to some of them, and the others reject it
// with a ValidationError rather than ignore it:
//
//   - WithCodec: ParamsOf, RowOf, UDTCodecOf and TupleCodecOf.
//   - WithEncoder: ParamsOf.
//   - ReuseBuffers: RowOf.
//   - IgnoreExtraColumns: RowOf, StreamRowOf and UDTCodecOf.
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
	default:
		return "UDTCodecOf"
	}
}

// structOptionSet is the options of one mapping, before they are bound to
// its struct type.
type structOptionSet struct {
	codecs       map[reflect.Type]erasedEntry
	streams      map[reflect.Type]erasedStreamEntry
	reuseBuffers bool
	ignoreExtra  bool
}

type structConfig[P any] struct {
	codecs       map[reflect.Type]builtinField[P]
	streams      map[reflect.Type]streamBuiltinField[P]
	reuseBuffers bool
	ignoreExtra  bool
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
// the type's underlying type (resolveField, sameLayout). eachStructField
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
	return paramOp[P]{field: f, get: op.get, width: op.width, columnKind: op.columnKind, alt: op.alt, kind: op.kind, erased: true}, true
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
	out := rowOp[R]{access: op.access, width: op.width, columnKind: op.columnKind, alt: op.alt, kind: op.kind, compiled: op.compiled}
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

// bindEntry returns e's fields for struct P.
func bindEntry[P any](e erasedEntry) builtinField[P] {
	var entry builtinField[P]
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
	plain, nullable := erasedEntry{param: erasedParamOf(o.enc)}, erasedEntry{param: erasedParamOf(NullableEncoder(o.enc))}
	slice := erasedEntry{param: erasedParamOf(Encoder[[]V](sequenceEncoder[V]{kind: anySequence, element: o.enc}))}
	if o.codec != nil {
		plain.column, nullable.column = erasedColumnOf(Decoder[V](o.codec)), erasedColumnOf(NullableDecoder(Decoder[V](o.codec)))
		slice.column = erasedColumnOf(Decoder[[]V](SliceCodec(o.codec)))
		pointer := PointerCodec(o.codec)
		c.codecs[reflect.TypeFor[*V]()] = erasedEntry{param: erasedParamOf(Encoder[*V](pointer)), column: erasedColumnOf(Decoder[*V](pointer))}
		mapValues(c, o.codec)
	}
	c.codecs[reflect.TypeFor[V]()] = plain
	c.codecs[reflect.TypeFor[Nullable[V]]()] = nullable
	c.codecs[reflect.TypeFor[[]V]()] = slice
	c.codecs[reflect.TypeFor[BindValue[V]]()] = erasedEntry{param: erasedParamOf(BindEncoder(o.enc))}
}

// mapValues maps map[K]V fields with GoMapCodec for each built-in map key
// type K, as the built-in map fields do.
func mapValues[V any](c *structOptionSet, value Codec[V]) {
	mapValue(c, BoolCodec, value)
	mapValue(c, Int8Codec, value)
	mapValue(c, Int16Codec, value)
	mapValue(c, Int32Codec, value)
	mapValue(c, Int64Codec, value)
	mapValue(c, IntCodec, value)
	mapValue(c, Float32Codec, value)
	mapValue(c, Float64Codec, value)
	mapValue(c, StringCodec, value)
	mapValue(c, UUIDCodec, value)
	mapValue(c, TimeUUIDCodec, value)
	mapValue(c, InetCodec, value)
	mapValue(c, DateCodec, value)
	mapValue(c, TimeOfDayCodec, value)
	mapValue(c, TimestampCodec, value)
}

func mapValue[K comparable, V any](c *structOptionSet, key Codec[K], value Codec[V]) {
	m := GoMapCodec(key, value)
	c.codecs[reflect.TypeFor[map[K]V]()] = erasedEntry{param: erasedParamOf(Encoder[map[K]V](m)), column: erasedColumnOf(Decoder[map[K]V](m))}
}

// WithCodec maps the struct's fields of type V, Nullable[V], *V, []V (a
// list, set or vector), map[K]V for a built-in map key type K, and
// BindValue[V] (a parameter only) with codec: a type with no built-in codec,
// such as a UDT, or a replacement for V's default. V is inferred from codec,
// as in WithCodec(statusCodec). Recognized built-in codec tags take
// precedence. The option also applies to generated mappings; other
// generated custom-expression codecs remain defaults. ParamsOf, RowOf,
// UDTCodecOf and TupleCodecOf apply it; StreamRowOf decodes with stream
// decoders, so it rejects it: use WithStreamDecoder.
func WithCodec[V any](codec Codec[V]) StructOption {
	return codecOption[V]{enc: codec, codec: codec, name: "WithCodec"}
}

// WithEncoder maps the parameter fields of type V, Nullable[V], []V and
// BindValue[V] with encoder, for a type that is only ever bound, as
// WithCodec does for both directions. Only ParamsOf applies it.
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
func generatedRow[R any](fields structFields[R], target structTarget, opts ...StructOption) (RowDecoder[R], error) {
	config, err := structOptions[R](target, opts)
	if err != nil {
		return nil, err
	}
	if fields.Columns == nil {
		return reflectRow(config)
	}
	fields, err = generatedStructOptions(fields, config)
	if err != nil {
		return nil, err
	}
	reset, err := bufferReset[R](config.reuseBuffers)
	if err != nil {
		return nil, err
	}
	return structRow(fields.Columns, reset, config.ignoreExtra), nil
}

// generatedStructOptions replaces only fields selected by type options. The
// generated defaults may contain Go codec expressions that reflection cannot
// evaluate, so rebuilding the entire mapping reflectively would lose them.
// Field discovery and option resolution happen here, never during execution.
func generatedStructOptions[P any](fields structFields[P], config structConfig[P]) (structFields[P], error) {
	if len(config.codecs) == 0 && len(config.streams) == 0 {
		return fields, nil
	}
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
	var builtins []builtinField[P]
	var streamBuiltins []streamBuiltinField[P]
	if len(config.codecs) != 0 {
		builtins = builtinFields[P]()
	}
	if len(config.streams) != 0 {
		streamBuiltins = streamBuiltinFields[P]()
	}
	err := eachStructField(reflect.TypeFor[P](), func(sf reflect.StructField, tag structmap.FieldTag) error {
		if entry, ok := config.codecs[sf.Type]; ok && !tagSelectsBuiltin(builtins, sf.Type, tag.Codec) {
			if i, ok := params[tag.Column]; ok && entry.param != nil {
				fields.Params[i] = entry.param(tag.Column, sf.Offset)
			}
			if i, ok := columns[tag.Column]; ok && entry.column != nil {
				fields.Columns[i] = entry.column(tag.Column, sf.Offset)
			}
		}
		if entry, ok := config.streams[sf.Type]; ok && !tagSelectsBuiltin(streamBuiltins, sf.Type, tag.Stream) {
			if i, ok := streams[tag.Column]; ok {
				fields.StreamColumns[i] = entry.column(tag.Column, sf.Offset)
			}
		}
		return nil
	})
	return fields, err
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
		params[i] = f.entry.param(f.column, f.offset)
	}
	return structParams(params), nil
}

// ReflectRow maps the fields of struct R by reflection, once, here.
func reflectRow[R any](config structConfig[R]) (RowDecoder[R], error) {
	fields, err := reflectFields(config)
	if err != nil {
		return nil, err
	}
	columns := make([]OutputField[R], len(fields))
	for i, f := range fields {
		if f.entry.column == nil {
			return nil, fmt.Errorf("gocql: struct mapping of %s: field %s: type %s only binds parameters", reflect.TypeFor[R](), f.name, f.typ)
		}
		columns[i] = f.entry.column(f.column, f.offset)
	}
	reset, err := bufferReset[R](config.reuseBuffers)
	if err != nil {
		return nil, err
	}
	return structRow(columns, reset, config.ignoreExtra), nil
}

func structParams[P any](fields []InputField[P]) ParamEncoder[P] {
	return paramsCodec[P]{fields: append([]InputField[P](nil), fields...), optional: true}
}

func structRow[R any](fields []OutputField[R], reset func(*R), extra bool) RowDecoder[R] {
	return rowCodec[R]{fields: append([]OutputField[R](nil), fields...), optional: true, extra: extra, reset: reset}
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

type ignoreExtraColumnsOption struct{}

func (ignoreExtraColumnsOption) option() (string, structTarget) {
	return "IgnoreExtraColumns", targetRows | targetStream | targetUDT
}
func (ignoreExtraColumnsOption) apply(c *structOptionSet) { c.ignoreExtra = true }

// IgnoreExtraColumns makes RowOf and StreamRowOf skip a result column that no
// field maps, so that SELECT * keeps working after ALTER TABLE ADD. Without
// it, every result column needs a field, which catches a misspelled tag.
// UDTCodecOf takes it as UDTOptions.AllowExtraDatabaseFields.
func IgnoreExtraColumns() StructOption { return ignoreExtraColumnsOption{} }

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
	typ          reflect.Type
	column, name string
	entry        builtinField[P]
	offset       uintptr
}

func reflectFields[P any](config structConfig[P]) ([]reflectedField[P], error) {
	builtins := builtinFields[P]()
	var fields []reflectedField[P]
	err := eachStructField(reflect.TypeFor[P](), func(sf reflect.StructField, tag structmap.FieldTag) error {
		entry, err := resolveField(builtins, config.codecs, sf.Type, tag.Codec, "cql.WithCodec")
		if err == nil {
			fields = append(fields, reflectedField[P]{entry: entry, column: tag.Column, name: sf.Name, typ: sf.Type, offset: sf.Offset})
		}
		return err
	})
	return fields, err
}

// structOptions binds opts to struct P for the target constructor, which
// rejects an option it does not apply.
func structOptions[P any](target structTarget, opts []StructOption) (structConfig[P], error) {
	t := reflect.TypeFor[P]()
	config := structConfig[P]{codecs: map[reflect.Type]builtinField[P]{}, streams: map[reflect.Type]streamBuiltinField[P]{}}
	if t.Kind() != reflect.Struct {
		return config, typeValidation("struct", "%s is not a struct", t)
	}
	set := structOptionSet{codecs: map[reflect.Type]erasedEntry{}, streams: map[reflect.Type]erasedStreamEntry{}}
	for _, opt := range opts {
		if opt == nil {
			return config, typeValidation("StructOption", "nil option for %s of %s", target, t)
		}
		if name, targets := opt.option(); targets&target == 0 {
			return config, typeValidation(name, "%s does not apply it", target)
		}
		opt.apply(&set)
	}
	for typ, e := range set.codecs {
		config.codecs[typ] = bindEntry[P](e)
	}
	for typ, e := range set.streams {
		column := e.column
		config.streams[typ] = streamBuiltinField[P]{column: func(name string, offset uintptr) StreamOutputField[P] {
			return erasedStreamColumn[P](column(name, offset))
		}}
	}
	config.reuseBuffers, config.ignoreExtra = set.reuseBuffers, set.ignoreExtra
	return config, nil
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

// resolveField picks a field's codec: a built-in named by its tag, then an
// option for its type, then the built-in default.
func resolveField[E interface {
	meta() structmap.BuiltinFieldType
}](builtins []E, options map[reflect.Type]E, typ reflect.Type, codec, option string) (E, error) {
	var zero E
	// A named type maps like its underlying type when that has a built-in
	// codec, and so do slices, pointers, maps and Nullable or BindValue
	// wrappers of it. Their fields read the field as the built-in type,
	// whose memory layout is the same.
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

func (f builtinField[P]) meta() structmap.BuiltinFieldType { return f.BuiltinFieldType }

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
	return structStreamRow(fields.StreamColumns, config.ignoreExtra), nil
}

// ReflectStreamRow maps the fields of struct R by reflection with the
// streaming decoders: every column needs a field, and a field whose column is
// not selected is reset by its decoder and otherwise left alone. Decoders
// reuse the storage of []byte and slice fields across rows.
func reflectStreamRow[R any](config structConfig[R]) (StreamRowDecoder[R], error) {
	builtins := streamBuiltinFields[R]()
	var columns []StreamOutputField[R]
	err := eachStructField(reflect.TypeFor[R](), func(sf reflect.StructField, tag structmap.FieldTag) error {
		entry, err := resolveField(builtins, config.streams, sf.Type, tag.Stream, "cql.WithStreamDecoder")
		if err == nil {
			columns = append(columns, entry.column(tag.Column, sf.Offset))
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return structStreamRow(columns, config.ignoreExtra), nil
}

func structStreamRow[R any](fields []StreamOutputField[R], extra bool) StreamRowDecoder[R] {
	return streamRowsCodec[R]{fields: streamDeclarations(fields), structMapping: true, ignoreExtra: extra}
}
