// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/structmap"
	"github.com/scylladb/gocql/v2/internal/syncx"
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
	// Every entry instantiates field accessors for P, so the table grows the
	// code of each mapped struct type. Go maps are the exception: their key and
	// value pairs multiply, so goMapFields shares one accessor per struct type
	// among all of them.
	var fields []builtinField[P]
	add := func(entries ...builtinField[P]) { fields = append(fields, entries...) }
	add(builtin[P](BoolCodec, "bool", "BoolCodec", true)...)
	add(builtin[P](Int8Codec, "int8", "Int8Codec", true)...)
	add(builtin[P](Int16Codec, "int16", "Int16Codec", true)...)
	add(builtin[P](Int32Codec, "int32", "Int32Codec", true)...)
	add(builtin[P](Int64Codec, "int64", "Int64Codec", true)...)
	add(builtin[P](IntCodec, "int", "IntCodec", true)...)
	add(builtin[P](Float32Codec, "float32", "Float32Codec", true)...)
	add(builtin[P](Float64Codec, "float64", "Float64Codec", true)...)
	// ASCIIStringCodec names StringCodec, which maps ascii too, in a tag.
	add(builtin[P](StringCodec, "string", "StringCodec", true, "ASCIIStringCodec")...)
	add(builtin[P](TextCodec, "cql.Text", "TextCodec", true)...)
	add(builtin[P](ByteSliceCodec, "[]byte", "ByteSliceCodec", true)...)
	add(builtin[P](BytesCodec, "cql.Bytes", "BytesCodec", true)...)
	add(builtin[P](UUIDCodec, "cql.UUID", "UUIDCodec", true)...)
	add(builtin[P](TimeUUIDCodec, "cql.TimeUUID", "TimeUUIDCodec", true)...)
	add(builtin[P](InetCodec, "netip.Addr", "InetCodec", true)...)
	add(builtin[P](VarintCodec, "cql.Varint", "VarintCodec", true)...)
	add(builtin[P](VarintBigIntCodec, "*big.Int", "VarintBigIntCodec", true)...)
	add(builtin[P](DecimalCodec, "cql.Decimal", "DecimalCodec", true)...)
	add(builtin[P](DateCodec, "cql.Date", "DateCodec", true)...)
	add(builtin[P](TimeOfDayCodec, "cql.TimeOfDay", "TimeOfDayCodec", true)...)
	add(builtin[P](TimestampCodec, "cql.Timestamp", "TimestampCodec", true)...)
	add(builtin[P](DurationCodec, "cql.Duration", "DurationCodec", true)...)
	add(builtin[P](TimestampTimeCodec, "time.Time", "TimestampTimeCodec", true)...)
	add(goMapFields[P]()...)
	return fields
}

// builtin returns the entries for V, Nullable[V], *V (unless V is a
// pointer), BindValue[V] (parameters only), and []V as whichever of a list,
// a set or a vector the column is (the default), or as a list
// (`codec=ListCodec`), a set (`codec=SetCodec`) or a vector
// (`codec=VectorCodec`) of c. name is c's exported name, which a `codec=`
// tag selects it by, as it does each of aliases.
func builtin[P, V any](c Codec[V], typeSrc, name string, def bool, aliases ...string) []builtinField[P] {
	names := append([]string{name}, aliases...)
	plain := builtinField[P]{
		BuiltinFieldType: structmap.BuiltinFieldType{
			Type: reflect.TypeFor[V](), TypeSrc: typeSrc,
			EncoderSrc: "cql." + name, DecoderSrc: "cql." + name,
			Names: names, Default: def,
		},
		param:  fieldParam[P](Encoder[V](c)),
		column: fieldColumn[P](Decoder[V](c)),
	}
	nullable := builtinField[P]{
		BuiltinFieldType: structmap.BuiltinFieldType{
			Type: reflect.TypeFor[Nullable[V]](), TypeSrc: "cql.Nullable[" + typeSrc + "]",
			EncoderSrc: "cql.NullableEncoder(cql." + name + ")", DecoderSrc: "cql.NullableDecoder(cql." + name + ")",
			Names: names, Default: def,
		},
		param:  fieldParam[P](NullableEncoder[V](c)),
		column: fieldColumn[P](NullableDecoder[V](c)),
	}
	bind := builtinField[P]{
		BuiltinFieldType: structmap.BuiltinFieldType{
			Type: reflect.TypeFor[BindValue[V]](), TypeSrc: "cql.BindValue[" + typeSrc + "]",
			EncoderSrc: "cql.BindEncoder(cql." + name + ")",
			Names:      names, Default: def,
		},
		param: fieldParam[P](BindEncoder[V](c)),
	}
	collection := func(codec Codec[[]V], src string, names []string, def bool) builtinField[P] {
		return builtinField[P]{
			BuiltinFieldType: structmap.BuiltinFieldType{
				Type: reflect.TypeFor[[]V](), TypeSrc: "[]" + typeSrc,
				EncoderSrc: src, DecoderSrc: src, Names: names, Default: def,
			},
			param:  fieldParam[P](Encoder[[]V](codec)),
			column: fieldColumn[P](Decoder[[]V](codec)),
		}
	}
	fields := []builtinField[P]{plain, nullable, bind,
		collection(SliceCodec(c), "cql.SliceCodec(cql."+name+")", names, def)}
	if reflect.TypeFor[V]().Kind() != reflect.Pointer {
		fields = append(fields, builtinField[P]{
			BuiltinFieldType: structmap.BuiltinFieldType{
				Type: reflect.TypeFor[*V](), TypeSrc: "*" + typeSrc,
				EncoderSrc: "cql.PointerCodec(cql." + name + ")", DecoderSrc: "cql.PointerCodec(cql." + name + ")",
				Names: names, Default: def,
			},
			param:  fieldParam[P](Encoder[*V](PointerCodec(c))),
			column: fieldColumn[P](Decoder[*V](PointerCodec(c))),
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

// unsafe: in fieldParam, fieldColumn and fieldStreamColumn, offset is the
// reflect offset of a field of type exactly V, or of a named type whose
// underlying type is V and so has V's layout (resolveField, underlyingOf;
// eachStructField sums the offsets of flattened value embeds; pointer embeds
// are rejected), so it
// stays inside the struct the arithmetic starts from and the result is a
// typed pointer the GC sees.

func fieldParam[P, V any](enc Encoder[V]) func(string, uintptr) InputField[P] {
	return func(name string, offset uintptr) InputField[P] {
		return Param(name, enc, func(v P) V { return *(*V)(unsafe.Add(unsafe.Pointer(&v), offset)) })
	}
}

func fieldColumn[R, V any](dec Decoder[V]) func(string, uintptr) OutputField[R] {
	return func(name string, offset uintptr) OutputField[R] {
		return Column(name, dec, func(r *R) *V { return (*V)(unsafe.Add(unsafe.Pointer(r), offset)) })
	}
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

// Options name a field type V but not the struct, so they cannot
// instantiate the struct's field accessors. They build them for erasedBase
// instead, a stand-in whose pointer is the struct's address, and
// erasedParam, erasedColumn and erasedStreamColumn adapt them to the struct.
//
// unsafe: an erased accessor adds the field's reflect offset to the
// struct's address, as fieldParam does; the struct is P or R, whose field at
// that offset has type exactly V (resolveField matches by reflect.Type).
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

// erasedParam binds a parameter of P with a field built for erasedBase. A
// parameter struct arrives by value, so its address would escape to the
// heap on every bind; encode copies it into a pooled slot instead.
type erasedParam[P any] struct {
	inner InputField[*erasedBase]
	slots *syncx.Pool[*P]
}

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
	slot := f.slots.Get()
	*slot = value
	err := f.inner.encode(w, (*erasedBase)(unsafe.Pointer(slot)))
	var zero P
	*slot = zero
	f.slots.Put(slot)
	return err
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

// erasedStreamColumn decodes a streamed column of R with a field built for
// erasedBase.
func erasedStreamColumn[R any](field StreamOutputField[erasedBase]) StreamOutputField[R] {
	inner := field.streamDeclaration()
	return &streamOutputField[R]{loc: inner.loc,
		resetMissing: func(row *R) { inner.resetMissing(erased(row)) },
		compile: func(typ DataType) (streamFieldPlan[R], error) {
			plan, err := inner.compile(typ)
			if err != nil {
				return streamFieldPlan[R]{}, err
			}
			return streamFieldPlan[R]{
				reset:  func(row *R) { plan.reset(erased(row)) },
				decode: func(r *StreamRowReader, row *R) error { return plan.decode(r, erased(row)) },
			}, nil
		},
	}
}

// bindEntry returns e's fields for struct P.
func bindEntry[P any](e erasedEntry) builtinField[P] {
	var entry builtinField[P]
	if e.param != nil {
		slots := syncx.NewPool(func() *P { return new(P) })
		entry.param = func(name string, offset uintptr) InputField[P] {
			return erasedParam[P]{inner: e.param(name, offset), slots: slots}
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
	for _, entry := range builtins {
		meta := entry.meta()
		if meta.Type == typ {
			for _, candidate := range meta.Names {
				if candidate == name {
					return true
				}
			}
		}
	}
	return false
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
		// (see fieldParam), and old keeps the previous backing arrays alive.
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
	var candidates []structmap.BuiltinFieldType
	var entries []E
	for _, b := range builtins {
		if b.meta().Type == typ {
			candidates = append(candidates, b.meta())
			entries = append(entries, b)
		}
	}
	if len(candidates) == 0 && typ.Name() != "" {
		// A named type maps like its underlying type when that has a
		// built-in codec. Its fields read the field as the underlying
		// type, whose memory layout is the same.
		for _, b := range builtins {
			if underlyingOf(typ, b.meta().Type) {
				candidates = append(candidates, b.meta())
				entries = append(entries, b)
			}
		}
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

// underlyingOf reports whether u is the underlying type of the named type t:
// u is predeclared or unnamed, of t's kind, and t converts to it, which for
// types of one kind needs identical underlying types.
func underlyingOf(t, u reflect.Type) bool {
	return u.PkgPath() == "" && u.Kind() == t.Kind() && t.ConvertibleTo(u)
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
	var fields []streamBuiltinField[P]
	add := func(entries ...streamBuiltinField[P]) { fields = append(fields, entries...) }
	add(streamBuiltin[P](StreamBool, "bool", "Bool", true)...)
	add(streamBuiltin[P](StreamInt8, "int8", "Int8", true)...)
	add(streamBuiltin[P](StreamInt16, "int16", "Int16", true)...)
	add(streamBuiltin[P](StreamInt32, "int32", "Int32", true)...)
	add(streamBuiltin[P](StreamInt64, "int64", "Int64", true)...)
	add(streamBuiltin[P](StreamFloat32, "float32", "Float32", true)...)
	add(streamBuiltin[P](StreamFloat64, "float64", "Float64", true)...)
	add(streamBuiltin[P](StreamBlob, "[]byte", "Blob", true)...)
	// ASCII names StreamText, which decodes ascii too, in a tag.
	add(streamBuiltin[P](StreamText, "[]byte", "Text", false, "ASCII")...)
	add(streamBuiltin[P](StreamVarint, "[]byte", "Varint", false)...)
	add(streamBuiltin[P](StreamUUID, "cql.UUID", "UUID", true)...)
	add(streamBuiltin[P](StreamTimeUUID, "cql.TimeUUID", "TimeUUID", true)...)
	add(streamBuiltin[P](StreamInet, "netip.Addr", "Inet", true)...)
	add(streamBuiltin[P](StreamDecimal, "cql.DecimalParts", "Decimal", true)...)
	add(streamBuiltin[P](StreamDate, "cql.Date", "Date", true)...)
	add(streamBuiltin[P](StreamTime, "cql.TimeOfDay", "Time", true)...)
	add(streamBuiltin[P](StreamTimestamp, "cql.Timestamp", "Timestamp", true)...)
	add(streamBuiltin[P](StreamDuration, "cql.Duration", "Duration", true)...)
	return fields
}

// streamBuiltin returns the entries for V, Nullable[V], and []V as whichever
// of a list, a set or a vector the column is (the default), or as a list
// (`stream=List`), a set (`stream=Set`) or a vector (`stream=Vector`). For a
// decoder that is not its type's default, as Text is not for []byte, the tag
// that names it, or one of aliases, also selects the slice of it, and there
// is no list, set or vector entry: those tags already select the default
// decoder's.
func streamBuiltin[P, V any](dec StreamDecoder[V], typeSrc, name string, def bool, aliases ...string) []streamBuiltinField[P] {
	names := append([]string{name}, aliases...)
	entry := func(typ reflect.Type, typeSrc, decSrc string, names []string, def bool, column func(string, uintptr) StreamOutputField[P]) streamBuiltinField[P] {
		return streamBuiltinField[P]{column: column, BuiltinFieldType: structmap.BuiltinFieldType{
			Type: typ, TypeSrc: typeSrc, DecoderSrc: decSrc, Names: names, Default: def,
		}}
	}
	src := "cql.Stream" + name
	plain := []streamBuiltinField[P]{
		entry(reflect.TypeFor[V](), typeSrc, src, names, def, fieldStreamColumn[P](dec)),
		entry(reflect.TypeFor[Nullable[V]](), "cql.Nullable["+typeSrc+"]", "cql.StreamNullable("+src+")",
			names, def, fieldStreamColumn[P](StreamNullable(dec))),
	}
	if !def {
		return append(plain, entry(reflect.TypeFor[[]V](), "[]"+typeSrc, "cql.StreamSlice("+src+")",
			names, false, fieldStreamColumn[P](StreamSlice(dec))))
	}
	return append(plain, []streamBuiltinField[P]{
		// A slice takes its element's default decoder.
		entry(reflect.TypeFor[[]V](), "[]"+typeSrc, "cql.StreamSlice("+src+")",
			nil, def, fieldStreamColumn[P](StreamSlice(dec))),
		entry(reflect.TypeFor[[]V](), "[]"+typeSrc, "cql.StreamList("+src+")",
			[]string{"List"}, false, fieldStreamColumn[P](StreamList(dec))),
		entry(reflect.TypeFor[[]V](), "[]"+typeSrc, "cql.StreamSet("+src+")",
			[]string{"Set"}, false, fieldStreamColumn[P](StreamSet(dec))),
		entry(reflect.TypeFor[[]V](), "[]"+typeSrc, "cql.StreamVector("+src+")",
			[]string{"Vector"}, false, fieldStreamColumn[P](StreamVector(dec))),
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

func fieldStreamColumn[R, V any](dec StreamDecoder[V]) func(string, uintptr) StreamOutputField[R] {
	return func(name string, offset uintptr) StreamOutputField[R] {
		// unsafe: as fieldParam.
		return StreamColumn(name, dec, func(r *R) *V { return (*V)(unsafe.Add(unsafe.Pointer(r), offset)) })
	}
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
