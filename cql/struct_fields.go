// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"fmt"
	"reflect"
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
	add(builtin[P](Float32Codec, "float32", "Float32Codec", true)...)
	add(builtin[P](Float64Codec, "float64", "Float64Codec", true)...)
	add(builtin[P](StringCodec, "string", "StringCodec", true)...)
	add(builtin[P](ASCIIStringCodec, "string", "ASCIIStringCodec", false)...)
	add(builtin[P](ByteSliceCodec, "[]byte", "ByteSliceCodec", true)...)
	add(builtin[P](BlobCodec, "cql.Bytes", "BlobCodec", true)...)
	add(builtin[P](UUIDCodec, "cql.UUID", "UUIDCodec", true)...)
	add(builtin[P](TimeUUIDCodec, "cql.TimeUUID", "TimeUUIDCodec", true)...)
	add(builtin[P](InetCodec, "netip.Addr", "InetCodec", true)...)
	add(builtin[P](CounterCodec, "cql.Counter", "CounterCodec", true)...)
	add(builtin[P](VarintCodec, "cql.Varint", "VarintCodec", true)...)
	add(builtin[P](DecimalCodec, "cql.Decimal", "DecimalCodec", true)...)
	add(builtin[P](DateCodec, "cql.Date", "DateCodec", true)...)
	add(builtin[P](TimeCodec, "cql.TimeOfDay", "TimeCodec", true)...)
	add(builtin[P](TimestampCodec, "cql.Timestamp", "TimestampCodec", true)...)
	add(builtin[P](DurationCodec, "cql.Duration", "DurationCodec", true)...)
	add(goMapFields[P]()...)
	return fields
}

// builtin returns the entries for V, Nullable[V], and []V as a list (the
// default), a set (`codec=SetCodec`) or a vector (`codec=VectorCodec`) of c.
// name is c's exported name, which a `codec=` tag selects it by.
func builtin[P, V any](c Codec[V], typeSrc, name string, def bool) []builtinField[P] {
	names := []string{name}
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
	fields := []builtinField[P]{plain, nullable,
		collection(ListCodec(c), "cql.ListCodec(cql."+name+")", names, def)}
	if def {
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
// reflect offset of a field of type exactly V (eachStructField sums the
// offsets of flattened value embeds; pointer embeds are rejected), so it
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

// StructOption configures ParamsOf, RowOf and cql.StreamRowOf. Each option
// affects only some of them, and the others ignore it:
//
//   - WithCodec: ParamsOf and RowOf. cql.StreamRowOf decodes with stream
//     decoders, so it ignores WithCodec; use cql.WithStreamCodec.
//   - cql.WithStreamCodec: cql.StreamRowOf only.
//   - ReuseBuffers: RowOf only. ParamsOf has no destination, and
//     cql.StreamRowOf always reuses its slice storage.
//
// Each call builds an independent mapping; options never change a generated
// mapping in place.
type StructOption[P any] interface{ apply(*structConfig[P]) }

type structConfig[P any] struct {
	codecs       map[reflect.Type]builtinField[P]
	streams      map[reflect.Type]streamBuiltinField[P]
	reuseBuffers bool
}

type codecOption[P, V any] struct{ codec Codec[V] }

func (o codecOption[P, V]) apply(c *structConfig[P]) {
	c.codecs[reflect.TypeFor[V]()] = builtinField[P]{param: fieldParam[P](Encoder[V](o.codec)), column: fieldColumn[P](Decoder[V](o.codec))}
	// A Codec is no streaming decoder; StreamRowOf needs WithStreamCodec.
	c.codecs[reflect.TypeFor[Nullable[V]]()] = builtinField[P]{
		param:  fieldParam[P](NullableEncoder[V](o.codec)),
		column: fieldColumn[P](NullableDecoder[V](o.codec)),
	}
}

// WithCodec maps P's fields of type V, and of Nullable[V], with c: a type
// with no built-in codec, such as a UDT, or a replacement for V's default.
// Recognized built-in codec tags take precedence. The option also applies to
// generated mappings; other generated custom-expression codecs remain defaults.
// It affects ParamsOf and RowOf; cql.StreamRowOf ignores it.
func WithCodec[P, V any](codec Codec[V]) StructOption[P] { return codecOption[P, V]{codec: codec} }

// structFields is a struct's buffered mapping as cmd/cqlgen generates it
// (StructFields), or as cql.StreamRowOf generates it (StreamColumns).
type structFields[P any] struct {
	Params        []InputField[P]
	Columns       []OutputField[P]
	StreamColumns []StreamOutputField[P]
}

// GeneratedParams binds parameters from the generated mapping fields, with
// opts applied to it, or reflects P when fields has no buffered mapping.
func generatedParams[P any](fields structFields[P], opts ...StructOption[P]) (ParamEncoder[P], error) {
	if fields.Params == nil {
		return reflectParams(opts...)
	}
	config, err := structOptions(reflect.TypeFor[P](), opts)
	if err != nil {
		return nil, err
	}
	fields, err = generatedStructOptions(fields, config)
	if err != nil {
		return nil, err
	}
	return structParams(fields.Params), nil
}

// GeneratedRow decodes rows with the generated mapping fields, with opts
// applied to it, or reflects R when fields has no buffered mapping.
func generatedRow[R any](fields structFields[R], opts ...StructOption[R]) (RowDecoder[R], error) {
	if fields.Columns == nil {
		return reflectRow(opts...)
	}
	config, err := structOptions(reflect.TypeFor[R](), opts)
	if err != nil {
		return nil, err
	}
	fields, err = generatedStructOptions(fields, config)
	if err != nil {
		return nil, err
	}
	reset, err := bufferReset[R](config.reuseBuffers)
	if err != nil {
		return nil, err
	}
	return structRow(fields.Columns, reset), nil
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
			if i, ok := params[tag.Column]; ok {
				fields.Params[i] = entry.param(tag.Column, sf.Offset)
			}
			if i, ok := columns[tag.Column]; ok {
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
func reflectParams[P any](opts ...StructOption[P]) (ParamEncoder[P], error) {
	fields, err := reflectFields(opts)
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
func reflectRow[R any](opts ...StructOption[R]) (RowDecoder[R], error) {
	fields, err := reflectFields(opts)
	if err != nil {
		return nil, err
	}
	columns := make([]OutputField[R], len(fields))
	for i, f := range fields {
		columns[i] = f.entry.column(f.column, f.offset)
	}
	config, _ := structOptions(reflect.TypeFor[R](), opts) // reflectFields checked opts
	reset, err := bufferReset[R](config.reuseBuffers)
	if err != nil {
		return nil, err
	}
	return structRow(columns, reset), nil
}

func structParams[P any](fields []InputField[P]) ParamEncoder[P] {
	return paramsCodec[P]{fields: append([]InputField[P](nil), fields...), optional: true}
}

func structRow[R any](fields []OutputField[R], reset func(*R)) RowDecoder[R] {
	return rowCodec[R]{fields: append([]OutputField[R](nil), fields...), optional: true, reset: reset}
}

type reuseBuffersOption[P any] struct{}

func (reuseBuffersOption[P]) apply(c *structConfig[P]) { c.reuseBuffers = true }

// ReuseBuffers makes RowOf keep the storage of P's slice fields, such as
// []byte and lists, between rows decoded into the same destination, as
// RowInto does. The next row overwrites that storage, so copy a field that
// must outlive its row. Other fields, maps included, are zeroed as usual.
// It affects RowOf only: ParamsOf has no destination, and cql.StreamRowOf
// always reuses slice storage.
func ReuseBuffers[P any]() StructOption[P] { return reuseBuffersOption[P]{} }

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
	column string
	entry  builtinField[P]
	offset uintptr
}

func reflectFields[P any](opts []StructOption[P]) ([]reflectedField[P], error) {
	t := reflect.TypeFor[P]()
	config, err := structOptions(t, opts)
	if err != nil {
		return nil, err
	}
	builtins := builtinFields[P]()
	var fields []reflectedField[P]
	err = eachStructField(t, func(sf reflect.StructField, tag structmap.FieldTag) error {
		entry, err := resolveField(builtins, config.codecs, sf.Type, tag.Codec, "cql.WithCodec")
		if err == nil {
			fields = append(fields, reflectedField[P]{entry: entry, column: tag.Column, offset: sf.Offset})
		}
		return err
	})
	return fields, err
}

func structOptions[P any](t reflect.Type, opts []StructOption[P]) (structConfig[P], error) {
	config := structConfig[P]{codecs: map[reflect.Type]builtinField[P]{}, streams: map[reflect.Type]streamBuiltinField[P]{}}
	if t.Kind() != reflect.Struct {
		return config, fmt.Errorf("gocql: struct mapping of %s: not a struct", t)
	}
	for _, opt := range opts {
		if opt == nil {
			return config, fmt.Errorf("gocql: struct mapping of %s: nil option", t)
		}
		opt.apply(&config)
	}
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
	add(streamBuiltin[P](StreamText, "[]byte", "Text", false)...)
	add(streamBuiltin[P](StreamASCII, "[]byte", "ASCII", false)...)
	add(streamBuiltin[P](StreamVarint, "[]byte", "Varint", false)...)
	add(streamBuiltin[P](StreamUUID, "cql.UUID", "UUID", true)...)
	add(streamBuiltin[P](StreamTimeUUID, "cql.TimeUUID", "TimeUUID", true)...)
	add(streamBuiltin[P](StreamInet, "netip.Addr", "Inet", true)...)
	add(streamBuiltin[P](StreamCounter, "cql.Counter", "Counter", true)...)
	add(streamBuiltin[P](StreamDecimal, "cql.DecimalParts", "Decimal", true)...)
	add(streamBuiltin[P](StreamDate, "cql.Date", "Date", true)...)
	add(streamBuiltin[P](StreamTime, "cql.TimeOfDay", "Time", true)...)
	add(streamBuiltin[P](StreamTimestamp, "cql.Timestamp", "Timestamp", true)...)
	add(streamBuiltin[P](StreamDuration, "cql.Duration", "Duration", true)...)
	return fields
}

// streamBuiltin returns the entries for V, Nullable[V], and []V as a list
// (the default), a set (`stream=Set`), or a vector (`stream=Vector`). For a
// decoder that is not its type's default, as Text is not for []byte, the tag
// that names it also selects the list of it, and there is no set or vector
// entry: "Set" and "Vector" already select those of the default decoder.
func streamBuiltin[P, V any](dec StreamDecoder[V], typeSrc, name string, def bool) []streamBuiltinField[P] {
	entry := func(typ reflect.Type, typeSrc, decSrc string, names []string, def bool, column func(string, uintptr) StreamOutputField[P]) streamBuiltinField[P] {
		return streamBuiltinField[P]{column: column, BuiltinFieldType: structmap.BuiltinFieldType{
			Type: typ, TypeSrc: typeSrc, DecoderSrc: decSrc, Names: names, Default: def,
		}}
	}
	src := "cql.Stream" + name
	plain := []streamBuiltinField[P]{
		entry(reflect.TypeFor[V](), typeSrc, src, []string{name}, def, fieldStreamColumn[P](dec)),
		entry(reflect.TypeFor[Nullable[V]](), "cql.Nullable["+typeSrc+"]", "cql.StreamNullable("+src+")",
			[]string{name}, def, fieldStreamColumn[P](StreamNullable(dec))),
	}
	if !def {
		return append(plain, entry(reflect.TypeFor[[]V](), "[]"+typeSrc, "cql.StreamList("+src+")",
			[]string{name}, false, fieldStreamColumn[P](StreamList(dec))))
	}
	return append(plain, []streamBuiltinField[P]{
		// A list or set takes its element's default decoder.
		entry(reflect.TypeFor[[]V](), "[]"+typeSrc, "cql.StreamList("+src+")",
			[]string{"List"}, def, fieldStreamColumn[P](StreamList(dec))),
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

type streamCodecOption[P, V any] struct{ decoder StreamDecoder[V] }

func (o streamCodecOption[P, V]) apply(c *structConfig[P]) {
	c.streams[reflect.TypeFor[V]()] = streamBuiltinField[P]{column: fieldStreamColumn[P](o.decoder)}
	c.streams[reflect.TypeFor[Nullable[V]]()] = streamBuiltinField[P]{column: fieldStreamColumn[P](StreamNullable(o.decoder))}
	c.streams[reflect.TypeFor[[]V]()] = streamBuiltinField[P]{column: fieldStreamColumn[P](StreamList(o.decoder))}
}

// WithStreamCodec maps P's fields of type V, cql.Nullable[V] and []V (as a list)
// with decoder in StreamRowOf. Recognized built-in stream tags take
// precedence, including in generated mappings. It affects StreamRowOf only;
// cql.ParamsOf and cql.RowOf ignore it.
func WithStreamCodec[P, V any](decoder StreamDecoder[V]) StructOption[P] {
	return streamCodecOption[P, V]{decoder: decoder}
}

// GeneratedStreamRow decodes streamed rows with the generated stream
// columns, with opts applied to them, or reflects R when columns is nil.
func generatedStreamRow[R any](columns []StreamOutputField[R], opts ...StructOption[R]) (StreamRowDecoder[R], error) {
	fields := structFields[R]{StreamColumns: columns}
	if fields.StreamColumns == nil {
		return reflectStreamRow(opts...)
	}
	config, err := structOptions(reflect.TypeFor[R](), opts)
	if err != nil {
		return nil, err
	}
	fields, err = generatedStructOptions(fields, config)
	if err != nil {
		return nil, err
	}
	return structStreamRow(fields.StreamColumns), nil
}

// ReflectStreamRow maps the fields of struct R by reflection with the
// streaming decoders: every column needs a field, and a field whose column is
// not selected is reset by its decoder and otherwise left alone. Decoders
// reuse the storage of []byte and slice fields across rows.
func reflectStreamRow[R any](opts ...StructOption[R]) (StreamRowDecoder[R], error) {
	t := reflect.TypeFor[R]()
	config, err := structOptions(t, opts)
	if err != nil {
		return nil, err
	}
	builtins := streamBuiltinFields[R]()
	var columns []StreamOutputField[R]
	err = eachStructField(t, func(sf reflect.StructField, tag structmap.FieldTag) error {
		entry, err := resolveField(builtins, config.streams, sf.Type, tag.Stream, "cql.WithStreamCodec")
		if err == nil {
			columns = append(columns, entry.column(tag.Column, sf.Offset))
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return structStreamRow(columns), nil
}

func structStreamRow[R any](fields []StreamOutputField[R]) StreamRowDecoder[R] {
	return streamRowsCodec[R]{reset: func(*R) {}, fields: streamDeclarations(fields), structMapping: true}
}
