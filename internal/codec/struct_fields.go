// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unsafe"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

// Struct mappings build the same Param and Column fields an application writes
// by hand, from a struct's exported fields and `cql:"name,codec=X"` tags.
// Reflection runs once, when the mapping is built; the field accessors read at
// a fixed offset, so encoding and decoding stay reflection-free and keep the
// built-in direct ops. cmd/cqlgen generates the same fields from source, using
// the tag grammar and type table below, and registers them with RegisterStruct.

// FieldTag is a struct field's parsed `cql` tag.
type FieldTag struct {
	Column string // column name; the lowercased field name when untagged
	Codec  string // codec override: a built-in codec name, or (cqlgen only) a Go expression
	Stream string // streaming decoder override, like Codec, for StreamRowOf
}

// ParseFieldTag returns the column mapping of a struct field. ok is false for
// a field that maps to no column: an unexported one, or one tagged "-".
func ParseFieldTag(fieldName string, exported bool, tag reflect.StructTag) (FieldTag, bool, error) {
	value, tagged := tag.Lookup("cql")
	if value == "-" || !exported {
		return FieldTag{}, false, nil
	}
	parts := splitTag(value)
	result := FieldTag{Column: parts[0]}
	if !tagged || result.Column == "" {
		result.Column = strings.ToLower(fieldName)
	}
	for _, option := range parts[1:] {
		key, val, _ := strings.Cut(option, "=")
		switch {
		case key == "codec" && val != "":
			result.Codec = val
		case key == "stream" && val != "":
			result.Stream = val
		default:
			return FieldTag{}, false, fmt.Errorf("gocql: field %s: unknown cql tag option %q", fieldName, option)
		}
	}
	return result, true, nil
}

// splitTag splits a tag value at the commas outside brackets, so a codec
// expression such as MapCodec(StringCodec, Int64) stays one option.
func splitTag(value string) []string {
	var parts []string
	depth, start := 0, 0
	for i, r := range value {
		switch r {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, value[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, value[start:])
}

// BuiltinFieldType is one Go field type mapped without configuration.
type BuiltinFieldType struct {
	Type reflect.Type
	// TypeSrc, EncoderSrc and DecoderSrc spell the type and codecs in Go
	// source, qualified with "cql." or "netip.".
	TypeSrc, EncoderSrc, DecoderSrc string
	// Names are the codec names a `codec=` tag selects this entry by.
	Names []string
	// Default marks the entry used for Type without a `codec=` tag.
	Default bool
}

type builtinField[P any] struct {
	param  func(name string, offset uintptr) InputField[P]
	column func(name string, offset uintptr) OutputField[P]
	BuiltinFieldType
}

// builtinFields returns the built-in field types, each with its Nullable form.
func builtinFields[P any]() []builtinField[P] {
	// Every entry instantiates field accessors for P, so the table grows the
	// code of each mapped struct type; maps stay out of it (MapCodec with
	// WithCodec or a codec= tag) because key and value pairs multiply.
	var fields []builtinField[P]
	add := func(entries ...builtinField[P]) { fields = append(fields, entries...) }
	add(builtin[P](Bool, Bool, Bool, "bool", "Bool", "Bool", "Bool", true)...)
	add(builtin[P](Int8, Int8, Int8, "int8", "Int8", "Int8", "Int8", true)...)
	add(builtin[P](Int16, Int16, Int16, "int16", "Int16", "Int16", "Int16", true)...)
	add(builtin[P](Int32, Int32, Int32, "int32", "Int32", "Int32", "Int32", true)...)
	add(builtin[P](Int64, Int64, Int64, "int64", "Int64", "Int64", "Int64", true)...)
	add(builtin[P](Float32, Float32, Float32, "float32", "Float32", "Float32", "Float32", true)...)
	add(builtin[P](Float64, Float64, Float64, "float64", "Float64", "Float64", "Float64", true)...)
	add(builtin[P](StringCodec, StringCodec, StringCodec, "string", "StringCodec", "StringCodec", "StringCodec", true)...)
	add(builtin[P](ASCIIStringCodec, ASCIIStringCodec, ASCIIStringCodec, "string", "ASCIIStringCodec", "ASCIIStringCodec", "ASCIIStringCodec", false)...)
	add(builtin[P](BytesEncoder, OwnedBlobCodec, OwnedBlobCodec, "[]byte", "BytesEncoder", "OwnedBlobCodec", "OwnedBlobCodec", true)...)
	add(builtin[P](BlobCodec, BlobCodec, BlobCodec, "cql.Bytes", "BlobCodec", "BlobCodec", "BlobCodec", true)...)
	add(builtin[P](UUIDCodec, UUIDCodec, UUIDCodec, "cql.UUID", "UUIDCodec", "UUIDCodec", "UUIDCodec", true)...)
	add(builtin[P](TimeUUIDCodec, TimeUUIDCodec, TimeUUIDCodec, "cql.TimeUUID", "TimeUUIDCodec", "TimeUUIDCodec", "TimeUUIDCodec", true)...)
	add(builtin[P](InetCodec, InetCodec, InetCodec, "netip.Addr", "InetCodec", "InetCodec", "InetCodec", true)...)
	add(builtin[P](CounterCodec, CounterCodec, CounterCodec, "cql.Counter", "CounterCodec", "CounterCodec", "CounterCodec", true)...)
	add(builtin[P](VarintCodec, VarintCodec, VarintCodec, "cql.Varint", "VarintCodec", "VarintCodec", "VarintCodec", true)...)
	add(builtin[P](DecimalCodec, DecimalCodec, DecimalCodec, "cql.Decimal", "DecimalCodec", "DecimalCodec", "DecimalCodec", true)...)
	add(builtin[P](DateCodec, DateCodec, DateCodec, "cql.Date", "DateCodec", "DateCodec", "DateCodec", true)...)
	add(builtin[P](TimeCodec, TimeCodec, TimeCodec, "cql.TimeOfDay", "TimeCodec", "TimeCodec", "TimeCodec", true)...)
	add(builtin[P](TimestampCodec, TimestampCodec, TimestampCodec, "cql.Timestamp", "TimestampCodec", "TimestampCodec", "TimestampCodec", true)...)
	add(builtin[P](DurationCodec, DurationCodec, DurationCodec, "cql.Duration", "DurationCodec", "DurationCodec", "DurationCodec", true)...)
	return fields
}

// builtin returns the entries for V, Nullable[V], and []V as a list (the
// default) or a set (`codec=SetCodec`) of elem, the codec V's elements use.
func builtin[P, V any](enc Encoder[V], dec Decoder[V], elem Codec[V], typeSrc, encName, decName, elemName string, def bool) []builtinField[P] {
	names := []string{encName}
	if decName != encName {
		names = append(names, decName)
	}
	plain := builtinField[P]{
		BuiltinFieldType: BuiltinFieldType{
			Type: reflect.TypeFor[V](), TypeSrc: typeSrc,
			EncoderSrc: "cql." + encName, DecoderSrc: "cql." + decName,
			Names: names, Default: def,
		},
		param:  fieldParam[P](enc),
		column: fieldColumn[P](dec),
	}
	nullable := builtinField[P]{
		BuiltinFieldType: BuiltinFieldType{
			Type: reflect.TypeFor[cqltype.Nullable[V]](), TypeSrc: "cql.Nullable[" + typeSrc + "]",
			EncoderSrc: "cql.NullableEncoder(cql." + encName + ")", DecoderSrc: "cql.NullableDecoder(cql." + decName + ")",
			Names: names, Default: def,
		},
		param:  fieldParam[P](NullableEncoder(enc)),
		column: fieldColumn[P](NullableDecoder(dec)),
	}
	collection := func(codec Codec[[]V], src string, names []string, def bool) builtinField[P] {
		return builtinField[P]{
			BuiltinFieldType: BuiltinFieldType{
				Type: reflect.TypeFor[[]V](), TypeSrc: "[]" + typeSrc,
				EncoderSrc: src, DecoderSrc: src, Names: names, Default: def,
			},
			param:  fieldParam[P](Encoder[[]V](codec)),
			column: fieldColumn[P](Decoder[[]V](codec)),
		}
	}
	fields := []builtinField[P]{plain, nullable,
		collection(ListCodec(elem), "cql.ListCodec(cql."+elemName+")", names, def)}
	if def {
		fields = append(fields, collection(SetCodec(elem), "cql.SetCodec(cql."+elemName+")", []string{"SetCodec"}, false))
	}
	return fields
}

// BuiltinFieldTypes returns the Go field types mapped without configuration.
func BuiltinFieldTypes() []BuiltinFieldType {
	fields := builtinFields[struct{}]()
	types := make([]BuiltinFieldType, len(fields))
	for i, f := range fields {
		types[i] = f.BuiltinFieldType
	}
	return types
}

// SelectBuiltin picks among the built-in entries for one Go type: the one
// named codec, or the default when codec is empty.
func SelectBuiltin(candidates []BuiltinFieldType, codec string) (int, bool) {
	for i, c := range candidates {
		if codec == "" && c.Default {
			return i, true
		}
		for _, name := range c.Names {
			if codec != "" && name == codec {
				return i, true
			}
		}
	}
	return -1, false
}

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

// StructOption configures a reflected struct mapping.
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
	c.codecs[reflect.TypeFor[cqltype.Nullable[V]]()] = builtinField[P]{
		param:  fieldParam[P](NullableEncoder[V](o.codec)),
		column: fieldColumn[P](NullableDecoder[V](o.codec)),
	}
}

// WithCodec maps the fields of type V, and of Nullable[V], with codec. It is
// how a reflected mapping covers a type with no built-in codec, such as a UDT,
// and it replaces the built-in default for V. A field's `codec=` tag naming a
// built-in codec still wins. cqlgen takes the codec from the tag instead.
func WithCodec[P, V any](codec Codec[V]) StructOption[P] { return codecOption[P, V]{codec: codec} }

// StructFields is a struct's mapping, as cqlgen generates it. Params and
// Columns are nil when a field has no buffered codec, and StreamColumns when
// one has no streaming decoder; ParamsOf, RowOf and StreamRowOf then reflect,
// and report the field.
type StructFields[P any] struct {
	Params        []InputField[P]
	Columns       []OutputField[P]
	StreamColumns []StreamOutputField[P]
}

var structRegistry sync.Map // reflect.Type -> StructFields[P]

// RegisterStruct installs a generated mapping, which ParamsOf and RowOf use
// for P instead of reflection. It panics when P is already registered.
func RegisterStruct[P any](fields StructFields[P]) {
	if _, loaded := structRegistry.LoadOrStore(reflect.TypeFor[P](), fields); loaded {
		panic(fmt.Sprintf("gocql: struct %s registered twice", reflect.TypeFor[P]()))
	}
}

// RegisteredStruct returns the mapping RegisterStruct installed for P.
func RegisteredStruct[P any]() (StructFields[P], bool) {
	f, ok := structRegistry.Load(reflect.TypeFor[P]())
	if !ok {
		return StructFields[P]{}, false
	}
	return f.(StructFields[P]), true
}

// ParamsOf binds a statement's parameters from the fields of struct P, by
// column name. A field without a matching parameter is skipped, so one struct
// serves statements that bind a subset of its columns; every parameter still
// needs a field. A generated mapping is used when registered, otherwise P is
// reflected once, here.
func ParamsOf[P any](opts ...StructOption[P]) (ParamEncoder[P], error) {
	if f, ok := RegisteredStruct[P](); ok && f.Params != nil {
		return structParams(f.Params), nil
	}
	return ReflectParams(opts...)
}

// RowOf decodes rows into struct P by column name, like ParamsOf. A field
// whose column is not selected stays zero.
func RowOf[R any](opts ...StructOption[R]) (RowDecoder[R], error) {
	if f, ok := RegisteredStruct[R](); ok && f.Columns != nil {
		config, err := structOptions(reflect.TypeFor[R](), opts)
		if err != nil {
			return nil, err
		}
		reset, err := bufferReset[R](config.reuseBuffers)
		if err != nil {
			return nil, err
		}
		return structRow(f.Columns, reset), nil
	}
	return ReflectRow(opts...)
}

func MustParamsOf[P any](opts ...StructOption[P]) ParamEncoder[P] {
	p, err := ParamsOf(opts...)
	if err != nil {
		panic(err)
	}
	return p
}

func MustRowOf[R any](opts ...StructOption[R]) RowDecoder[R] {
	r, err := RowOf(opts...)
	if err != nil {
		panic(err)
	}
	return r
}

// ReflectParams is ParamsOf without the generated-mapping lookup.
func ReflectParams[P any](opts ...StructOption[P]) (ParamEncoder[P], error) {
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

// ReflectRow is RowOf without the generated-mapping lookup.
func ReflectRow[R any](opts ...StructOption[R]) (RowDecoder[R], error) {
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

// ReuseBuffers makes RowOf keep the storage of P's mapped slice fields, such
// as []byte and collections, between rows decoded into the same destination,
// as RowInto does: the next row overwrites what a previous row left there, so
// a caller that keeps such a field across rows must copy it. Other fields are
// zeroed as usual.
func ReuseBuffers[P any]() StructOption[P] { return reuseBuffersOption[P]{} }

// bufferReset returns the RowInto reset that keeps the storage of R's mapped
// slice fields, or nil, which zeroes the row, when reuse is off or R has none.
func bufferReset[R any](reuse bool) (func(*R), error) {
	if !reuse {
		return nil, nil
	}
	var offsets []uintptr
	err := eachStructField(reflect.TypeFor[R](), func(sf reflect.StructField, _ FieldTag) error {
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
	err = eachStructField(t, func(sf reflect.StructField, tag FieldTag) error {
		entry, err := resolveField(builtins, config.codecs, sf.Type, tag.Codec, "WithCodec")
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
func eachStructField(t reflect.Type, fn func(reflect.StructField, FieldTag) error) error {
	var fields []reflect.StructField
	var candidates []FieldCandidate
	var tags []FieldTag
	var walk func(t reflect.Type, prefix string, offset uintptr, depth int) error
	walk = func(t reflect.Type, prefix string, offset uintptr, depth int) error {
		for i := range t.NumField() {
			sf := t.Field(i)
			path := prefix + sf.Name
			if sf.Anonymous {
				embed, err := EmbeddedField(sf.Name, sf.Tag, sf.Type.Kind() == reflect.Pointer, sf.Type.Kind() == reflect.Struct)
				if err != nil {
					return fmt.Errorf("gocql: struct mapping of %s: %w", t, err)
				}
				if embed == EmbedSkip {
					continue
				}
				if embed == EmbedFlatten {
					if err := walk(sf.Type, path+".", offset+sf.Offset, depth+1); err != nil {
						return err
					}
					continue
				}
			}
			tag, ok, err := ParseFieldTag(sf.Name, sf.IsExported(), sf.Tag)
			if err != nil {
				return fmt.Errorf("gocql: struct mapping of %s: %w", t, err)
			}
			if !ok {
				continue
			}
			sf.Name, sf.Offset = path, offset+sf.Offset
			fields = append(fields, sf)
			tags = append(tags, tag)
			candidates = append(candidates, FieldCandidate{Column: tag.Column, Path: path, Depth: depth})
		}
		return nil
	}
	if err := walk(t, "", 0, 0); err != nil {
		return err
	}
	keep, err := ResolveFieldConflicts(candidates)
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

// Embedding says how a struct mapping treats an embedded field.
type Embedding int

const (
	EmbedField   Embedding = iota // an embedded non-struct: an ordinary field named after its type
	EmbedFlatten                  // a struct embedded by value: its fields are mapped as the outer struct's
	EmbedSkip                     // tagged `cql:"-"`
)

// EmbeddedField decides how to map an embedded field. Pointer embedding is
// rejected: its fields are not at a fixed offset, and decoding would have to
// allocate a nil one. A tag other than "-" is rejected too, since there is no
// column prefix to give.
func EmbeddedField(name string, tag reflect.StructTag, pointer, isStruct bool) (Embedding, error) {
	value, tagged := tag.Lookup("cql")
	switch {
	case value == "-":
		return EmbedSkip, nil
	case pointer:
		return 0, fmt.Errorf("embedded pointer field %s is not supported; embed it by value or tag it `cql:\"-\"`", name)
	case !isStruct:
		return EmbedField, nil
	case tagged:
		return 0, fmt.Errorf("embedded field %s: only the tag `cql:\"-\"` is allowed", name)
	}
	return EmbedFlatten, nil
}

// FieldCandidate is a mapped field found while walking a struct and the
// structs it embeds; Depth counts the embeddings above it.
type FieldCandidate struct {
	Column, Path string
	Depth        int
}

// ResolveFieldConflicts returns, in order, the candidates that map their
// column, like Go's field promotion: a shallower field hides deeper ones with
// the same column, and two at the same depth are an error.
func ResolveFieldConflicts(candidates []FieldCandidate) ([]int, error) {
	best := map[string]int{}
	for i, c := range candidates {
		j, ok := best[c.Column]
		switch {
		case !ok || c.Depth < candidates[j].Depth:
			best[c.Column] = i
		case c.Depth == candidates[j].Depth:
			return nil, fmt.Errorf("fields %s and %s both map column %q", candidates[j].Path, c.Path, c.Column)
		}
	}
	keep := make([]int, 0, len(best))
	for i, c := range candidates {
		if best[c.Column] == i {
			keep = append(keep, i)
		}
	}
	return keep, nil
}

// resolveField picks a field's codec: a built-in named by its tag, then an
// option for its type, then the built-in default.
func resolveField[E interface{ meta() BuiltinFieldType }](builtins []E, options map[reflect.Type]E, typ reflect.Type, codec, option string) (E, error) {
	var zero E
	var candidates []BuiltinFieldType
	var entries []E
	for _, b := range builtins {
		if b.meta().Type == typ {
			candidates = append(candidates, b.meta())
			entries = append(entries, b)
		}
	}
	if codec != "" {
		if i, ok := SelectBuiltin(candidates, codec); ok {
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
	if i, ok := SelectBuiltin(candidates, ""); ok {
		return entries[i], nil
	}
	return zero, fmt.Errorf("no codec for type %s; pass %s or use a supported type", typ, option)
}

func (f builtinField[P]) meta() BuiltinFieldType { return f.BuiltinFieldType }

// Streaming rows decode with StreamDecoders, which use their own Go types: text
// and blob columns decode into reusable []byte storage, and there is no string
// decoder. The `stream=` tag key picks among them like `codec=` does for
// buffered rows; a field's `codec=` tag does not apply to streaming.

type streamBuiltinField[P any] struct {
	column func(name string, offset uintptr) StreamOutputField[P]
	BuiltinFieldType
}

func (f streamBuiltinField[P]) meta() BuiltinFieldType { return f.BuiltinFieldType }

// streamBuiltinFields returns the built-in streaming field types, each with
// its Nullable form and, for scalars, its list and set forms.
func streamBuiltinFields[P any]() []streamBuiltinField[P] {
	var fields []streamBuiltinField[P]
	add := func(entries ...streamBuiltinField[P]) { fields = append(fields, entries...) }
	add(streamBuiltin[P](StreamBool, "bool", "StreamBool", true)...)
	add(streamBuiltin[P](StreamInt8, "int8", "StreamInt8", true)...)
	add(streamBuiltin[P](StreamInt16, "int16", "StreamInt16", true)...)
	add(streamBuiltin[P](StreamInt32, "int32", "StreamInt32", true)...)
	add(streamBuiltin[P](StreamInt64, "int64", "StreamInt64", true)...)
	add(streamBuiltin[P](StreamFloat32, "float32", "StreamFloat32", true)...)
	add(streamBuiltin[P](StreamFloat64, "float64", "StreamFloat64", true)...)
	add(streamBuiltin[P](StreamBlob, "[]byte", "StreamBlob", true)...)
	add(streamBuiltin[P](StreamText, "[]byte", "StreamText", false)...)
	add(streamBuiltin[P](StreamASCII, "[]byte", "StreamASCII", false)...)
	add(streamBuiltin[P](StreamVarint, "[]byte", "StreamVarint", false)...)
	add(streamBuiltin[P](StreamUUID, "cql.UUID", "StreamUUID", true)...)
	add(streamBuiltin[P](StreamTimeUUID, "cql.TimeUUID", "StreamTimeUUID", true)...)
	add(streamBuiltin[P](StreamInet, "netip.Addr", "StreamInet", true)...)
	add(streamBuiltin[P](StreamCounter, "cql.Counter", "StreamCounter", true)...)
	add(streamBuiltin[P](StreamDecimal, "cql.StreamDecimalValue", "StreamDecimal", true)...)
	add(streamBuiltin[P](StreamDate, "cql.Date", "StreamDate", true)...)
	add(streamBuiltin[P](StreamTime, "cql.TimeOfDay", "StreamTime", true)...)
	add(streamBuiltin[P](StreamTimestamp, "cql.Timestamp", "StreamTimestamp", true)...)
	add(streamBuiltin[P](StreamDuration, "cql.Duration", "StreamDuration", true)...)
	return fields
}

// streamBuiltin returns the entries for V, Nullable[V], and []V as a list
// (the default) or a set (`stream=StreamSet`).
func streamBuiltin[P, V any](dec StreamDecoder[V], typeSrc, name string, def bool) []streamBuiltinField[P] {
	entry := func(typ reflect.Type, typeSrc, decSrc string, names []string, def bool, column func(string, uintptr) StreamOutputField[P]) streamBuiltinField[P] {
		return streamBuiltinField[P]{column: column, BuiltinFieldType: BuiltinFieldType{
			Type: typ, TypeSrc: typeSrc, DecoderSrc: decSrc, Names: names, Default: def,
		}}
	}
	src := "cql." + name
	return []streamBuiltinField[P]{
		entry(reflect.TypeFor[V](), typeSrc, src, []string{name}, def, fieldStreamColumn[P](dec)),
		entry(reflect.TypeFor[cqltype.Nullable[V]](), "cql.Nullable["+typeSrc+"]", "cql.StreamNullable("+src+")",
			[]string{name}, def, fieldStreamColumn[P](StreamNullable(dec))),
		// A list or set takes its element's default decoder.
		entry(reflect.TypeFor[[]V](), "[]"+typeSrc, "cql.StreamList("+src+")",
			[]string{"StreamList"}, def, fieldStreamColumn[P](StreamList(dec))),
		entry(reflect.TypeFor[[]V](), "[]"+typeSrc, "cql.StreamSet("+src+")",
			[]string{"StreamSet"}, false, fieldStreamColumn[P](StreamSet(dec))),
	}
}

// StreamBuiltinFieldTypes returns the Go field types StreamRowOf maps without
// configuration. EncoderSrc is empty: streaming only decodes.
func StreamBuiltinFieldTypes() []BuiltinFieldType {
	fields := streamBuiltinFields[struct{}]()
	types := make([]BuiltinFieldType, len(fields))
	for i, f := range fields {
		types[i] = f.BuiltinFieldType
	}
	return types
}

func fieldStreamColumn[R, V any](dec StreamDecoder[V]) func(string, uintptr) StreamOutputField[R] {
	return func(name string, offset uintptr) StreamOutputField[R] {
		return StreamColumn(name, dec, func(r *R) *V { return (*V)(unsafe.Add(unsafe.Pointer(r), offset)) })
	}
}

type streamCodecOption[P, V any] struct{ decoder StreamDecoder[V] }

func (o streamCodecOption[P, V]) apply(c *structConfig[P]) {
	c.streams[reflect.TypeFor[V]()] = streamBuiltinField[P]{column: fieldStreamColumn[P](o.decoder)}
	c.streams[reflect.TypeFor[cqltype.Nullable[V]]()] = streamBuiltinField[P]{column: fieldStreamColumn[P](StreamNullable(o.decoder))}
	c.streams[reflect.TypeFor[[]V]()] = streamBuiltinField[P]{column: fieldStreamColumn[P](StreamList(o.decoder))}
}

// WithStreamCodec maps P's fields of type V, Nullable[V] and []V (as a list)
// with decoder in StreamRowOf, like WithCodec does for ParamsOf and RowOf.
func WithStreamCodec[P, V any](decoder StreamDecoder[V]) StructOption[P] {
	return streamCodecOption[P, V]{decoder: decoder}
}

// StreamRowOf decodes streamed rows into struct P by column name, mapped like
// RowOf with the streaming decoders: every column needs a field, and a field
// whose column is not selected is reset by its decoder and otherwise left
// alone. Decoders reuse the storage of []byte and slice fields across rows.
func StreamRowOf[R any](opts ...StructOption[R]) (StreamRowDecoder[R], error) {
	if f, ok := RegisteredStruct[R](); ok && f.StreamColumns != nil {
		return structStreamRow(f.StreamColumns), nil
	}
	return ReflectStreamRow(opts...)
}

func MustStreamRowOf[R any](opts ...StructOption[R]) StreamRowDecoder[R] {
	r, err := StreamRowOf(opts...)
	if err != nil {
		panic(err)
	}
	return r
}

// ReflectStreamRow is StreamRowOf without the generated-mapping lookup.
func ReflectStreamRow[R any](opts ...StructOption[R]) (StreamRowDecoder[R], error) {
	t := reflect.TypeFor[R]()
	config, err := structOptions(t, opts)
	if err != nil {
		return nil, err
	}
	builtins := streamBuiltinFields[R]()
	var columns []StreamOutputField[R]
	err = eachStructField(t, func(sf reflect.StructField, tag FieldTag) error {
		entry, err := resolveField(builtins, config.streams, sf.Type, tag.Stream, "WithStreamCodec")
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
	return streamRowsCodec[R]{reset: func(*R) {}, fields: append([]StreamOutputField[R](nil), fields...), structMapping: true}
}
