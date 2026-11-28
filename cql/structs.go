// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

// StructFields is a struct's buffered mapping. cmd/cqlgen generates it as a
// method of the struct,
//
//	func (T) CQLFields() cql.StructFields[T]
//
// which ParamsOf and RowOf call instead of reflecting T. A nil slice means
// that direction reflects T, and reports a field it cannot map. The
// streaming mapping is a separate CQLStreamColumns method; see cql.StreamRowOf.
//
// The zero StructFields reflects T in both directions.
type StructFields[P any] struct {
	// Params maps the struct's fields to bind markers, or is nil to reflect
	// them.
	Params []InputField[P]
	// Columns maps result columns to the struct's fields, or is nil to
	// reflect them.
	Columns []OutputField[P]
}

// ParamsOf binds a statement's parameters from the exported fields of struct
// P by column name. A field's column is its `cql:"name"` tag, or its name in
// snake_case, as gocqlx maps it (CreatedAt is created_at, UserID user_id);
// `cql:"-"` skips it and `cql:"name,codec=ListCodec"` picks a
// built-in codec other than the type's default (`cql:",codec=..."` keeps the
// snake_case name). Names match the column exactly, so a quoted mixed-case
// column, or a name snake_case splits otherwise (UserIDs is user_i_ds),
// needs a tag. Unexported fields other than embedded
// structs are ignored, and one with a cql tag other than "-" is an error,
// also inside an embedded struct. A field without a matching parameter is
// skipped, so one struct serves statements that bind a subset of its
// columns, but every parameter needs a field. A named marker (:name) that the
// statement repeats binds the same field each time; two ? markers the server
// names alike, as it names WHERE a > ? AND a < ? after their column, are
// rejected when the statement is prepared, since one field would bind both:
// name them, as in WHERE a > :from AND a < :to. The fields of a struct
// embedded by value, exported or not, map as if declared in P. Like Go's
// field promotion, but by column rather than Go name, a shallower field
// hides deeper ones with the same column whatever their order, and two at
// the shallowest depth of a column are an error. An embedded field tagged
// `cql:"-"` is skipped; otherwise embedding by pointer, or any other tag on
// an embedded struct, is rejected. An embedded non-struct is an ordinary
// field named after its type.
//
// A field's codec is derived from its Go type, recursively. A leaf type has
// a built-in codec: bool, int8, int16, int32, int64, int (bigint),
// float32, float64, string (text and ascii, StringCodec), []byte
// (ByteSliceCodec), cql.Text and cql.Bytes (borrowed views, TextViewCodec and
// BytesViewCodec), UUID, TimeUUID, netip.Addr, Varint, *big.Int, Decimal,
// Date, TimeOfDay, Timestamp, Duration, time.Time (timestamp,
// TimestampTimeCodec) and time.Duration (duration, TimeDurationCodec). A
// named type defined from one of the predeclared ones, such as type Status
// string, maps like it, and [16]byte maps a uuid or timeuuid like UUID. A
// type with a CQLCodec method, func (T) CQLCodec() cql.Codec[T] or the same
// on *T, as cmd/cqlgen -udt generates for a UDT struct, maps with its codec.
// From those, []V and [N]V map a list, a set or a vector, whichever the
// column is (`codec=ListCodec`, `codec=SetCodec` or `codec=VectorCodec`
// requires one; an array needs exactly N elements), *V maps V with NULL as
// nil, Nullable[V] maps V with NULL as invalid, map[K]V maps a map, Pair[A, B]
// maps a tuple of two members, and BindValue[V] binds V, NULL or UNSET as a
// parameter only, so RowOf rejects it; the forms nest, as in [][]int32,
// map[string][]string or *Nullable[[]Status]. A `codec=` tag naming a leaf
// codec, such as `codec=ASCIIStringCodec`, applies to the leaves of the
// field's type it maps; a tag holding a Go expression, which only cmd/cqlgen
// evaluates, is honoured by reflection only through the codec derived from
// WithCodec or a CQLCodec method. WithCodec maps a type wherever it
// appears, and overrides its default.
//
// A nil pointer, slice or map binds NULL, or an empty collection, unless the
// field's tag has nilunset, `cql:"name,nilunset"`, or NilAsUnset is given:
// then it binds UNSET, which leaves the column unchanged.
//
// ParamsOf applies WithCodec, WithEncoder and NilAsUnset, and rejects other
// options.
//
// When P or *P has a CQLFields method, as cmd/cqlgen generates, its mapping
// is used; otherwise P is reflected once, here. Either way encoding is
// reflection-free, and a scalar field is as fast as hand-written Params. The
// method is found through P's method set, so a package-level variable built
// with MustParamsOf sees it whatever the initialization order.
func ParamsOf[P any](opts ...StructOption) (ParamEncoder[P], error) {
	return generatedParams(generatedFields[P](), targetParams, opts...)
}

// RowOf decodes rows into struct R by column name, mapped like ParamsOf. A
// field whose column is not selected stays zero, but every result column
// needs a field, unless IgnoreExtraFields is given, and a result with two
// columns of the same name needs positional Row and ColumnAt instead.
//
// A NULL cell fails to decode into a field that cannot hold NULL, with an
// error matching ErrNull, unless the field's tag has nullzero,
// `cql:"name,nullzero"`, or NullAsZero is given: then the field stays zero.
// A Nullable or pointer field holds NULL as invalid or nil, and a collection
// field as empty.
//
// Explicit Params and Row are stricter: every field needs a column, and
// every column a field.
//
// RowOf applies WithCodec, ReuseBuffers, IgnoreExtraFields and NullAsZero,
// and rejects other options.
func RowOf[R any](opts ...StructOption) (RowDecoder[R], error) {
	return generatedRow(generatedFields[R](), targetRows, opts...)
}

// MustParamsOf is ParamsOf that panics on error, for package-level variables.
func MustParamsOf[P any](opts ...StructOption) ParamEncoder[P] {
	return must(ParamsOf[P](opts...))
}

// MustRowOf is RowOf that panics on error, for package-level variables.
func MustRowOf[R any](opts ...StructOption) RowDecoder[R] { return must(RowOf[R](opts...)) }

// UDTCodecOf maps a UDT's fields to the fields of struct T by name, as
// ParamsOf and RowOf map columns, for a UDT column or a UDT inside a
// collection. Every UDT field needs a struct field and every struct field a
// UDT field, unless IgnoreExtraFields or AllowMissingFields relaxes it; a UDT
// field the struct leaves out is bound as NULL. UDTName names the UDT it
// expects; without it the UDT matches by fields alone.
//
// A NULL UDT field decodes as the struct field's zero value, and so does a
// trailing field the server leaves out of a value written before ALTER TYPE
// ADD added the field, so a UDT keeps decoding after its type grows; a
// Nullable or pointer field holds such a field as invalid or nil.
//
// Pass the codec to WithCodec to map fields of type T, and []T, *T and
// map values of T, in an enclosing struct, or return it from T's CQLCodec
// method, as cmd/cqlgen -udt generates, to map T wherever it appears without
// an option. UDTCodecOf applies WithCodec, for T's own fields, UDTName,
// IgnoreExtraFields, AllowMissingFields and NullAsZero, and rejects other
// options.
func UDTCodecOf[T any](opts ...StructOption) (Codec[T], error) {
	config, err := structOptions[T](targetUDT, opts)
	if err != nil {
		return nil, err
	}
	fields := generatedFields[T]()
	params, err := generatedParams(fields, targetUDT, opts...)
	if err != nil {
		return nil, err
	}
	rows, err := generatedRow(fields, targetUDT, opts...)
	if err != nil {
		return nil, err
	}
	return recordCodec[T]{
		recordEncoder: recordEncoder[T]{kind: KindUDT, codec: params, options: config.udt},
		recordDecoder: recordDecoder[T]{kind: KindUDT, codec: rows, options: config.udt},
	}, nil
}

// MustUDTCodecOf is UDTCodecOf that panics on error, for package-level
// variables.
func MustUDTCodecOf[T any](opts ...StructOption) Codec[T] {
	return must(UDTCodecOf[T](opts...))
}

// generatedFields returns the mapping of P's CQLFields method, with a value
// or pointer receiver, or the zero StructFields, which reflects P.
func generatedFields[P any]() structFields[P] {
	if g, ok := any(new(P)).(interface{ CQLFields() StructFields[P] }); ok {
		f := g.CQLFields()
		return structFields[P]{Params: f.Params, Columns: f.Columns}
	}
	return structFields[P]{}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
