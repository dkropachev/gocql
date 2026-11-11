// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import codec "github.com/scylladb/gocql/v2/internal/codec"

// StructOption configures ParamsOf, RowOf and stream.RowOf. Each option
// affects only some of them, and the others ignore it:
//
//   - WithCodec: ParamsOf and RowOf. stream.RowOf decodes with stream
//     decoders, so it ignores WithCodec; use stream.WithCodec.
//   - stream.WithCodec: stream.RowOf only.
//   - ReuseBuffers: RowOf only. ParamsOf has no destination, and
//     stream.RowOf always reuses its slice storage.
//
// Each call builds an independent mapping; options never change a generated
// mapping in place.
type StructOption[P any] = codec.StructOption[P]

// StructFields is a struct's buffered mapping. cmd/cqlgen generates it as a
// method of the struct,
//
//	func (T) CQLFields() cql.StructFields[T]
//
// which ParamsOf and RowOf call instead of reflecting T. A nil slice means
// that direction reflects T, and reports a field it cannot map. The
// streaming mapping is a separate CQLStreamColumns method; see stream.RowOf.
type StructFields[P any] struct {
	Params  []InputField[P]
	Columns []OutputField[P]
}

// ParamsOf binds a statement's parameters from the exported fields of struct
// P by column name. A field's column is its `cql:"name"` tag, or its
// lowercased name; `cql:"-"` skips it and `cql:"name,codec=ASCIIStringCodec"`
// picks a built-in codec other than the type's default. A field without a
// matching parameter is skipped, so one struct serves statements that bind a
// subset of its columns, but every parameter needs a field. The fields of a
// struct embedded by value map as if declared in P, and an outer field hides
// an embedded one with the same column; embedding by pointer is rejected.
// Slice fields map lists by default (ListCodec; `codec=SetCodec` for sets,
// `codec=VectorCodec` for vectors). A map[K]V field maps with GoMapCodec when
// K and V have built-in codecs that decode into owned storage: the numeric
// and boolean types, string, UUID, TimeUUID, netip.Addr, Date, TimeOfDay and
// Timestamp, and as values also []byte and Duration. Other maps, and
// []Entry[K, V] fields for MapCodec, need WithCodec or a `codec=` tag.
//
// When P or *P has a CQLFields method, as cmd/cqlgen generates, its mapping
// is used; otherwise P is reflected once, here. Either way encoding is
// reflection-free and as fast as hand-written Params. The method is found
// through P's method set, so a package-level variable built with
// MustParamsOf sees it whatever the initialization order.
func ParamsOf[P any](opts ...StructOption[P]) (ParamEncoder[P], error) {
	return codec.GeneratedParams(generatedFields[P](), opts...)
}

// RowOf decodes rows into struct P by column name, mapped like ParamsOf. A
// field whose column is not selected stays zero.
func RowOf[R any](opts ...StructOption[R]) (RowDecoder[R], error) {
	return codec.GeneratedRow(generatedFields[R](), opts...)
}

// MustParamsOf is ParamsOf that panics on error, for package-level variables.
func MustParamsOf[P any](opts ...StructOption[P]) ParamEncoder[P] {
	return must(ParamsOf(opts...))
}

// MustRowOf is RowOf that panics on error, for package-level variables.
func MustRowOf[R any](opts ...StructOption[R]) RowDecoder[R] { return must(RowOf(opts...)) }

// WithCodec maps P's fields of type V, and of Nullable[V], with c: a type
// with no built-in codec, such as a UDT, or a replacement for V's default.
// Recognized built-in codec tags take precedence. The option also applies to
// generated mappings; other generated custom-expression codecs remain defaults.
// It affects ParamsOf and RowOf; stream.RowOf ignores it.
func WithCodec[P, V any](c Codec[V]) StructOption[P] { return codec.WithCodec[P, V](c) }

// generatedFields returns the mapping of P's CQLFields method, with a value
// or pointer receiver, or the zero StructFields, which reflects P.
func generatedFields[P any]() codec.StructFields[P] {
	if g, ok := any(new(P)).(interface{ CQLFields() StructFields[P] }); ok {
		f := g.CQLFields()
		return codec.StructFields[P]{Params: f.Params, Columns: f.Columns}
	}
	return codec.StructFields[P]{}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// ReuseBuffers makes RowOf keep the storage of P's slice fields, such as
// []byte and lists, between rows decoded into the same destination, as
// RowInto does. The next row overwrites that storage, so copy a field that
// must outlive its row. Other fields, maps included, are zeroed as usual.
// It affects RowOf only: ParamsOf has no destination, and stream.RowOf
// always reuses slice storage.
func ReuseBuffers[P any]() StructOption[P] { return codec.ReuseBuffers[P]() }

// ListCodec encodes and decodes a list as a []T, the form struct mappings
// use for slice fields. A null cell, how CQL stores an empty list, decodes as
// an empty slice. Decoding allocates the slice unless the destination has the
// capacity (see ReuseBuffers). ListDecoder uses the same eager decoding path.
func ListCodec[T any](element Codec[T]) Codec[[]T] { return codec.ListCodec[T](element) }

// SetCodec is ListCodec for a set; elements keep the order the server sends.
func SetCodec[T any](element Codec[T]) Codec[[]T] { return codec.SetCodec[T](element) }

// MapCodec encodes and decodes a map as a []Entry[K, V], in the order the
// server sends; keys need not be comparable, and DecodeInto reuses the
// slice's capacity. Struct mappings take it through WithCodec or a `codec=`
// tag. GoMapCodec decodes into a Go map instead.
func MapCodec[K, V any](key Codec[K], value Codec[V]) Codec[[]Entry[K, V]] {
	return codec.MapCodec[K, V](key, value)
}

// GoMapCodec encodes and decodes a map as a Go map[K]V, the form struct
// mappings use for map fields of built-in key and value types. Encoding
// writes the entries in Go's map iteration order; the server keeps a map in
// key order. A response that repeats a key fails to decode, and a null cell,
// how CQL stores an empty map, decodes as an empty one. DecodeInto clears the
// destination map and reuses it.
func GoMapCodec[K comparable, V any](key Codec[K], value Codec[V]) Codec[map[K]V] {
	return codec.GoMapCodec[K, V](key, value)
}

// VectorCodec encodes and decodes a vector as a []T of its declared dimension.
func VectorCodec[T any](element Codec[T]) Codec[[]T] { return codec.VectorCodec(element) }
