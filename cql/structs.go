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
	return generatedParams(generatedFields[P](), opts...)
}

// RowOf decodes rows into struct P by column name, mapped like ParamsOf. A
// field whose column is not selected stays zero.
func RowOf[R any](opts ...StructOption[R]) (RowDecoder[R], error) {
	return generatedRow(generatedFields[R](), opts...)
}

// MustParamsOf is ParamsOf that panics on error, for package-level variables.
func MustParamsOf[P any](opts ...StructOption[P]) ParamEncoder[P] {
	return must(ParamsOf(opts...))
}

// MustRowOf is RowOf that panics on error, for package-level variables.
func MustRowOf[R any](opts ...StructOption[R]) RowDecoder[R] { return must(RowOf(opts...)) }

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
