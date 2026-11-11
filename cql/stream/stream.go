// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package stream decodes streamed rows into caller-owned storage. It is the
// streaming counterpart of the buffered decoders in package cql: RowInto,
// RowOf, Column, ColumnAt, OneColumn, Tuple and UDT mirror cql.RowInto,
// cql.RowOf, cql.Column, cql.ColumnAt, cql.OneColumn, cql.TupleDecoder and
// cql.UDTDecoder. Both share column matching and struct tags, but compile
// separate plans because their storage and lifetime contracts differ.
//
// A Statement pairs CQL and a cql.ParamEncoder with a RowDecoder;
// Session.PrepareStream prepares it. Decoders write payloads directly into
// supplied storage and require enough capacity: seed buffers before
// decoding, and a decoder reports cqlerr.StreamCapacityError instead of
// growing them. Text, ascii, blob and varint values decode into []byte, so
// applications choose separate row structs when buffered and streaming field
// types differ.
//
// List, Set, Vector and Map decode into the same Go containers as cql's
// ListCodec, SetCodec, VectorCodec and MapCodec, with streaming element
// decoders. Nullable preserves NULL; unwrapped list, set and map decoders
// accept NULL as an empty collection, matching buffered decoding, and
// vectors reject it.
package stream

import (
	"net/netip"

	"github.com/scylladb/gocql/v2/cql"
	codec "github.com/scylladb/gocql/v2/internal/codec"
)

// Statement pairs CQL and typed parameters with a decoder that writes rows
// into caller-owned storage. Session.PrepareStream prepares it; streaming is
// always prepared. No buffered row decoder is required.
type Statement[P, R any] struct {
	// Params encodes P. It may be nil when P is cql.EmptyParams.
	Params cql.ParamEncoder[P]
	// Rows decodes into seeded caller storage and must not be nil.
	Rows RowDecoder[R]
	CQL  string
	// Kind classifies ordinary and conditional statements for safe execution.
	Kind cql.StatementKind
}

// Decoder compiles a value decoder that writes into seeded caller storage.
type Decoder[T any] interface {
	Compile(cql.DataType) (ValuePlan[T], error)
}

// RowDecoder compiles row mappings that decode into seeded caller storage.
type RowDecoder[R any] interface {
	Compile(cql.Columns) (RowPlan[R], error)
}

// Resetter resets a field absent from metadata while preserving buffer capacity.
type Resetter[T any] interface {
	Reset(*T)
}

// ValuePlan decodes one value of a compiled column type into caller storage.
// Reset clears the logical value while keeping buffer capacity, and
// DecodeInto reads the value from a ValueReader into the destination.
type ValuePlan[T any] = codec.StreamValuePlan[T]

// RowPlan decodes rows of compiled metadata into caller storage through a
// RowReader.
type RowPlan[R any] = codec.StreamRowPlan[R]

// RowReader is a scoped, checked reader over one streamed row. Custom plans
// read each column with its ReadColumn method and skip one with SkipColumn;
// it never exposes retained byte views.
type RowReader = codec.StreamRowReader

// ValueReader is a scoped, limited reader over one streamed value.
type ValueReader = codec.StreamValueReader

// OutputField is one column of a streamed row mapping, built with Column or
// ColumnAt.
type OutputField[R any] = codec.StreamOutputField[R]

// RowInto maps result columns into caller storage. reset clears application state before
// each row and must preserve mapped buffer capacity. Only a successful NextInto publishes a row.
func RowInto[R any](reset func(*R), fields ...OutputField[R]) RowDecoder[R] {
	return codec.StreamRowInto[R](reset, fields...)
}

// Column maps a named result column into caller storage returned by field.
// Seed buffer capacity before decoding; stream decoders do not grow payload buffers.
func Column[R any, V any](name string, decoder Decoder[V], field func(*R) *V) OutputField[R] {
	return codec.StreamColumn[R, V](name, decoder, field)
}

// ColumnAt maps a zero-based result column into seeded caller storage returned by field.
func ColumnAt[R any, V any](index int, decoder Decoder[V], field func(*R) *V) OutputField[R] {
	return codec.StreamColumnAt[R, V](index, decoder, field)
}

// OneColumn decodes one result column into seeded caller storage with decoder.
func OneColumn[T any](decoder Decoder[T]) RowDecoder[T] {
	return codec.StreamOneColumn[T](decoder)
}

// RowOf decodes streamed rows into struct R by column name, mapped like
// cql.RowOf but with the streaming decoders, which have their own Go types:
// text, ascii, blob and varint columns decode into []byte (blob by default;
// pick another with `cql:"name,stream=Text"`), and slices decode lists
// (`stream=Set` for sets, `stream=Vector` for vectors). Storage of []byte and
// slice fields is reused across rows. Every column needs a field.
//
// When R or *R has a CQLStreamColumns method, as cmd/cqlgen generates, its
// mapping is used; otherwise R is reflected once, here. Of the
// cql.StructOption values, RowOf applies WithCodec from this package and
// ignores cql.WithCodec and cql.ReuseBuffers.
func RowOf[R any](opts ...cql.StructOption[R]) (RowDecoder[R], error) {
	var columns []OutputField[R]
	if g, ok := any(new(R)).(interface{ CQLStreamColumns() []OutputField[R] }); ok {
		columns = g.CQLStreamColumns()
	}
	return codec.GeneratedStreamRow(columns, opts...)
}

// MustRowOf is RowOf that panics on error, for package-level variables.
func MustRowOf[R any](opts ...cql.StructOption[R]) RowDecoder[R] {
	r, err := RowOf(opts...)
	if err != nil {
		panic(err)
	}
	return r
}

// WithCodec maps P's fields of type V, cql.Nullable[V] and []V (as a list)
// with decoder in RowOf. Recognized built-in stream tags take precedence,
// including in generated mappings. It affects RowOf only; cql.ParamsOf and
// cql.RowOf ignore it.
func WithCodec[P, V any](decoder Decoder[V]) cql.StructOption[P] {
	return codec.WithStreamCodec[P, V](decoder)
}

// List decodes a list into seeded slice slots using child. Seed each nested buffer too.
// NULL decodes as an empty slice while preserving capacity; Nullable preserves NULL.
func List[T any](child Decoder[T]) Decoder[[]T] { return codec.StreamList[T](child) }

// Set decodes a set into seeded slice slots using child, preserving server order.
// NULL decodes as an empty slice while preserving capacity; Nullable preserves NULL.
func Set[T any](child Decoder[T]) Decoder[[]T] { return codec.StreamSet[T](child) }

// Vector decodes a vector into seeded slice storage using child.
func Vector[T any](child Decoder[T]) Decoder[[]T] {
	return codec.StreamVector[T](child)
}

// Map decodes a map into seeded entry slots using key and value decoders.
// NULL decodes as an empty slice while preserving capacity; Nullable preserves NULL.
func Map[K any, V any](key Decoder[K], value Decoder[V]) Decoder[[]cql.Entry[K, V]] {
	return codec.StreamMap[K, V](key, value)
}

// Nullable preserves NULL while decoding valid values into caller storage with base.
func Nullable[T any](base Decoder[T]) Decoder[cql.Nullable[T]] {
	return codec.StreamNullable[T](base)
}

// Empty adds empty-cell decoding to base while preserving caller buffer capacity.
func Empty[T any](base Decoder[T]) Decoder[cql.EmptyValueOf[T]] {
	return codec.EmptyStream[T](base)
}

// Tuple decodes tuple fields into caller storage with decoder.
func Tuple[R any](decoder RowDecoder[R]) Decoder[R] {
	return codec.StreamTuple[R](decoder)
}

// UDT decodes UDT fields into caller storage with decoder and explicit matching options.
func UDT[R any](decoder RowDecoder[R], options cql.UDTOptions) Decoder[R] {
	return codec.StreamUDT[R](decoder, options)
}

// DecimalValue is a decimal decoded into caller storage: the unscaled
// two's-complement big-endian integer and the scale.
type DecimalValue = codec.StreamDecimalValue

// EncodedValue is a validated cell of any type decoded into caller storage:
// its type, state and body bytes.
type EncodedValue = codec.StreamEncodedValue

// Scalar decoders. Text, ASCII, Blob and Varint write the payload into a
// []byte with enough capacity; Text and ASCII validate it.
var (
	Bool      Decoder[bool]          = codec.StreamBool
	Int8      Decoder[int8]          = codec.StreamInt8
	Int16     Decoder[int16]         = codec.StreamInt16
	Int32     Decoder[int32]         = codec.StreamInt32
	Int64     Decoder[int64]         = codec.StreamInt64
	Counter   Decoder[cql.Counter]   = codec.StreamCounter
	Float32   Decoder[float32]       = codec.StreamFloat32
	Float64   Decoder[float64]       = codec.StreamFloat64
	Date      Decoder[cql.Date]      = codec.StreamDate
	Time      Decoder[cql.TimeOfDay] = codec.StreamTime
	Timestamp Decoder[cql.Timestamp] = codec.StreamTimestamp
	Duration  Decoder[cql.Duration]  = codec.StreamDuration
	UUID      Decoder[cql.UUID]      = codec.StreamUUID
	TimeUUID  Decoder[cql.TimeUUID]  = codec.StreamTimeUUID
	Inet      Decoder[netip.Addr]    = codec.StreamInet
	Text      Decoder[[]byte]        = codec.StreamText
	ASCII     Decoder[[]byte]        = codec.StreamASCII
	Blob      Decoder[[]byte]        = codec.StreamBlob
	Varint    Decoder[[]byte]        = codec.StreamVarint
	Decimal   Decoder[DecimalValue]  = codec.StreamDecimal
	// Encoded decodes a cell of any type, validated against its metadata,
	// into an EncodedValue.
	Encoded Decoder[EncodedValue] = codec.StreamEncoded
)
