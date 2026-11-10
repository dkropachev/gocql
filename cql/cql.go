// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package cql holds the CQL data vocabulary: statements, types, values,
// codecs, consistency levels and server errors. It does no I/O.
//
// Row, RowOf, Column, OneColumn, TupleDecoder and UDTDecoder describe buffered
// results. Their StreamRow, StreamRowOf, StreamColumn, StreamOneColumn,
// StreamTuple and StreamUDT counterparts describe incremental decoding into
// caller storage. Both paths share column matching and struct tags, but compile
// separate plans because their storage and lifetime contracts differ.
//
// ListCodec, SetCodec and VectorCodec encode and eagerly decode []T. MapCodec
// uses []Entry[K, V], preserving server order and supporting non-comparable keys.
// The corresponding Encoder and Decoder constructors provide either direction
// independently, with the same Go container types. StreamList, StreamSet,
// StreamVector and StreamMap use those containers too, with streaming element
// decoders. Buffered collection decoding accepts NULL as an empty collection;
// use Nullable to preserve that distinction. Vectors require their declared
// dimension and use Nullable for NULL.
//
// Buffered Bytes and Text elements may borrow a page, including when held in
// an eagerly decoded slice. Clone recursively detaches borrowed values before
// their page is released. Stream decoders write payloads directly into supplied
// storage and require enough capacity; they report StreamCapacityError instead
// of growing buffers. Streaming text and blob values use []byte, so applications
// choose separate row structs when buffered and streaming field types differ.
// StreamNullable preserves NULL. Unwrapped list/set/map stream decoders accept
// NULL as an empty collection, matching buffered decoding; vectors reject it.
//
// DataType and ColumnSpec describe public CQL types and columns. Option is for
// inherited settings, Nullable for nullable application data and results,
// BindValue for parameters that also need UNSET, and EmptyValueOf for supported
// empty CQL values. Their zero values mean inherited, NULL, present zero value,
// and ordinary zero value, respectively.
//
// ParseConsistency and ParseUUID return errors. Their MustParseConsistency and
// MustParseUUID forms panic and are useful for checked program constants.
package cql

import cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

type Consistency = cqltype.Consistency

const (
	Any         = cqltype.Any
	One         = cqltype.One
	Two         = cqltype.Two
	Three       = cqltype.Three
	Quorum      = cqltype.Quorum
	All         = cqltype.All
	LocalQuorum = cqltype.LocalQuorum
	EachQuorum  = cqltype.EachQuorum
	Serial      = cqltype.Serial
	LocalSerial = cqltype.LocalSerial
	LocalOne    = cqltype.LocalOne
)

// Option distinguishes an explicit zero value from an inherited setting.
// Its zero value inherits the setting; Set=true applies Value.
type Option[T any] = cqltype.Option[T]

// Some returns an Option set to v.
func Some[T any](v T) Option[T] { return cqltype.Some(v) }
