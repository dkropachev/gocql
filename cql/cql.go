// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package cql holds the CQL data vocabulary: statements, types, values,
// codecs, consistency levels and server errors. It does no I/O.
//
// A Statement pairs CQL text with a ParamEncoder and a RowDecoder. Params,
// Param and ParamAt map parameters; Row, RowInto, Column, ColumnAt and
// OneColumn map buffered result rows; ParamsOf and RowOf map the fields of a
// struct. Two subpackages build on this one: package stream decodes rows
// incrementally into caller storage (stream.Statement, stream.RowInto,
// stream.RowOf), and package dynamic binds and reads values whose types are
// known only at run time (dynamic.Value, dynamic.Params, dynamic.Rows).
//
// ListCodec, SetCodec and VectorCodec encode and eagerly decode []T. MapCodec
// uses []Entry[K, V], preserving server order and supporting non-comparable
// keys; GoMapCodec uses a Go map[K]V. The corresponding Encoder and Decoder
// constructors provide either direction independently, with the same Go
// container types. Buffered collection decoding accepts NULL as an empty
// collection; use Nullable to preserve that distinction. Vectors require
// their declared dimension and use Nullable for NULL.
//
// Buffered Bytes and Text elements may borrow a page, including when held in
// an eagerly decoded slice. Clone recursively detaches borrowed values before
// their page is released. StringCodec and ByteSliceCodec decode owned copies.
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

// Unpaged is the page size that requests a result without paging, as in
// Options{PageSize: cql.Some(cql.Unpaged)} or Profile.PageSize. The request
// carries no page size, so the server returns the whole result in one
// response, within its own result size limits. Use it for results known to be
// small; a large unpaged result costs server and client memory at once.
const Unpaged = cqltype.Unpaged
