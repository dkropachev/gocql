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

const ()
