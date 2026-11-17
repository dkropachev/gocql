// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package cql holds the CQL data vocabulary: statements, types, values,
// codecs and consistency levels, and the errors of its own values and codecs.
// It does no I/O.
//
// A Statement pairs CQL text with a ParamEncoder and a RowDecoder. Params,
// Param and ParamAt map parameters; Row, RowInto, Column, ColumnAt and
// OneColumn map buffered result rows; ParamsOf and RowOf map the fields of a
// struct.
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
//
// # Dynamic values
//
// Value holds a CQL value whose type is known only from metadata at run time.
// It carries its DataType and cell state explicitly; the constructors
// (BoolValue, TextValue, ListValue, UDTValue, ...) validate the payload, so
// binding does not check it again. DynamicParams and NamedDynamicParams bind
// []Value and []ValueField, DynamicRows decodes a DynamicRow, and DynamicCodec
// encodes and decodes a single Value, as in Statement[[]Value, DynamicRow].
//
// Values decoded from a response may borrow its page: Rows, Page and Each values
// expire with their page, while One and Clone detach them. Constructors that
// take slices or bytes borrow them until the parameters are captured; keep
// them unchanged until then.
//
// # Streaming
//
// The Stream declarations decode streamed rows into caller-owned storage.
// They are the streaming counterparts of the buffered decoders:
// StreamRowInto, StreamRowOf, StreamColumn, StreamColumnAt, StreamOneColumn,
// StreamTuple and StreamUDT mirror RowInto, RowOf, Column, ColumnAt,
// OneColumn, TupleDecoder and UDTDecoder. Both share column matching and
// struct tags, but compile separate plans because their storage and lifetime
// contracts differ.
//
// A StreamStatement pairs CQL and a ParamEncoder with a StreamRowDecoder;
// Session.PrepareStream prepares it. Stream decoders write payloads directly
// into supplied storage and require enough capacity: seed buffers before
// decoding, and a decoder reports StreamCapacityError instead of growing
// them. Text, ascii, blob and varint values decode into []byte, so
// applications choose separate row structs when buffered and streaming field
// types differ.
//
// StreamList, StreamSet, StreamVector and StreamMap decode into the same Go
// containers as ListCodec, SetCodec, VectorCodec and MapCodec, with streaming
// element decoders. StreamNullable preserves NULL; unwrapped list, set and map
// decoders accept NULL as an empty collection, matching buffered decoding,
// and vectors reject it.
package cql
