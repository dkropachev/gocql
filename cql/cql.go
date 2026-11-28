// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package cql holds the CQL data vocabulary: statements, types, values,
// codecs and consistency levels, and the errors of its own values and codecs.
// It does no I/O.
//
// A Statement pairs CQL text with a ParamEncoder and a RowDecoder. Params,
// Param and ParamAt map parameters; Row, RowInto, Column, ColumnAt and
// OneColumn map buffered result rows; ParamsOf and RowOf map the fields of a
// struct, deriving each field's codec from its Go type: built-in codecs for
// the scalar types, a type's own CQLCodec method, and slices, arrays,
// pointers, maps, Nullable and Pair built from those. CodecOf returns that
// codec for one type, and ParamOf and ColumnOf bind or decode one value
// with it. Select, Write and NewStatement build a statement, and
// WithIdempotent and WithBindMarkers return a copy with those fields set.
//
// ListCodec, SetCodec and VectorCodec encode and eagerly decode []T. MapEntriesCodec
// uses []Entry[K, V], preserving server order and supporting non-comparable
// keys; MapCodec uses a Go map[K]V. The corresponding Encoder and Decoder
// constructors provide either direction independently, with the same Go
// container types. Buffered collection decoding accepts NULL as an empty
// collection; use Nullable to preserve that distinction. Vectors require
// their declared dimension and use Nullable for NULL.
//
// Buffered Bytes and Text elements may borrow a page, including when held in
// an eagerly decoded slice. Clone recursively detaches borrowed values before
// their page is released. StringCodec and ByteSliceCodec decode owned copies,
// and a Varint or Decimal always owns its value.
//
// DataType and ColumnSpec describe public CQL types and columns. Option is for
// inherited settings, Nullable for nullable application data and results,
// BindValue for parameters that also need UNSET, and MaybeEmpty for supported
// empty CQL values. Their zero values mean inherited, NULL, UNSET, and
// ordinary zero value, respectively.
//
// ParseConsistency and ParseUUID return errors. Their MustParseConsistency and
// MustParseUUID forms panic and are useful for checked program constants.
//
// # Dynamic values
//
// Value holds a CQL value whose type is known only from metadata at run time.
// It carries its DataType and cell state explicitly; the constructors
// (BooleanValue, TextValue, ListValue, UDTValue, ...) validate the payload, so
// binding does not check it again. DynamicParams and NamedDynamicParams bind
// []Value and []ValueField, DynamicRows decodes a DynamicRow, and DynamicCodec
// encodes and decodes a single Value, as in Statement[[]Value, DynamicRow].
// A Value binds a cell by the rules of the typed codecs: an integer binds any
// integer type and a float either float type, with a range check, text and
// ascii bind each other, and a UDT value binds its UDT after fields were
// added, which bind NULL.
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
// NewStreamStatement builds one and Session.PrepareStream prepares it. Stream decoders write payloads directly
// into supplied storage and require enough capacity: seed buffers before
// decoding, and a decoder reports StreamCapacityError instead of growing
// them. Text, ascii, blob and varint values decode into []byte, so
// applications choose separate row structs when buffered and streaming field
// types differ. StreamString, StreamPointer, StreamMap,
// StreamTimestampTime, StreamTimeDuration and StreamDecimal allocate
// each value instead, for fields an application keeps.
//
// StreamList, StreamSet, StreamVector and StreamMapEntries decode into the same Go
// containers as ListCodec, SetCodec, VectorCodec and MapEntriesCodec, with streaming
// element decoders. StreamNullable preserves NULL; unwrapped list, set and map
// decoders accept NULL as an empty collection, matching buffered decoding,
// and vectors reject it.
package cql
