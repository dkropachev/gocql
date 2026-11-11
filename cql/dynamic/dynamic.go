// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package dynamic binds and reads CQL values whose types are known only from
// metadata at run time. A Value carries its cql.DataType and cell state
// explicitly; the constructors validate the payload, so binding does not
// check it again. Params and NamedParams bind []Value and []Field, Rows
// decodes a Row, and Codec encodes and decodes a single Value, as in
// cql.Statement[[]dynamic.Value, dynamic.Row].
//
// Values decoded from a response may borrow its page: Cursor and Each values
// expire with their page, while One and Clone detach them. Constructors that
// take slices or bytes borrow them until the parameters are captured; keep
// them unchanged until then.
package dynamic

import (
	"net/netip"

	"github.com/scylladb/gocql/v2/cql"
	codec "github.com/scylladb/gocql/v2/internal/codec"
)

// Value is an explicitly typed CQL value: a type, a cell state (present,
// NULL, UNSET or empty) and the payload that type selects. Type and State
// report the first two. The accessors Bool, Signed, Float, Text, Bytes, UUID,
// Inet, Duration, Varint and Decimal return the payload of a present scalar
// of a matching type and an error otherwise; Elements, Entries and Fields
// iterate a collection, map, tuple or UDT. Clone detaches a decoded value
// from its page. The zero Value has no type and binds nowhere.
type Value = codec.Value

// Entry is one key and value of a map Value.
type Entry = codec.ValueEntry

// Field is one named field of a UDT Value, or one named parameter of
// NamedParams.
type Field = codec.ValueField

// Iterator walks the members of a collection Value. Next advances and
// reports whether a member is available, Value returns it, and Err reports
// the failure that stopped iteration. Collect reads the remaining members
// into a slice, detached from the page.
type Iterator[T any] = codec.ValueIterator[T]

// Row holds columns and metadata-driven values. Columns returns the result
// metadata, At and Get return a column's Value by position and name, and
// Clone detaches the row. Cursor and Each values may borrow the response;
// One detaches the row before returning.
type Row = codec.DynamicRow

var (
	// Codec checks and serializes the explicit variant selected by
	// Value.Type, and decodes any cell into a Value.
	Codec cql.Codec[Value] = codec.DynamicCodec
	// Params binds explicitly typed values in physical marker order.
	Params cql.ParamEncoder[[]Value] = codec.DynamicParams
	// NamedParams maps unique input names to server marker order. Repeated
	// server markers reuse one input field; duplicate or unused input names
	// fail.
	NamedParams cql.ParamEncoder[[]Field] = codec.NamedDynamicParams
	// Rows compiles a lazy row decoder retaining the actual ordered metadata.
	Rows cql.RowDecoder[Row] = codec.DynamicRows
)

// Null constructs a NULL cell of t.
func Null(t cql.DataType) Value { return codec.NullValue(t) }

// Unset constructs an UNSET parameter of t; UNSET is not a valid result or nested value.
func Unset(t cql.DataType) Value { return codec.UnsetDynamic(t) }

// Empty constructs an empty cell of t, rejecting types that cannot represent it.
func Empty(t cql.DataType) (Value, error) { return codec.EmptyValue(t) }

// Bool constructs a dynamic CQL boolean value.
func Bool(b bool) Value { return codec.BoolValue(b) }

// TinyInt constructs a dynamic CQL tinyint value.
func TinyInt(n int8) Value { return codec.TinyIntValue(n) }

// SmallInt constructs a dynamic CQL smallint value.
func SmallInt(n int16) Value { return codec.SmallIntValue(n) }

// Int constructs a dynamic CQL int value.
func Int(n int32) Value { return codec.IntValue(n) }

// BigInt constructs a dynamic CQL bigint value.
func BigInt(n int64) Value { return codec.BigIntValue(n) }

// Counter constructs a dynamic CQL counter value.
func Counter(n cql.Counter) Value { return codec.CounterValue(n) }

// Float constructs a dynamic CQL float value.
func Float(n float32) Value { return codec.FloatValue(n) }

// Double constructs a dynamic CQL double value.
func Double(n float64) Value { return codec.DoubleValue(n) }

// Varint constructs a dynamic CQL varint value.
func Varint(n cql.Varint) Value { return codec.VarintValue(n) }

// Decimal constructs a dynamic CQL decimal value.
func Decimal(n cql.Decimal) Value { return codec.DecimalValue(n) }

// Text constructs a dynamic CQL text value, rejecting invalid UTF-8.
func Text(s string) (Value, error) { return codec.TextValue(s) }

// ASCII constructs a dynamic CQL ascii value, rejecting non-ASCII bytes.
func ASCII(s string) (Value, error) { return codec.ASCIIValue(s) }

// Blob constructs a dynamic CQL blob value. It borrows b until parameter capture; keep b immutable until then.
func Blob(b []byte) Value { return codec.BlobValue(b) }

// UUID constructs a dynamic CQL uuid value.
func UUID(u cql.UUID) Value { return codec.UUIDValue(u) }

// TimeUUID constructs a dynamic CQL timeuuid value, rejecting a UUID that is
// not version 1.
func TimeUUID(u cql.TimeUUID) (Value, error) { return codec.TimeUUIDValue(u) }

// Inet constructs a dynamic CQL inet value, rejecting an invalid address or
// one with a zone.
func Inet(a netip.Addr) (Value, error) { return codec.InetValue(a) }

// Date constructs a dynamic CQL date value.
func Date(n cql.Date) Value { return codec.DateValue(n) }

// Time constructs a dynamic CQL time value, rejecting a time outside the day.
func Time(n cql.TimeOfDay) (Value, error) { return codec.TimeValue(n) }

// Timestamp constructs a dynamic CQL timestamp value.
func Timestamp(n cql.Timestamp) Value { return codec.TimestampValue(n) }

// Duration constructs a dynamic CQL duration value, rejecting components
// with mixed signs.
func Duration(d cql.Duration) (Value, error) { return codec.DurationValue(d) }

// List constructs a dynamic list value, validating its elements against t.
// The input slice and borrowed elements must remain immutable until captured; Clone detaches them.
func List(t cql.DataType, v []Value) (Value, error) { return codec.ListValue(t, v) }

// Set constructs a dynamic set value, validating its elements against t.
// The input slice and borrowed elements must remain immutable until captured; Clone detaches them.
func Set(t cql.DataType, v []Value) (Value, error) { return codec.SetValue(t, v) }

// Map constructs a dynamic map value, validating its members against t.
// Input storage is borrowed until captured; Clone recursively detaches it.
func Map(t cql.DataType, entries []Entry) (Value, error) { return codec.MapValue(t, entries) }

// Vector constructs a dynamic vector value, checking its dimension and element types.
func Vector(t cql.DataType, v []Value) (Value, error) { return codec.VectorValue(t, v) }

// Tuple constructs a dynamic tuple value, validating its members against t.
// Input storage is borrowed until captured; Clone recursively detaches it.
func Tuple(t cql.DataType, v []Value) (Value, error) { return codec.TupleValue(t, v) }

// UDT constructs a dynamic UDT value, validating its members against t.
// Input storage is borrowed until captured; Clone recursively detaches it.
func UDT(t cql.DataType, fields []Field) (Value, error) { return codec.UDTValue(t, fields) }
