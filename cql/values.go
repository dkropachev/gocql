// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"math/big"
	"net/netip"
	"time"

	codec "github.com/scylladb/gocql/v2/internal/codec"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	model "github.com/scylladb/gocql/v2/internal/model"
)

var ASCIICodec = codec.ASCIICodec
var ASCIIEncoder = codec.ASCIIEncoder

// ASCIIValue constructs a dynamic CQL ASCII value.
func ASCIIValue(s string) (Value, error) { return codec.ASCIIValue(s) }

type BatchKind = cqltype.BatchKind

// BigIntValue constructs a dynamic CQL BigInt value.
func BigIntValue(n int64) Value { return codec.BigIntValue(n) }

// BindEncoder wraps base to encode Present, Null and Unset parameter states.
func BindEncoder[T any](base Encoder[T]) Encoder[BindValue[T]] { return codec.BindEncoder[T](base) }

// BindValue controls parameters using Present, Null, or Unset.
// Its zero value binds the ordinary zero value of T.
type BindValue[T any] = cqltype.BindValue[T]

var BlobCodec = codec.BlobCodec

// BlobValue constructs a dynamic CQL Blob value. It borrows b until parameter capture; keep b immutable until then.
func BlobValue(b []byte) Value { return codec.BlobValue(b) }

var Bool = codec.Bool

// BoolValue constructs a dynamic CQL Bool value.
func BoolValue(b bool) Value { return codec.BoolValue(b) }

// BorrowBytes borrows b without copying. The caller must keep it immutable until
// all operations using the resulting view have completed.
func BorrowBytes(b []byte) Bytes { return cqltype.BorrowBytes(b) }

// BorrowText borrows a valid UTF-8 string, rejecting malformed input without a panic.
func BorrowText(s string) (Text, error) { return cqltype.BorrowText(s) }

// Bytes is an immutable view of response bytes. Cursor views expire on advancement or Close
// unless a PageLease pins the page; One detaches them. Clone creates an owned copy.
type Bytes = cqltype.Bytes

var BytesEncoder = codec.BytesEncoder

type CASResult[R any] = codec.CASResult[R]

// CASRows decodes the [applied] column and the conditional result with decoder.
func CASRows[R any](decoder RowDecoder[R]) RowDecoder[CASResult[R]] { return codec.CASRows[R](decoder) }

type CellState = cqltype.CellState

// Codec combines typed encoding and decoding for one CQL value.
type Codec[T any] = codec.Codec[T]

// Deprecated: use (*ValueIterator[T]).Collect.
func Collect[T any](iterator *ValueIterator[T]) ([]T, error) { return iterator.Collect() }

// Column maps the named result column through valueCodec into the address returned by field.
// The accessor must return a non-nil address within the destination and must not retain it.
func Column[R any, V any](name string, valueCodec Decoder[V], field func(*R) *V) OutputField[R] {
	return codec.Column[R, V](name, valueCodec, field)
}

// ColumnAt maps the zero-based result column through valueCodec into field.
func ColumnAt[R any, V any](index int, valueCodec Decoder[V], field func(*R) *V) OutputField[R] {
	return codec.ColumnAt[R, V](index, valueCodec, field)
}

type ColumnSpec = cqltype.ColumnSpec
type Columns = cqltype.Columns

const ConditionalStatement = cqltype.ConditionalStatement

type Counter = codec.Counter

var CounterCodec = codec.CounterCodec

// CounterValue constructs a dynamic CQL Counter value.
func CounterValue(n Counter) Value { return codec.CounterValue(n) }

type DataType = cqltype.DataType
type Date = codec.Date

var DateCodec = codec.DateCodec

// DateFromTime converts a time to a CQL date, rejecting dates outside the supported range.
func DateFromTime(t time.Time) (Date, error) { return codec.DateFromTime(t) }

// DateValue constructs a dynamic CQL Date value.
func DateValue(n Date) Value { return codec.DateValue(n) }

type Decimal = codec.Decimal

var DecimalCodec = codec.DecimalCodec

// DecimalValue constructs a dynamic CQL Decimal value.
func DecimalValue(n Decimal) Value { return codec.DecimalValue(n) }

// DecodeValue checks the requested type before invoking the decoder. Unset is
// an input-binding state and is never admitted as a result value.
func DecodeValue[T any](decoder Decoder[T], value EncodedValue) (T, error) {
	return codec.DecodeValue[T](decoder, value)
}

// Decoder decodes one checked CQL value. Clone recursively detaches borrowed storage.
// Custom decoders must implement this contract even when used only through One.
type Decoder[T any] = codec.Decoder[T]

// DoubleValue constructs a dynamic CQL Double value.
func DoubleValue(n float64) Value { return codec.DoubleValue(n) }

type Duration = cqltype.Duration

var DurationCodec = codec.DurationCodec

// DurationValue constructs a dynamic CQL Duration value.
func DurationValue(d Duration) (Value, error) { return codec.DurationValue(d) }

var DynamicCodec = codec.DynamicCodec
var DynamicParams = codec.DynamicParams

// DynamicRow holds columns and metadata-driven values. Cursor and Each values may borrow
// the response; Clone detaches them and One detaches the row before returning.
type DynamicRow = codec.DynamicRow

var DynamicRows = codec.DynamicRows

const EmptyCell = cqltype.EmptyCell

// EmptyCodec adds the explicit empty-cell state to base; it does not add NULL support.
func EmptyCodec[T any](base Codec[T]) Codec[EmptyValueOf[T]] { return codec.EmptyCodec[T](base) }

// EmptyValue constructs an empty cell of t, rejecting types that cannot represent it.
func EmptyValue(t DataType) (Value, error) { return codec.EmptyValue(t) }

// EmptyValueOf distinguishes an empty CQL value from an ordinary value.
// Its zero value encodes the ordinary zero value of T; Empty=true selects empty.
type EmptyValueOf[T any] = cqltype.EmptyValueOf[T]

// EncodeValueBorrowed encodes one value without copying borrowed input payloads.
// The caller must keep those payloads immutable while the encoded value is used.
func EncodeValueBorrowed[T any](typ DataType, encoder Encoder[T], value T) (EncodedValue, error) {
	return codec.EncodeValueBorrowed[T](typ, encoder, value)
}

// EncodeValue encodes one value and copies every borrowed input payload, so the
// result does not depend on the caller's buffers. Use EncodeValueBorrowed to
// skip the copy.
func EncodeValue[T any](typ DataType, encoder Encoder[T], value T) (EncodedValue, error) {
	return codec.EncodeValue[T](typ, encoder, value)
}

type EncodedValue = codec.EncodedValue
type Encoder[T any] = codec.Encoder[T]
type Entry[K any, V any] = codec.Entry[K, V]

var Float32 = codec.Float32
var Float64 = codec.Float64

// FloatValue constructs a dynamic CQL Float value.
func FloatValue(n float32) Value { return codec.FloatValue(n) }

// IgnoreColumn consumes the named result column without storing it.
func IgnoreColumn[R any](name string) OutputField[R] { return codec.IgnoreColumn[R](name) }

var InetCodec = codec.InetCodec

// InetValue constructs a dynamic CQL Inet value.
func InetValue(a netip.Addr) (Value, error) { return codec.InetValue(a) }

type InputField[P any] = codec.InputField[P]

var Int16 = codec.Int16
var Int32 = codec.Int32
var Int64 = codec.Int64
var Int8 = codec.Int8

// IntValue constructs a dynamic CQL Int value.
func IntValue(n int32) Value { return codec.IntValue(n) }

// ListDecoder eagerly decodes every element into a []T. Elements may borrow
// page storage; Clone recursively detaches them.
func ListDecoder[T any](element Decoder[T]) Decoder[[]T] { return codec.ListDecoder[T](element) }

// ListEncoder encodes a slice as a CQL list using element.
func ListEncoder[T any](element Encoder[T]) Encoder[[]T] { return codec.ListEncoder[T](element) }

// ListType constructs a CQL list type with the supplied element type.
func ListType(element DataType) (DataType, error) { return cqltype.ListType(element) }

// ListValue constructs a dynamic list value, validating its elements against t.
// The input slice and borrowed elements must remain immutable until captured; Clone detaches them.
func ListValue(t DataType, v []Value) (Value, error) { return codec.ListValue(t, v) }

// MapDecoder eagerly decodes ordered entries; keys need not be Go-comparable.
// Entries may contain borrowed values; Clone recursively detaches them.
func MapDecoder[K any, V any](key Decoder[K], value Decoder[V]) Decoder[[]Entry[K, V]] {
	return codec.MapDecoder[K, V](key, value)
}

// MapEncoder encodes ordered entries as a CQL map; keys need not be Go-comparable.
func MapEncoder[K any, V any](key Encoder[K], value Encoder[V]) Encoder[[]Entry[K, V]] {
	return codec.MapEncoder[K, V](key, value)
}

// MapParams adapts an encoder for B by mapping each input A to B.
func MapParams[A any, B any](valueCodec ParamEncoder[B], mapValue func(A) B) ParamEncoder[A] {
	return codec.MapParams[A, B](valueCodec, mapValue)
}

// MapRows maps decoded A rows to B; cloneValue must recursively detach borrowed data in B.
func MapRows[A any, B any](valueCodec RowDecoder[A], mapValue func(A) B, cloneValue func(B) B) RowDecoder[B] {
	return codec.MapRows[A, B](valueCodec, mapValue, cloneValue)
}

// MapType constructs a CQL Map type from its complete type description.
func MapType(key DataType, value DataType) (DataType, error) { return cqltype.MapType(key, value) }

// MapValue constructs a dynamic Map value, validating its members against t.
// Input storage is borrowed until captured; Clone recursively detaches it.
func MapValue(t DataType, entries []ValueEntry) (Value, error) { return codec.MapValue(t, entries) }

// MaxTimeUUID generates a "fake" time based UUID (version 1) which will be
// the biggest possible UUID generated for the provided timestamp.
//
// UUIDs generated by this function are not unique and are mostly suitable only
// in queries to select a time range of a Cassandra's TimeUUID column.
func MaxTimeUUID(t time.Time) UUID { return cqltype.MaxTimeUUID(t) }

// MinTimeUUID generates a "fake" time based UUID (version 1) which will be
// the smallest possible UUID generated for the provided timestamp.
//
// UUIDs generated by this function are not unique and are mostly suitable only
// in queries to select a time range of a Cassandra's TimeUUID column.
func MinTimeUUID(t time.Time) UUID { return cqltype.MinTimeUUID(t) }

// MustRandomUUID returns RandomUUID or panics if the random source fails.
func MustRandomUUID() UUID { return cqltype.MustRandomUUID() }

var NamedDynamicParams = codec.NamedDynamicParams

// NamedScalar adapts a named application type through explicitly typed conversions.
func NamedScalar[A any, B any](base Codec[B], to func(A) B, from func(B) A) Codec[A] {
	return codec.NamedScalar[A, B](base, to, from)
}

// NewColumns snapshots column specifications into immutable metadata.
func NewColumns(columns ...ColumnSpec) Columns { return cqltype.NewColumns(columns...) }

// NewEncodedValue checks the descriptor and state/body framing. Scalar content
// is checked by the selected decoder when DecodeValue is called.
func NewEncodedValue(typ DataType, state CellState, body Bytes) (EncodedValue, error) {
	return codec.NewEncodedValue(typ, state, body)
}

// NewTimeUUID generates a new time based UUID (version 1) using the current
// time as the timestamp.
func NewTimeUUID() UUID { return cqltype.NewTimeUUID() }

var EmptyParamsEncoder = codec.EmptyParamsEncoder
var EmptyRowDecoder = codec.EmptyRowDecoder

const Null = cqltype.Null

// NullValue constructs a NULL cell of t.
func NullValue(t DataType) Value { return codec.NullValue(t) }

// Nullable represents a value or CQL NULL; its zero value is NULL.
// It supports result decoding and binding nullable application fields.
// Use BindValue for parameters that also need UNSET.
type Nullable[T any] = cqltype.Nullable[T]

// NullableCodec adds NULL encoding and decoding to base.
func NullableCodec[T any](base Codec[T]) Codec[Nullable[T]] { return codec.NullableCodec[T](base) }

// NullableDecoder preserves NULL separately from the decoded zero value.
func NullableDecoder[T any](base Decoder[T]) Decoder[Nullable[T]] {
	return codec.NullableDecoder[T](base)
}

// NullableEncoder encodes invalid Nullable values as NULL and valid values with base.
func NullableEncoder[T any](base Encoder[T]) Encoder[Nullable[T]] {
	return codec.NullableEncoder[T](base)
}

// OneColumn decodes exactly one result column using decoder.
func OneColumn[T any](decoder Decoder[T]) RowDecoder[T] { return codec.OneColumn[T](decoder) }

// OneParam encodes one positional bind marker using encoder.
func OneParam[T any](encoder Encoder[T]) ParamEncoder[T] { return codec.OneParam[T](encoder) }

const OrdinaryStatement = cqltype.OrdinaryStatement

type OutputField[R any] = codec.OutputField[R]
type Pair[A any, B any] = codec.Pair[A, B]

// PairParams combines two encoders, assigning the first leftColumns parameter columns to a.
func PairParams[A any, B any](leftColumns int, a ParamEncoder[A], b ParamEncoder[B]) ParamEncoder[Pair[A, B]] {
	return codec.PairParams[A, B](leftColumns, a, b)
}

// PairRows combines two decoders, assigning the first leftColumns result columns to a.
func PairRows[A any, B any](leftColumns int, a RowDecoder[A], b RowDecoder[B]) RowDecoder[Pair[A, B]] {
	return codec.PairRows[A, B](leftColumns, a, b)
}

// Param maps a named bind marker through valueCodec from the value returned by get.
func Param[P any, V any](name string, valueCodec Encoder[V], get func(P) V) InputField[P] {
	return codec.Param[P, V](name, valueCodec, get)
}

// ParamAt maps a zero-based bind marker through valueCodec from get.
func ParamAt[P any, V any](index int, valueCodec Encoder[V], get func(P) V) InputField[P] {
	return codec.ParamAt[P, V](index, valueCodec, get)
}

// ParamEncoder compiles a reusable parameter plan against bind-marker metadata.
type ParamEncoder[P any] = codec.ParamEncoder[P]

// ParamPlan encodes typed parameters in server order using ParamWriter.
type ParamPlan[P any] = codec.ParamPlan[P]
type ParamWriter = codec.ParamWriter

// Params builds a typed parameter encoder from field mappings. Every bind marker must be mapped.
func Params[P any](fields ...InputField[P]) ParamEncoder[P] { return codec.Params[P](fields...) }

// ParseUUID parses a 32 digit hexadecimal number (that might contain hypens)
// representing an UUID.
func ParseUUID(input string) (UUID, error) { return cqltype.ParseUUID(input) }

// MustParseUUID is ParseUUID that panics on error.
func MustParseUUID(input string) UUID { return cqltype.MustParseUUID(input) }

type PayloadEntry = model.PayloadEntry

const Present = cqltype.Present

// RandomUUID generates a totally random UUID (version 4) as described in
// RFC 4122.
func RandomUUID() (UUID, error) { return cqltype.RandomUUID() }

// Deprecated: use (*RowReader).ReadColumn.
func ReadColumn[T any](reader *RowReader, decoder Decoder[T]) (T, error) {
	return reader.ReadColumn(decoder)
}

// Deprecated: use (*StreamRowReader).ReadColumn.
func ReadStreamColumn[T any](reader *StreamRowReader, plan StreamValuePlan[T], dst *T) error {
	return reader.ReadColumn(plan, dst)
}

// Row builds a row decoder from field mappings. Borrowed fields follow the execution lifetime;
// One detaches them, Each lends them during its callback, and cursors lend them until advancement.
func Row[R any](fields ...OutputField[R]) RowDecoder[R] { return codec.Row[R](fields...) }

// RowDecoder compiles a reusable row plan against actual result metadata. Clone must
// recursively detach response storage, including nested fields; One relies on this contract.
type RowDecoder[R any] = codec.RowDecoder[R]

// RowPlan decodes checked rows into caller destinations. Clone must recursively detach
// all borrowed response storage; custom plans are cloned before One returns.
type RowPlan[R any] = codec.RowPlan[R]
type RowReader = codec.RowReader

// ScalarType returns the shared descriptor for a supported scalar kind. It
// fails only for non-scalar kinds; use the predeclared values such as TextType
// when the kind is known at compile time.
func ScalarType(kind Type) (DataType, error) { return cqltype.ScalarType(kind) }

// Predeclared scalar types. Each equals ScalarType of the matching Type
// constant. Treat them as constants: reassigning one changes only code that
// reads the variable, not ScalarType or decoded metadata.
var (
	ASCIIType     = cqltype.ASCIIType
	BigIntType    = cqltype.BigIntType
	BlobType      = cqltype.BlobType
	BooleanType   = cqltype.BooleanType
	CounterType   = cqltype.CounterType
	DecimalType   = cqltype.DecimalType
	DoubleType    = cqltype.DoubleType
	FloatType     = cqltype.FloatType
	IntType       = cqltype.IntType
	TextType      = cqltype.TextType
	TimestampType = cqltype.TimestampType
	UUIDType      = cqltype.UUIDType
	VarcharType   = cqltype.VarcharType
	VarintType    = cqltype.VarintType
	TimeUUIDType  = cqltype.TimeUUIDType
	InetType      = cqltype.InetType
	DateType      = cqltype.DateType
	TimeType      = cqltype.TimeType
	SmallIntType  = cqltype.SmallIntType
	TinyIntType   = cqltype.TinyIntType
	DurationType  = cqltype.DurationType
)

// SetDecoder eagerly decodes every element into a []T. Elements may borrow
// page storage; Clone recursively detaches them.
func SetDecoder[T any](element Decoder[T]) Decoder[[]T] { return codec.SetDecoder[T](element) }

// SetEncoder encodes a slice as a CQL set using element.
func SetEncoder[T any](element Encoder[T]) Encoder[[]T] { return codec.SetEncoder[T](element) }

// SetType constructs a CQL set type with the supplied element type.
func SetType(element DataType) (DataType, error) { return cqltype.SetType(element) }

// SetValue constructs a dynamic set value, validating its elements against t.
// The input slice and borrowed elements must remain immutable until captured; Clone detaches them.
func SetValue(t DataType, v []Value) (Value, error) { return codec.SetValue(t, v) }

// SmallIntValue constructs a dynamic CQL SmallInt value.
func SmallIntValue(n int16) Value { return codec.SmallIntValue(n) }

type StatementKind = cqltype.StatementKind

var StreamASCII = codec.StreamASCII
var StreamBlob = codec.StreamBlob
var StreamBool = codec.StreamBool

// StreamColumn maps a named result column into caller storage returned by field.
// Seed buffer capacity before decoding; stream decoders do not grow payload buffers.
func StreamColumn[R any, V any](name string, decoder StreamDecoder[V], field func(*R) *V) StreamOutputField[R] {
	return codec.StreamColumn[R, V](name, decoder, field)
}

// StreamColumnAt maps a zero-based result column into seeded caller storage returned by field.
func StreamColumnAt[R any, V any](index int, decoder StreamDecoder[V], field func(*R) *V) StreamOutputField[R] {
	return codec.StreamColumnAt[R, V](index, decoder, field)
}

var StreamCounter = codec.StreamCounter
var StreamDate = codec.StreamDate
var StreamDecimal = codec.StreamDecimal

type StreamDecimalValue = codec.StreamDecimalValue
type StreamDecoder[T any] = codec.StreamDecoder[T]

var StreamDuration = codec.StreamDuration

// EmptyStream adds empty-cell decoding to base while preserving caller buffer capacity.
func EmptyStream[T any](base StreamDecoder[T]) StreamDecoder[EmptyValueOf[T]] {
	return codec.EmptyStream[T](base)
}

var StreamEncoded = codec.StreamEncoded

type StreamEncodedValue = codec.StreamEncodedValue

var StreamFloat32 = codec.StreamFloat32
var StreamFloat64 = codec.StreamFloat64
var StreamInet = codec.StreamInet
var StreamInt16 = codec.StreamInt16
var StreamInt32 = codec.StreamInt32
var StreamInt64 = codec.StreamInt64
var StreamInt8 = codec.StreamInt8

// StreamList decodes a list into seeded slice slots using child. Seed each nested buffer too.
func StreamList[T any](child StreamDecoder[T]) StreamDecoder[[]T] { return codec.StreamList[T](child) }

// StreamMap decodes a map into seeded entry slots using key and value decoders.
func StreamMap[K any, V any](key StreamDecoder[K], value StreamDecoder[V]) StreamDecoder[[]Entry[K, V]] {
	return codec.StreamMap[K, V](key, value)
}

// StreamNullable preserves NULL while decoding valid values into caller storage with base.
func StreamNullable[T any](base StreamDecoder[T]) StreamDecoder[Nullable[T]] {
	return codec.StreamNullable[T](base)
}

// StreamOneColumn decodes one result column into seeded caller storage with decoder.
func StreamOneColumn[T any](decoder StreamDecoder[T]) StreamRowDecoder[T] {
	return codec.StreamOneColumn[T](decoder)
}

type StreamOutputField[R any] = codec.StreamOutputField[R]
type StreamResetter[T any] = codec.StreamResetter[T]

// StreamRow maps result columns into caller storage. reset clears application state before
// each row and must preserve mapped buffer capacity. Only a successful NextInto publishes a row.
func StreamRow[R any](reset func(*R), fields ...StreamOutputField[R]) StreamRowDecoder[R] {
	return codec.StreamRow[R](reset, fields...)
}

type StreamRowDecoder[R any] = codec.StreamRowDecoder[R]
type StreamRowPlan[R any] = codec.StreamRowPlan[R]
type StreamRowReader = codec.StreamRowReader

// StreamSet decodes a set into seeded slice slots using child, preserving server order.
func StreamSet[T any](child StreamDecoder[T]) StreamDecoder[[]T] { return codec.StreamSet[T](child) }

var StreamText = codec.StreamText
var StreamTime = codec.StreamTime
var StreamTimeUUID = codec.StreamTimeUUID
var StreamTimestamp = codec.StreamTimestamp

// StreamTuple decodes tuple fields into caller storage with decoder.
func StreamTuple[R any](decoder StreamRowDecoder[R]) StreamDecoder[R] {
	return codec.StreamTuple[R](decoder)
}

// StreamUDT decodes UDT fields into caller storage with decoder and explicit matching options.
func StreamUDT[R any](options UDTOptions, decoder StreamRowDecoder[R]) StreamDecoder[R] {
	return codec.StreamUDT[R](options, decoder)
}

var StreamUUID = codec.StreamUUID

type StreamValuePlan[T any] = codec.StreamValuePlan[T]
type StreamValueReader = codec.StreamValueReader

var StreamVarint = codec.StreamVarint

// StreamVector decodes a vector into seeded slice storage using child.
func StreamVector[T any](child StreamDecoder[T]) StreamDecoder[[]T] {
	return codec.StreamVector[T](child)
}

var StringEncoder = codec.StringEncoder

// StringCodec encodes a string and decodes text into a newly allocated
// string that stays valid after the row is released. Use TextCodec to decode
// without copying.
var StringCodec = codec.StringCodec

// ASCIIStringCodec is StringCodec for the ascii type.
var ASCIIStringCodec = codec.ASCIIStringCodec

// Text is a validated text view with the same lifetime as Bytes. String materializes
// an owned string; cursor callers must do so before the view expires.
type Text = cqltype.Text

var TextCodec = codec.TextCodec

// TextValue constructs a dynamic CQL Text value.
func TextValue(s string) (Value, error) { return codec.TextValue(s) }

var TimeCodec = codec.TimeCodec

type TimeOfDay = codec.TimeOfDay
type TimeUUID = codec.TimeUUID

var TimeUUIDCodec = codec.TimeUUIDCodec

// TimeUUIDValue constructs a dynamic CQL TimeUUID value.
func TimeUUIDValue(u TimeUUID) (Value, error) { return codec.TimeUUIDValue(u) }

// TimeUUIDWith generates a new time based UUID (version 1) as described in
// RFC4122 with given parameters. t is the number of 100's of nanoseconds
// since 15 Oct 1582 (60bits). clock is the number of clock sequence (14bits).
// node is a slice to gurarantee the uniqueness of the UUID (up to 6bytes).
// Note: calling this function does not increment the static clock sequence.
func TimeUUIDWith(t int64, clock uint32, node []byte) UUID {
	return cqltype.TimeUUIDWith(t, clock, node)
}

// TimeValue constructs a dynamic CQL Time value.
func TimeValue(n TimeOfDay) (Value, error) { return codec.TimeValue(n) }

type Timestamp = codec.Timestamp

var TimestampCodec = codec.TimestampCodec

// TimestampFromTime converts a time to a CQL millisecond timestamp, rejecting overflow.
func TimestampFromTime(t time.Time) (Timestamp, error) { return codec.TimestampFromTime(t) }

// TimestampValue constructs a dynamic CQL Timestamp value.
func TimestampValue(n Timestamp) Value { return codec.TimestampValue(n) }

// TinyIntValue constructs a dynamic CQL TinyInt value.
func TinyIntValue(n int8) Value { return codec.TinyIntValue(n) }

// TupleDecoder consumes tuple members inside one cell without expanding result
// columns. A missing serialized suffix is passed to member decoders as null.
func TupleDecoder[T any](valueCodec RowDecoder[T]) Decoder[T] {
	return codec.TupleDecoder[T](valueCodec)
}

// TupleEncoder encodes its parameters as members of one physical tuple cell.
func TupleEncoder[T any](valueCodec ParamEncoder[T]) Encoder[T] {
	return codec.TupleEncoder[T](valueCodec)
}

// TupleType constructs a CQL Tuple type from its complete type description.
func TupleType(fields ...DataType) (DataType, error) { return cqltype.TupleType(fields...) }

// TupleValue constructs a dynamic Tuple value, validating its members against t.
// Input storage is borrowed until captured; Clone recursively detaches it.
func TupleValue(t DataType, v []Value) (Value, error) { return codec.TupleValue(t, v) }

type Type = cqltype.Type

const TypeAscii = cqltype.TypeAscii
const TypeBigInt = cqltype.TypeBigInt
const TypeBlob = cqltype.TypeBlob
const TypeBoolean = cqltype.TypeBoolean
const TypeCounter = cqltype.TypeCounter
const TypeCustom = cqltype.TypeCustom
const TypeDate = cqltype.TypeDate
const TypeDecimal = cqltype.TypeDecimal
const TypeDouble = cqltype.TypeDouble
const TypeDuration = cqltype.TypeDuration
const TypeFloat = cqltype.TypeFloat
const TypeInet = cqltype.TypeInet

const TypeInt = cqltype.TypeInt
const TypeList = cqltype.TypeList
const TypeMap = cqltype.TypeMap
const TypeSet = cqltype.TypeSet
const TypeSmallInt = cqltype.TypeSmallInt
const TypeText = cqltype.TypeText
const TypeTime = cqltype.TypeTime
const TypeTimeUUID = cqltype.TypeTimeUUID
const TypeTimestamp = cqltype.TypeTimestamp
const TypeTinyInt = cqltype.TypeTinyInt
const TypeTuple = cqltype.TypeTuple
const TypeUDT = cqltype.TypeUDT
const TypeUUID = cqltype.TypeUUID
const TypeVarchar = cqltype.TypeVarchar
const TypeVarint = cqltype.TypeVarint

// UDTDecoder decodes UDT fields with valueCodec and explicit identity and matching options.
func UDTDecoder[T any](valueCodec RowDecoder[T], options UDTOptions) Decoder[T] {
	return codec.UDTDecoder[T](valueCodec, options)
}

// UDTEncoder matches fields against a complete UDT descriptor.
func UDTEncoder[T any](valueCodec ParamEncoder[T], options UDTOptions) Encoder[T] {
	return codec.UDTEncoder[T](valueCodec, options)
}

type UDTOptions = codec.UDTOptions

// UDTOptionsFor returns options naming typ's UDT, with every relaxation off.
// It fails when typ is not a UDT.
func UDTOptionsFor(typ DataType) (UDTOptions, error) { return codec.UDTOptionsFor(typ) }

// UDTType constructs a CQL UDT type from its complete type description.
func UDTType(keyspace string, name string, fields ...ColumnSpec) (DataType, error) {
	return cqltype.UDTType(keyspace, name, fields...)
}

// UDTValue constructs a dynamic UDT value, validating its members against t.
// Input storage is borrowed until captured; Clone recursively detaches it.
func UDTValue(t DataType, fields []ValueField) (Value, error) { return codec.UDTValue(t, fields) }

type UUID = cqltype.UUID

var UUIDCodec = codec.UUIDCodec

// UUIDFromBytes converts a raw byte slice to an UUID.
func UUIDFromBytes(input []byte) (UUID, error) { return cqltype.UUIDFromBytes(input) }

// UUIDFromTime generates a new time based UUID (version 1) as described in
// RFC 4122. This UUID contains the MAC address of the node that generated
// the UUID, the given timestamp and a sequence number.
func UUIDFromTime(t time.Time) UUID { return cqltype.UUIDFromTime(t) }

// UUIDValue constructs a dynamic CQL UUID value.
func UUIDValue(u UUID) Value { return codec.UUIDValue(u) }

const UnknownStatement = cqltype.UnknownStatement
const Unset = cqltype.Unset

// UnsetDynamic constructs an UNSET parameter of t; UNSET is not a valid result or nested value.
func UnsetDynamic(t DataType) Value { return codec.UnsetDynamic(t) }

type Value = codec.Value
type ValueCursor = codec.ValueCursor
type ValueEntry = codec.ValueEntry
type ValueField = codec.ValueField
type ValueIterator[T any] = codec.ValueIterator[T]
type ValueReader = codec.ValueReader
type ValueWriter = codec.ValueWriter

const VariantFuture = cqltype.VariantFuture
const VariantIETF = cqltype.VariantIETF
const VariantMicrosoft = cqltype.VariantMicrosoft
const VariantNCSCompat = cqltype.VariantNCSCompat

type Varint = codec.Varint

var VarintCodec = codec.VarintCodec

// VarintFromBigInt constructs a Varint from v without retaining its mutable big.Int storage.
func VarintFromBigInt(v *big.Int) Varint { return codec.VarintFromBigInt(v) }

// VarintFromBytes borrows a nonempty signed two's-complement big-endian representation.
// Keep the input immutable until parameter capture, or clone the result.
func VarintFromBytes(b []byte) (Varint, error) { return codec.VarintFromBytes(b) }

// VarintValue constructs a dynamic CQL Varint value.
func VarintValue(n Varint) Value { return codec.VarintValue(n) }

// VectorDataType constructs a vector type with an element type and fixed nonzero dimension.
func VectorDataType(element DataType, dimension uint16) (DataType, error) {
	return cqltype.VectorDataType(element, dimension)
}

// VectorDecoder eagerly decodes every element into a []T. Elements may borrow
// page storage; Clone recursively detaches them.
func VectorDecoder[T any](element Decoder[T]) Decoder[[]T] { return codec.VectorDecoder[T](element) }

// VectorEncoder encodes a vector of the declared dimension using element.
func VectorEncoder[T any](element Encoder[T]) Encoder[[]T] { return codec.VectorEncoder[T](element) }

// VectorValue constructs a dynamic vector value, checking its dimension and element types.
func VectorValue(t DataType, v []Value) (Value, error) { return codec.VectorValue(t, v) }

// Deprecated: use (*ParamWriter).WriteParam.
func WriteParam[T any](writer *ParamWriter, encoder Encoder[T], value T) error {
	return writer.WriteParam(encoder, value)
}

const (
	LoggedBatch   = cqltype.LoggedBatch
	UnloggedBatch = cqltype.UnloggedBatch
	CounterBatch  = cqltype.CounterBatch
)

// EmptyParams is the input type of a statement without bind markers.
type EmptyParams = codec.EmptyRecord

// EmptyRow is the row type of a statement without result columns.
type EmptyRow = codec.EmptyRecord
