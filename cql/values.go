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

func ASCIIValue(a0 string) (Value, error) { return codec.ASCIIValue(a0) }

type BatchKind = cqltype.BatchKind

func BigIntValue(a0 int64) Value                             { return codec.BigIntValue(a0) }
func BindEncoder[T any](a0 Encoder[T]) Encoder[BindValue[T]] { return codec.BindEncoder[T](a0) }

type BindValue[T any] = cqltype.BindValue[T]

var BlobCodec = codec.BlobCodec

func BlobValue(a0 []byte) Value { return codec.BlobValue(a0) }

var Bool = codec.Bool

func BoolValue(a0 bool) Value            { return codec.BoolValue(a0) }
func BorrowBytes(a0 []byte) Bytes        { return cqltype.BorrowBytes(a0) }
func BorrowText(a0 string) (Text, error) { return cqltype.BorrowText(a0) }

type Bytes = cqltype.Bytes

var BytesEncoder = codec.BytesEncoder

type CASResult[R any] = codec.CASResult[R]

func CASRows[R any](a0 RowDecoder[R]) RowDecoder[CASResult[R]] { return codec.CASRows[R](a0) }

type CellState = cqltype.CellState
type Codec[T any] = codec.Codec[T]
type CodecError = cqltype.CodecError

func Collect[T any](a0 *ValueIterator[T]) ([]T, error) { return codec.Collect[T](a0) }

type CollectionType = cqltype.CollectionType

func Column[R any, V any](a0 string, a1 Decoder[V], a2 func(*R) *V) OutputField[R] {
	return codec.Column[R, V](a0, a1, a2)
}
func ColumnAt[R any, V any](a0 int, a1 Decoder[V], a2 func(*R) *V) OutputField[R] {
	return codec.ColumnAt[R, V](a0, a1, a2)
}

type ColumnInfo = cqltype.ColumnInfo
type ColumnSpec = cqltype.ColumnSpec
type Columns = cqltype.Columns

const ConditionalStatement = cqltype.ConditionalStatement

type Counter = codec.Counter

var CounterCodec = codec.CounterCodec

func CounterValue(a0 Counter) Value { return codec.CounterValue(a0) }

type DataType = cqltype.DataType
type Date = codec.Date

var DateCodec = codec.DateCodec

func DateFromTime(a0 time.Time) (Date, error) { return codec.DateFromTime(a0) }
func DateValue(a0 Date) Value                 { return codec.DateValue(a0) }

type Decimal = codec.Decimal

var DecimalCodec = codec.DecimalCodec

func DecimalValue(a0 Decimal) Value { return codec.DecimalValue(a0) }
func DecodeValue[T any](a0 Decoder[T], a1 EncodedValue) (T, error) {
	return codec.DecodeValue[T](a0, a1)
}

type Decoder[T any] = codec.Decoder[T]

func DoubleValue(a0 float64) Value { return codec.DoubleValue(a0) }

type Duration = cqltype.Duration

var DurationCodec = codec.DurationCodec

func DurationValue(a0 Duration) (Value, error) { return codec.DurationValue(a0) }

var DynamicCodec = codec.DynamicCodec
var DynamicParams = codec.DynamicParams

type DynamicRow = codec.DynamicRow

var DynamicRows = codec.DynamicRows

const EmptyCell = cqltype.EmptyCell

func EmptyCodec[T any](a0 Codec[T]) Codec[EmptyValueOf[T]] { return codec.EmptyCodec[T](a0) }
func EmptyValue(a0 DataType) (Value, error)                { return codec.EmptyValue(a0) }

type EmptyValueOf[T any] = cqltype.EmptyValueOf[T]

func EncodeValueBorrowed[T any](a0 DataType, a1 Encoder[T], a2 T) (EncodedValue, error) {
	return codec.EncodeValueBorrowed[T](a0, a1, a2)
}
func EncodeValueOwned[T any](a0 DataType, a1 Encoder[T], a2 T) (EncodedValue, error) {
	return codec.EncodeValueOwned[T](a0, a1, a2)
}

type EncodedValue = codec.EncodedValue
type Encoder[T any] = codec.Encoder[T]
type Entry[K any, V any] = codec.Entry[K, V]

var ErrReleased = cqltype.ErrReleased
var Float32 = codec.Float32
var Float64 = codec.Float64

func FloatValue(a0 float32) Value                  { return codec.FloatValue(a0) }
func IgnoreColumn[R any](a0 string) OutputField[R] { return codec.IgnoreColumn[R](a0) }

var InetCodec = codec.InetCodec

func InetValue(a0 netip.Addr) (Value, error) { return codec.InetValue(a0) }

type InputField[P any] = codec.InputField[P]

var Int16 = codec.Int16
var Int32 = codec.Int32
var Int64 = codec.Int64
var Int8 = codec.Int8

func IntValue(a0 int32) Value { return codec.IntValue(a0) }

func ListDecoder[T any](a0 Decoder[T]) Decoder[Sequence[T]] { return codec.ListDecoder[T](a0) }
func ListEncoder[T any](a0 Encoder[T]) Encoder[[]T]         { return codec.ListEncoder[T](a0) }
func ListType(a0 DataType) (DataType, error)                { return cqltype.ListType(a0) }
func ListValue(a0 DataType, a1 []Value) (Value, error)      { return codec.ListValue(a0, a1) }

func MapDecoder[K any, V any](a0 Decoder[K], a1 Decoder[V]) Decoder[Mapping[K, V]] {
	return codec.MapDecoder[K, V](a0, a1)
}
func MapEncoder[K any, V any](a0 Encoder[K], a1 Encoder[V]) Encoder[[]Entry[K, V]] {
	return codec.MapEncoder[K, V](a0, a1)
}
func MapParams[A any, B any](a0 ParamEncoder[B], a1 func(A) B) ParamEncoder[A] {
	return codec.MapParams[A, B](a0, a1)
}
func MapRows[A any, B any](a0 RowDecoder[A], a1 func(A) B, a2 func(B) B) RowDecoder[B] {
	return codec.MapRows[A, B](a0, a1, a2)
}
func MapType(a0 DataType, a1 DataType) (DataType, error)   { return cqltype.MapType(a0, a1) }
func MapValue(a0 DataType, a1 []ValueEntry) (Value, error) { return codec.MapValue(a0, a1) }

type Mapping[K any, V any] = codec.Mapping[K, V]

func MaxTimeUUID(a0 time.Time) UUID { return cqltype.MaxTimeUUID(a0) }
func MinTimeUUID(a0 time.Time) UUID { return cqltype.MinTimeUUID(a0) }
func MustRandomUUID() UUID          { return cqltype.MustRandomUUID() }

var NamedDynamicParams = codec.NamedDynamicParams

func NamedScalar[A any, B any](a0 Codec[B], a1 func(A) B, a2 func(B) A) Codec[A] {
	return codec.NamedScalar[A, B](a0, a1, a2)
}

type NativeType = cqltype.NativeType

func NewColumns(a0 ...ColumnSpec) Columns { return cqltype.NewColumns(a0...) }

func NewEncodedValue(a0 DataType, a1 CellState, a2 Bytes) (EncodedValue, error) {
	return codec.NewEncodedValue(a0, a1, a2)
}

func NewTimeUUID() UUID { return cqltype.NewTimeUUID() }

var EmptyParamsEncoder = codec.EmptyParamsEncoder
var EmptyRowDecoder = codec.EmptyRowDecoder

const Null = cqltype.Null

func NullValue(a0 DataType) Value { return codec.NullValue(a0) }

type Nullable[T any] = cqltype.Nullable[T]

func NullableCodec[T any](a0 Codec[T]) Codec[Nullable[T]]       { return codec.NullableCodec[T](a0) }
func NullableDecoder[T any](a0 Decoder[T]) Decoder[Nullable[T]] { return codec.NullableDecoder[T](a0) }
func NullableEncoder[T any](a0 Encoder[T]) Encoder[Nullable[T]] { return codec.NullableEncoder[T](a0) }

func OneColumn[T any](a0 Decoder[T]) RowDecoder[T]  { return codec.OneColumn[T](a0) }
func OneParam[T any](a0 Encoder[T]) ParamEncoder[T] { return codec.OneParam[T](a0) }

const OrdinaryStatement = cqltype.OrdinaryStatement

type OutputField[R any] = codec.OutputField[R]
type Pair[A any, B any] = codec.Pair[A, B]

func PairParams[A any, B any](a0 int, a1 ParamEncoder[A], a2 ParamEncoder[B]) ParamEncoder[Pair[A, B]] {
	return codec.PairParams[A, B](a0, a1, a2)
}
func PairRows[A any, B any](a0 int, a1 RowDecoder[A], a2 RowDecoder[B]) RowDecoder[Pair[A, B]] {
	return codec.PairRows[A, B](a0, a1, a2)
}
func Param[P any, V any](a0 string, a1 Encoder[V], a2 func(P) V) InputField[P] {
	return codec.Param[P, V](a0, a1, a2)
}
func ParamAt[P any, V any](a0 int, a1 Encoder[V], a2 func(P) V) InputField[P] {
	return codec.ParamAt[P, V](a0, a1, a2)
}

type ParamEncoder[P any] = codec.ParamEncoder[P]
type ParamPlan[P any] = codec.ParamPlan[P]
type ParamWriter = codec.ParamWriter

func Params[P any](a0 ...InputField[P]) ParamEncoder[P] { return codec.Params[P](a0...) }
func ParseUUID(a0 string) (UUID, error)                 { return cqltype.ParseUUID(a0) }
func ParseUUIDMust(a0 string) UUID                      { return cqltype.ParseUUIDMust(a0) }

type PayloadEntry = model.PayloadEntry

const Present = cqltype.Present

type ProtocolError = cqltype.ProtocolError

func RandomUUID() (UUID, error)                                 { return cqltype.RandomUUID() }
func ReadColumn[T any](a0 *RowReader, a1 Decoder[T]) (T, error) { return codec.ReadColumn[T](a0, a1) }
func ReadStreamColumn[T any](a0 *StreamRowReader, a1 StreamValuePlan[T], a2 *T) error {
	return codec.ReadStreamColumn[T](a0, a1, a2)
}

func Row[R any](a0 ...OutputField[R]) RowDecoder[R] { return codec.Row[R](a0...) }

type RowDecoder[R any] = codec.RowDecoder[R]
type RowPlan[R any] = codec.RowPlan[R]
type RowReader = codec.RowReader

func ScalarType(a0 Type) (DataType, error) { return cqltype.ScalarType(a0) }

type Sequence[T any] = codec.Sequence[T]

func SetDecoder[T any](a0 Decoder[T]) Decoder[Sequence[T]] { return codec.SetDecoder[T](a0) }
func SetEncoder[T any](a0 Encoder[T]) Encoder[[]T]         { return codec.SetEncoder[T](a0) }
func SetType(a0 DataType) (DataType, error)                { return cqltype.SetType(a0) }
func SetValue(a0 DataType, a1 []Value) (Value, error)      { return codec.SetValue(a0, a1) }
func SmallIntValue(a0 int16) Value                         { return codec.SmallIntValue(a0) }

type StatementKind = cqltype.StatementKind

var StreamASCII = codec.StreamASCII
var StreamBlob = codec.StreamBlob
var StreamBool = codec.StreamBool

type StreamCapabilityError = codec.StreamCapabilityError
type StreamCapacityError = codec.StreamCapacityError

func StreamColumn[R any, V any](a0 string, a1 StreamDecoder[V], a2 func(*R) *V) StreamOutputField[R] {
	return codec.StreamColumn[R, V](a0, a1, a2)
}
func StreamColumnAt[R any, V any](a0 int, a1 StreamDecoder[V], a2 func(*R) *V) StreamOutputField[R] {
	return codec.StreamColumnAt[R, V](a0, a1, a2)
}

var StreamCounter = codec.StreamCounter
var StreamDate = codec.StreamDate
var StreamDecimal = codec.StreamDecimal

type StreamDecimalValue = codec.StreamDecimalValue
type StreamDecoder[T any] = codec.StreamDecoder[T]

var StreamDuration = codec.StreamDuration

func EmptyStream[T any](a0 StreamDecoder[T]) StreamDecoder[EmptyValueOf[T]] {
	return codec.EmptyStream[T](a0)
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

func StreamList[T any](a0 StreamDecoder[T]) StreamDecoder[[]T] { return codec.StreamList[T](a0) }
func StreamMap[K any, V any](a0 StreamDecoder[K], a1 StreamDecoder[V]) StreamDecoder[[]StreamMapEntry[K, V]] {
	return codec.StreamMap[K, V](a0, a1)
}

type StreamMapEntry[K any, V any] = codec.StreamMapEntry[K, V]

func StreamNullable[T any](a0 StreamDecoder[T]) StreamDecoder[Nullable[T]] {
	return codec.StreamNullable[T](a0)
}
func StreamOneColumn[T any](a0 StreamDecoder[T]) StreamRowDecoder[T] {
	return codec.StreamOneColumn[T](a0)
}

type StreamOutputField[R any] = codec.StreamOutputField[R]
type StreamResetter[T any] = codec.StreamResetter[T]

func StreamRow[R any](a0 func(*R), a1 ...StreamOutputField[R]) StreamRowDecoder[R] {
	return codec.StreamRow[R](a0, a1...)
}

type StreamRowDecoder[R any] = codec.StreamRowDecoder[R]
type StreamRowPlan[R any] = codec.StreamRowPlan[R]
type StreamRowReader = codec.StreamRowReader

func StreamSet[T any](a0 StreamDecoder[T]) StreamDecoder[[]T] { return codec.StreamSet[T](a0) }

var StreamText = codec.StreamText
var StreamTime = codec.StreamTime
var StreamTimeUUID = codec.StreamTimeUUID
var StreamTimestamp = codec.StreamTimestamp

func StreamTuple[R any](a0 StreamRowDecoder[R]) StreamDecoder[R] { return codec.StreamTuple[R](a0) }
func StreamUDT[R any](a0 UDTOptions, a1 StreamRowDecoder[R]) StreamDecoder[R] {
	return codec.StreamUDT[R](a0, a1)
}

var StreamUUID = codec.StreamUUID

type StreamValuePlan[T any] = codec.StreamValuePlan[T]
type StreamValueReader = codec.StreamValueReader

var StreamVarint = codec.StreamVarint

func StreamVector[T any](a0 StreamDecoder[T]) StreamDecoder[[]T] { return codec.StreamVector[T](a0) }

var StringEncoder = codec.StringEncoder

type Text = cqltype.Text

var TextCodec = codec.TextCodec

func TextValue(a0 string) (Value, error) { return codec.TextValue(a0) }

var TimeCodec = codec.TimeCodec

type TimeOfDay = codec.TimeOfDay
type TimeUUID = codec.TimeUUID

var TimeUUIDCodec = codec.TimeUUIDCodec

func TimeUUIDValue(a0 TimeUUID) (Value, error)         { return codec.TimeUUIDValue(a0) }
func TimeUUIDWith(a0 int64, a1 uint32, a2 []byte) UUID { return cqltype.TimeUUIDWith(a0, a1, a2) }
func TimeValue(a0 TimeOfDay) (Value, error)            { return codec.TimeValue(a0) }

type Timestamp = codec.Timestamp

var TimestampCodec = codec.TimestampCodec

func TimestampFromTime(a0 time.Time) (Timestamp, error) { return codec.TimestampFromTime(a0) }
func TimestampValue(a0 Timestamp) Value                 { return codec.TimestampValue(a0) }
func TinyIntValue(a0 int8) Value                        { return codec.TinyIntValue(a0) }
func TupleDecoder[T any](a0 RowDecoder[T]) Decoder[T]   { return codec.TupleDecoder[T](a0) }
func TupleEncoder[T any](a0 ParamEncoder[T]) Encoder[T] { return codec.TupleEncoder[T](a0) }
func TupleType(a0 ...DataType) (DataType, error)        { return cqltype.TupleType(a0...) }

type TupleTypeInfo = cqltype.TupleTypeInfo

func TupleValue(a0 DataType, a1 []Value) (Value, error) { return codec.TupleValue(a0, a1) }

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

type TypeInfo = cqltype.TypeInfo

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

func UDTDecoder[T any](a0 RowDecoder[T], a1 UDTOptions) Decoder[T] {
	return codec.UDTDecoder[T](a0, a1)
}
func UDTEncoder[T any](a0 ParamEncoder[T], a1 UDTOptions) Encoder[T] {
	return codec.UDTEncoder[T](a0, a1)
}

type UDTField = cqltype.UDTField
type UDTOptions = codec.UDTOptions

func UDTType(a0 string, a1 string, a2 ...ColumnSpec) (DataType, error) {
	return cqltype.UDTType(a0, a1, a2...)
}

type UDTTypeInfo = cqltype.UDTTypeInfo

func UDTValue(a0 DataType, a1 []ValueField) (Value, error) { return codec.UDTValue(a0, a1) }

type UUID = cqltype.UUID

var UUIDCodec = codec.UUIDCodec

func UUIDFromBytes(a0 []byte) (UUID, error) { return cqltype.UUIDFromBytes(a0) }
func UUIDFromTime(a0 time.Time) UUID        { return cqltype.UUIDFromTime(a0) }
func UUIDValue(a0 UUID) Value               { return codec.UUIDValue(a0) }

const UnknownStatement = cqltype.UnknownStatement
const Unset = cqltype.Unset

func UnsetDynamic(a0 DataType) Value { return codec.UnsetDynamic(a0) }

type ValidationError = cqltype.ValidationError
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

func VarintFromBigInt(a0 *big.Int) Varint                     { return codec.VarintFromBigInt(a0) }
func VarintFromBytes(a0 []byte) (Varint, error)               { return codec.VarintFromBytes(a0) }
func VarintValue(a0 Varint) Value                             { return codec.VarintValue(a0) }
func VectorDataType(a0 DataType, a1 uint16) (DataType, error) { return cqltype.VectorDataType(a0, a1) }
func VectorDecoder[T any](a0 Decoder[T]) Decoder[Sequence[T]] { return codec.VectorDecoder[T](a0) }
func VectorEncoder[T any](a0 Encoder[T]) Encoder[[]T]         { return codec.VectorEncoder[T](a0) }

type VectorType = cqltype.VectorType

func VectorValue(a0 DataType, a1 []Value) (Value, error) { return codec.VectorValue(a0, a1) }
func WriteParam[T any](a0 *ParamWriter, a1 Encoder[T], a2 T) error {
	return codec.WriteParam[T](a0, a1, a2)
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
