// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"fmt"
	"iter"
	"math"
	"net/netip"
	"sync"
	"time"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// Value is an explicitly typed CQL value: a type, a cell state (present,
// NULL, UNSET or empty) and the payload that type selects. Type and State
// report the first two. The accessors Bool, Signed, Float, Text, Bytes, UUID,
// TimeUUID, Inet, Time, TimeOfDay, Duration, Varint and Decimal return the
// payload of a present scalar of a matching type, and an error otherwise,
// which matches ErrNull for a NULL; Str copies text into a string. Elements,
// Entries and Fields range over a collection, map, tuple or UDT; Len counts
// its members and Field reads one UDT field. String prints the value as a
// CQL literal. Clone detaches a decoded value from its
// page.
//
// The constructors do not fail on a scalar payload: an invalid one, such as
// a TextValue that is not UTF-8 or an InetValue with a zone, fails when the
// value is bound. The zero Value has no type: binding it, or reading it with
// an accessor, fails with a ValidationError.
type Value struct {
	inet        netip.Addr
	typ         DataType
	shape       *streamValidationPlan
	values      []Value
	fields      []ValueField
	entries     []ValueEntry
	bytes       Bytes
	duration    Duration
	bits        uint64
	count       int
	vectorWidth int64
	scale       int32
	uuid        UUID
	state       CellState
	encoded     bool
}

// ValueField is one named field of a UDT Value, or one named parameter of
// NamedDynamicParams.
//
// Its zero value has an empty name and the zero Value.
type ValueField struct {
	// Name is the field's name, or the parameter's bind marker name.
	Name string
	// Value is the field's value.
	Value Value
}

// ValueEntry is one key and value of a map Value.
//
// Its zero value holds two zero Values.
type ValueEntry struct {
	// Key is the entry's key.
	Key Value
	// Value is the entry's value.
	Value Value
}

// Type returns the value's type.
func (v Value) Type() DataType { return v.typ }

// State returns the value's cell state.
func (v Value) State() CellState { return v.state }

var dynamicScalarTypes = func() [KindDuration + 1]DataType {
	var result [KindDuration + 1]DataType
	for kind := KindASCII; kind <= KindDuration; kind++ {
		result[kind], _ = ScalarType(kind)
	}
	return result
}()

func dynamicScalar(kind Kind) Value { return Value{typ: dynamicScalarTypes[kind]} }

// NullValue constructs a NULL cell of t.
func NullValue(t DataType) Value { return Value{typ: t, state: Null} }

// UnsetValue constructs an UNSET parameter of t; UNSET is not a valid result or nested value.
func UnsetValue(t DataType) Value { return Value{typ: t, state: Unset} }

// EmptyValue constructs an empty cell of t, rejecting types that cannot represent it.
func EmptyValue(t DataType) (Value, error) {
	if err := DynamicCodec.CheckType(t); err != nil {
		return Value{}, err
	}
	if !emptySupported(t) {
		return Value{}, typeValidation("type", "%s does not support Empty", t)
	}
	return Value{typ: t, state: EmptyCell}, nil
}

// BooleanValue constructs a dynamic CQL boolean value.
func BooleanValue(b bool) Value {
	v := dynamicScalar(KindBoolean)
	if b {
		v.bits = 1
	}
	return v
}

// TinyIntValue constructs a dynamic CQL tinyint value.
func TinyIntValue(n int8) Value {
	v := dynamicScalar(KindTinyInt)
	v.bits = uint64(n)
	return v
}

// SmallIntValue constructs a dynamic CQL smallint value.
func SmallIntValue(n int16) Value {
	v := dynamicScalar(KindSmallInt)
	v.bits = uint64(n)
	return v
}

// IntValue constructs a dynamic CQL int value.
func IntValue(n int32) Value { v := dynamicScalar(KindInt); v.bits = uint64(n); return v }

// BigIntValue constructs a dynamic CQL bigint value. It also binds a counter
// parameter, as in UPDATE ... SET c = c + ?.
func BigIntValue(n int64) Value { v := dynamicScalar(KindBigInt); v.bits = uint64(n); return v }

// FloatValue constructs a dynamic CQL float value.
func FloatValue(n float32) Value {
	v := dynamicScalar(KindFloat)
	v.bits = uint64(math.Float32bits(n))
	return v
}

// DoubleValue constructs a dynamic CQL double value.
func DoubleValue(n float64) Value {
	v := dynamicScalar(KindDouble)
	v.bits = math.Float64bits(n)
	return v
}

// TextValue constructs a dynamic CQL text value. Invalid UTF-8 fails when the
// value is bound or read with Text.
func TextValue(s string) Value {
	v := dynamicScalar(KindText)
	c, _ := wire.NewChain(wire.BorrowString(s))
	v.bytes = bytesView(c, wire.Ref{})
	return v
}

// ASCIIValue constructs a dynamic CQL ascii value. A non-ASCII byte fails when
// the value is bound or read with Text.
func ASCIIValue(s string) Value {
	v := TextValue(s)
	v.typ = dynamicScalarTypes[KindASCII]
	return v
}

// BlobValue constructs a dynamic CQL blob value. It borrows b until parameter capture; keep b immutable until then.
func BlobValue(b []byte) Value {
	v := dynamicScalar(KindBlob)
	v.bytes = BorrowBytes(b)
	return v
}

// UUIDValue constructs a dynamic CQL uuid value.
func UUIDValue(u UUID) Value { v := dynamicScalar(KindUUID); v.uuid = u; return v }

// TimeUUIDValue constructs a dynamic CQL timeuuid value. A UUID that is not
// version 1 fails when the value is bound.
func TimeUUIDValue(u TimeUUID) Value {
	v := dynamicScalar(KindTimeUUID)
	v.uuid = UUID(u)
	return v
}

// InetValue constructs a dynamic CQL inet value. An invalid address, or one
// with a zone, fails when the value is bound.
func InetValue(a netip.Addr) Value {
	v := dynamicScalar(KindInet)
	v.inet = a
	return v
}

// DateValue constructs a dynamic CQL date value.
func DateValue(n Date) Value { v := dynamicScalar(KindDate); v.bits = uint64(n); return v }

// TimeValue constructs a dynamic CQL time value. A time outside one day fails
// when the value is bound.
func TimeValue(n TimeOfDay) Value {
	v := dynamicScalar(KindTime)
	v.bits = uint64(n)
	return v
}

// TimestampValue constructs a dynamic CQL timestamp value.
func TimestampValue(n Timestamp) Value {
	v := dynamicScalar(KindTimestamp)
	v.bits = uint64(n)
	return v
}

// DurationValue constructs a dynamic CQL duration value. Components with mixed
// signs fail when the value is bound.
func DurationValue(d Duration) Value {
	v := dynamicScalar(KindDuration)
	v.duration = d
	return v
}

// VarintValue constructs a dynamic CQL varint value.
func VarintValue(n Varint) Value {
	v := dynamicScalar(KindVarint)
	v.bytes = varintBytes(n)
	return v
}

// DecimalValue constructs a dynamic CQL decimal value.
func DecimalValue(n Decimal) Value {
	v := dynamicScalar(KindDecimal)
	v.scale = n.Scale
	v.bytes = varintBytes(n.Unscaled)
	return v
}

func (v Value) require(kinds ...Kind) error {
	if err := validateDataType(v.typ); err != nil {
		return err
	}
	if v.state == Null {
		return typeValidation("value", "%s is %w", v.typ, ErrNull)
	}
	if v.state != Present {
		return typeValidation("value", "%s has state %v", v.typ, v.state)
	}
	for _, kind := range kinds {
		if v.typ.Kind() == kind {
			return checkBytesAlive(v.bytes)
		}
	}
	return typeValidation("value", "accessor does not accept %s", v.typ)
}

// Bool returns a present boolean.
func (v Value) Bool() (bool, error) { err := v.require(KindBoolean); return v.bits != 0, err }

// Signed returns a present tinyint, smallint, int, bigint, counter, date,
// time or timestamp as an int64.
func (v Value) Signed() (int64, error) {
	err := v.require(KindTinyInt, KindSmallInt, KindInt, KindBigInt, KindCounter, KindDate, KindTime, KindTimestamp)
	return int64(v.bits), err
}

// Float returns a present float or double as a float64.
func (v Value) Float() (float64, error) {
	if err := v.require(KindFloat, KindDouble); err != nil {
		return 0, err
	}
	if v.typ.Kind() == KindFloat {
		return float64(math.Float32frombits(uint32(v.bits))), nil
	}
	return math.Float64frombits(v.bits), nil
}

// Text returns a present text or ascii value. A decoded Text borrows the
// response; see Clone, or Str for a string that does not. It fails for a
// TextValue or ASCIIValue that is not valid for its type.
func (v Value) Text() (Text, error) {
	if err := v.require(KindText, KindASCII); err != nil {
		return Text{}, err
	}
	if !v.encoded {
		if err := validateText(v.bytes, v.typ.Kind() == KindASCII); err != nil {
			return Text{}, &ValidationError{Field: "value", Err: err}
		}
	}
	return textView(v.bytes), nil
}

// Str returns a present text or ascii value as a newly allocated string,
// which stays valid after the response is released. It fails as Text does,
// and with ErrReleased for a decoded value whose response was released.
func (v Value) Str() (string, error) {
	text, err := v.Text()
	if err != nil {
		return "", err
	}
	return text.Copy()
}

// Bytes returns a present blob. A decoded Bytes borrows the response; see
// Clone.
func (v Value) Bytes() (Bytes, error) {
	if err := v.require(KindBlob); err != nil {
		return Bytes{}, err
	}
	return v.bytes, nil
}

// UUID returns a present uuid or timeuuid.
func (v Value) UUID() (UUID, error) {
	if err := v.require(KindUUID, KindTimeUUID); err != nil {
		return UUID{}, err
	}
	return v.uuid, nil
}

// TimeUUID returns a present timeuuid.
func (v Value) TimeUUID() (TimeUUID, error) {
	if err := v.require(KindTimeUUID); err != nil {
		return TimeUUID{}, err
	}
	return TimeUUID(v.uuid), nil
}

// Time returns a present timestamp, or the midnight of a present date, as a
// UTC time.
func (v Value) Time() (time.Time, error) {
	if err := v.require(KindTimestamp, KindDate); err != nil {
		return time.Time{}, err
	}
	if v.typ.Kind() == KindDate {
		return Date(int32(v.bits)).Time(), nil
	}
	return Timestamp(int64(v.bits)).Time(), nil
}

// TimeOfDay returns a present time.
func (v Value) TimeOfDay() (TimeOfDay, error) {
	if err := v.require(KindTime); err != nil {
		return 0, err
	}
	return TimeOfDay(int64(v.bits)), nil
}

// Inet returns a present inet.
func (v Value) Inet() (netip.Addr, error) {
	if err := v.require(KindInet); err != nil {
		return netip.Addr{}, err
	}
	return v.inet, nil
}

// Duration returns a present duration.
func (v Value) Duration() (Duration, error) {
	if err := v.require(KindDuration); err != nil {
		return Duration{}, err
	}
	return v.duration, nil
}

// Varint returns a present varint. A decoded Varint borrows the response; see
// Clone.
func (v Value) Varint() (Varint, error) {
	if err := v.require(KindVarint); err != nil {
		return Varint{}, err
	}
	return varintView(v.bytes), nil
}

// Decimal returns a present decimal. A decoded Decimal borrows the response;
// see Clone.
func (v Value) Decimal() (Decimal, error) {
	if err := v.require(KindDecimal); err != nil {
		return Decimal{}, err
	}
	return Decimal{Scale: v.scale, Unscaled: varintView(v.bytes)}, nil
}

// Clone detaches a lazy aggregate by copying its encoded backing, without
// decoding its children. Explicit input descriptors are cloned recursively.
func (v Value) Clone() Value {
	if !bytesLease(v.bytes).IsZero() || v.bytes.Len() != 0 {
		v.bytes = v.bytes.Clone()
	}
	if v.values != nil {
		values := make([]Value, len(v.values))
		for i := range values {
			values[i] = v.values[i].Clone()
		}
		v.values = values
	}
	if v.fields != nil {
		fields := make([]ValueField, len(v.fields))
		for i, f := range v.fields {
			fields[i] = ValueField{Name: f.Name, Value: f.Value.Clone()}
		}
		v.fields = fields
	}
	if v.entries != nil {
		entries := make([]ValueEntry, len(v.entries))
		for i, e := range v.entries {
			entries[i] = ValueEntry{Key: e.Key.Clone(), Value: e.Value.Clone()}
		}
		v.entries = entries
	}
	return v
}

// DynamicCodec checks and serializes the explicit variant selected by
// Value.Type, and decodes any cell into a Value.
var DynamicCodec Codec[Value] = dynamicCodec{}

type dynamicCodec struct{}

func (dynamicCodec) CheckType(t DataType) error {
	perfCheckType()
	return checkDynamicType(t, 0)
}
func checkDynamicType(t DataType, depth int) error {
	_, err := checkedTypeHeight(t, depth, make(map[*dataTypeDescriptor]int), make(map[*dataTypeDescriptor]bool))
	return err
}
func checkedTypeHeight(t DataType, depth int, heights map[*dataTypeDescriptor]int, active map[*dataTypeDescriptor]bool) (int, error) {
	if err := validateDataType(t); err != nil {
		return 0, err
	}
	if depth > 128 {
		return 0, typeValidation("type", "dynamic nesting exceeds 128")
	}
	d := descriptorOf(t)
	if height, ok := heights[d]; ok {
		if depth+height > 128 {
			return 0, typeValidation("type", "dynamic nesting exceeds 128")
		}
		return height, nil
	}
	if active[d] {
		return 0, typeValidation("type", "cyclic descriptor")
	}
	active[d] = true
	height := 0
	child := func(t DataType) error {
		h, err := checkedTypeHeight(t, depth+1, heights, active)
		height = max(height, h+1)
		return err
	}
	var err error
	switch d.Kind {
	case KindList, KindSet:
		err = child(d.Element)
	case KindMap:
		err = child(d.Key)
		if err == nil {
			err = child(d.Element)
		}
	case KindTuple, KindUDT:
		if d.Kind == KindUDT && (d.Keyspace == "" || d.Name == "") {
			return 0, typeValidation("type", "UDT identity is incomplete")
		}
		names := make(map[string]bool)
		for i, f := range columnSlice(d.Fields) {
			if d.Kind == KindUDT {
				if f.Name == "" || names[f.Name] {
					return 0, typeValidation("type", "empty or duplicate UDT field %q", f.Name)
				}
				names[f.Name] = true
			}
			if err = child(f.Type); err != nil {
				return 0, fmt.Errorf("dynamic field %d: %w", i, err)
			}
		}
	case KindVector:
		if d.Dimension == 0 {
			return 0, typeValidation("type", "incomplete vector type")
		}
		err = child(d.Element)
		if err == nil {
			_, err = vectorFixedWidth(t)
		}
	default:
		err = validateScalarKind(d.Kind)
	}
	if err != nil {
		return 0, err
	}
	delete(active, d)
	heights[d] = height
	return height, nil
}
func dynamicCompatible(actual, wanted DataType) bool {
	if actual.Equal(wanted) {
		return true
	}
	// A bigint binds a counter.
	a, w := actual.Kind(), wanted.Kind()
	return (a == KindBigInt || a == KindCounter) && (w == KindBigInt || w == KindCounter)
}
func checkDynamicValue(t DataType, v Value, nested bool) error {
	if err := validateDataType(v.typ); err != nil {
		return err
	}
	if !dynamicCompatible(v.typ, t) {
		return typeValidation("value", "dynamic type %s does not match %s", v.typ, t)
	}
	switch v.state {
	case Present, Null:
	case Unset:
		if nested {
			return typeValidation("value", "unset is not permitted in nested values")
		}
	case EmptyCell:
		if !emptySupported(t) {
			return typeValidation("value", "%s does not support Empty", t)
		}
	default:
		return typeValidation("value", "invalid state %v", v.state)
	}
	return nil
}
func dynamicAggregate(t DataType, kind Kind, values []Value) (Value, error) {
	if err := DynamicCodec.CheckType(t); err != nil {
		return Value{}, err
	}
	return dynamicAggregateChecked(t, kind, values)
}
func dynamicAggregateChecked(t DataType, kind Kind, values []Value) (Value, error) {
	if t.Kind() != kind {
		return Value{}, typeValidation("type", "expected %s, got %s", kind, t)
	}
	if kind == KindVector && len(values) != int(descriptorOf(t).Dimension) {
		return Value{}, typeValidation("vector", "expected %d elements, got %d", descriptorOf(t).Dimension, len(values))
	}
	if kind == KindTuple && len(values) != descriptorOf(t).Fields.Len() {
		return Value{}, typeValidation("tuple", "expected %d fields, got %d", descriptorOf(t).Fields.Len(), len(values))
	}
	for i, v := range values {
		typ := descriptorOf(t).Element
		if kind == KindTuple {
			typ = columnSlice(descriptorOf(t).Fields)[i].Type
		}
		if err := checkDynamicValue(typ, v, true); err != nil {
			return Value{}, fmt.Errorf("dynamic element %d: %w", i, err)
		}
		if (kind == KindList || kind == KindSet) && v.state == Null {
			return Value{}, typeValidation("element", "collection element %d cannot be null", i)
		}
		if kind == KindVector && v.state != Present {
			width, err := vectorFixedWidth(typ)
			if err != nil {
				return Value{}, err
			}
			if v.state != EmptyCell || width != 0 {
				return Value{}, typeValidation("vector", "element %d must have a present body", i)
			}
		}
	}
	return Value{typ: t, values: values, count: len(values)}, nil
}

// ListValue constructs a dynamic list of element values, validating each
// against element. The value keeps a slice passed as values..., so it and
// borrowed values must remain unchanged until captured; Clone detaches them.
func ListValue(element DataType, values ...Value) (Value, error) {
	t, err := ListType(element)
	if err != nil {
		return Value{}, err
	}
	return dynamicAggregate(t, KindList, values)
}

// SetValue constructs a dynamic set of element values, validating each
// against element. The value keeps a slice passed as values..., so it and
// borrowed values must remain unchanged until captured; Clone detaches them.
func SetValue(element DataType, values ...Value) (Value, error) {
	t, err := SetType(element)
	if err != nil {
		return Value{}, err
	}
	return dynamicAggregate(t, KindSet, values)
}

// TupleValue constructs a dynamic tuple value, validating its members against t.
// Input storage is borrowed until captured; Clone recursively detaches it.
func TupleValue(t DataType, v []Value) (Value, error) {
	return dynamicAggregate(t, KindTuple, v)
}

// VectorValue constructs a dynamic vector of element values, whose
// dimension is the number of values, validating each against element.
// The value keeps a slice passed as values..., so it and borrowed values
// must remain unchanged until captured; Clone detaches them.
func VectorValue(element DataType, values ...Value) (Value, error) {
	if len(values) > math.MaxUint16 {
		return Value{}, typeValidation("dimension", "vector dimension %d exceeds 65535", len(values))
	}
	t, err := VectorType(element, uint16(len(values)))
	if err != nil {
		return Value{}, err
	}
	return dynamicAggregate(t, KindVector, values)
}

// MapValue constructs a dynamic map value, validating its members against t.
// Input storage is borrowed until captured; Clone recursively detaches it.
func MapValue(t DataType, entries []ValueEntry) (Value, error) {
	if err := DynamicCodec.CheckType(t); err != nil {
		return Value{}, err
	}
	return mapValueChecked(t, entries)
}
func mapValueChecked(t DataType, entries []ValueEntry) (Value, error) {
	if t.Kind() != KindMap {
		return Value{}, typeValidation("type", "expected map, got %s", t)
	}
	for i, e := range entries {
		if e.Key.state == Null || e.Value.state == Null {
			return Value{}, typeValidation("element", "map entry %d cannot contain null", i)
		}
		if err := checkDynamicValue(descriptorOf(t).Key, e.Key, true); err != nil {
			return Value{}, fmt.Errorf("dynamic map key %d: %w", i, err)
		}
		if err := checkDynamicValue(descriptorOf(t).Element, e.Value, true); err != nil {
			return Value{}, fmt.Errorf("dynamic map value %d: %w", i, err)
		}
	}
	return Value{typ: t, entries: entries, count: len(entries)}, nil
}

// UDTValue constructs a dynamic UDT value, validating its members against t.
// Input storage is borrowed until captured; Clone recursively detaches it.
func UDTValue(t DataType, fields []ValueField) (Value, error) {
	if err := DynamicCodec.CheckType(t); err != nil {
		return Value{}, err
	}
	return udtValueChecked(t, fields)
}
func udtValueChecked(t DataType, fields []ValueField) (Value, error) {
	if t.Kind() != KindUDT {
		return Value{}, typeValidation("type", "expected UDT, got %s", t)
	}
	if len(fields) != descriptorOf(t).Fields.Len() {
		return Value{}, typeValidation("fields", "expected %d UDT fields, got %d", descriptorOf(t).Fields.Len(), len(fields))
	}
	for i, f := range fields {
		for j := 0; j < i; j++ {
			if fields[j].Name == f.Name {
				return Value{}, typeValidation("fields", "duplicate UDT field %q", f.Name)
			}
		}
		found := false
		for _, col := range columnSlice(descriptorOf(t).Fields) {
			if col.Name == f.Name {
				found = true
				if err := checkDynamicValue(col.Type, f.Value, true); err != nil {
					return Value{}, fmt.Errorf("dynamic field %s: %w", f.Name, err)
				}
				break
			}
		}
		if !found {
			return Value{}, typeValidation("fields", "unknown UDT field %q", f.Name)
		}
	}
	return Value{typ: t, fields: fields, count: len(fields)}, nil
}

func (dynamicCodec) Clone(v Value) Value { return v.Clone() }
func (dynamicCodec) Encode(w *ValueWriter, v Value) error {
	if w == nil {
		return typeValidation("writer", "nil writer")
	}
	if err := DynamicCodec.CheckType(w.Type()); err != nil {
		return err
	}
	return encodeDynamicChecked(w, v)
}
func encodeDynamicChecked(w *ValueWriter, v Value) error {
	if err := checkDynamicValue(w.Type(), v, !w.allowUnset); err != nil {
		return err
	}
	switch v.state {
	case Null:
		return w.Null()
	case Unset:
		return w.Unset()
	case EmptyCell:
		return w.emptyValue()
	}
	switch v.typ.Kind() {
	case KindBoolean:
		return encodeDynamicScalar(w, BoolCodec, v.bits != 0)
	case KindTinyInt:
		return encodeDynamicScalar(w, Int8Codec, int8(v.bits))
	case KindSmallInt:
		return encodeDynamicScalar(w, Int16Codec, int16(v.bits))
	case KindInt:
		return encodeDynamicScalar(w, Int32Codec, int32(v.bits))
	case KindBigInt, KindCounter:
		return encodeDynamicScalar(w, Int64Codec, int64(v.bits))
	case KindFloat:
		return encodeDynamicScalar(w, Float32Codec, math.Float32frombits(uint32(v.bits)))
	case KindDouble:
		return encodeDynamicScalar(w, Float64Codec, math.Float64frombits(v.bits))
	case KindText, KindASCII:
		// A decoded payload was validated for its kind, and
		// checkDynamicValue matched that kind to the column, so it is
		// written without validating it again.
		if !v.encoded {
			if err := validateText(v.bytes, v.typ.Kind() == KindASCII); err != nil {
				return err
			}
		}
		return w.borrowView(v.bytes)
	case KindBlob:
		return encodeDynamicScalar(w, BytesCodec, v.bytes)
	case KindUUID:
		return encodeDynamicScalar(w, UUIDCodec, v.uuid)
	case KindTimeUUID:
		return encodeDynamicScalar(w, TimeUUIDCodec, TimeUUID(v.uuid))
	case KindInet:
		return encodeDynamicScalar(w, InetCodec, v.inet)
	case KindDate:
		return encodeDynamicScalar(w, DateCodec, Date(v.bits))
	case KindTime:
		return encodeDynamicScalar(w, TimeOfDayCodec, TimeOfDay(v.bits))
	case KindTimestamp:
		return encodeDynamicScalar(w, TimestampCodec, Timestamp(v.bits))
	case KindDuration:
		return encodeDynamicScalar(w, DurationCodec, v.duration)
	case KindVarint:
		return encodeDynamicScalar(w, VarintCodec, varintView(v.bytes))
	case KindDecimal:
		return encodeDynamicScalar(w, DecimalCodec, Decimal{Scale: v.scale, Unscaled: varintView(v.bytes)})
	default:
		return encodeDynamicAggregate(w, v)
	}
}
func decodeDynamicScalar[T any](r ValueReader, c Decoder[T], makeValue func(T) Value) (Value, error) {
	var payload T
	var err error
	if builtin, ok := c.(builtinCompiledDecoder[T]); ok {
		payload, err = builtin.decodeCompiled(r)
	} else {
		payload, err = c.Decode(r)
	}
	if err != nil {
		return Value{}, err
	}
	v := makeValue(payload)
	v.typ = r.Type()
	return v, nil
}
func (dynamicCodec) Decode(r ValueReader) (Value, error) {
	if err := DynamicCodec.CheckType(r.Type()); err != nil {
		return Value{}, err
	}
	return decodeDynamicChecked(r)
}
func decodeDynamicChecked(r ValueReader) (Value, error) { return decodeDynamicWithPlan(r, nil) }
func decodeDynamicWithPlan(r ValueReader, shape *streamValidationPlan) (Value, error) {
	if r.err != nil {
		return Value{}, r.err
	}
	if err := checkBytesAlive(r.Bytes()); err != nil {
		return Value{}, err
	}
	switch r.State() {
	case Null:
		return NullValue(r.Type()), nil
	case Unset:
		return Value{}, typeValidation("state", "unset cannot be decoded as a result")
	case Present:
	default:
		return Value{}, typeValidation("state", "invalid state %v", r.State())
	}
	if r.Bytes().Len() == 0 && emptySupported(r.Type()) {
		return Value{typ: r.Type(), state: EmptyCell}, nil
	}
	switch r.Type().Kind() {
	case KindBoolean:
		return decodeDynamicScalar(r, BoolCodec, BooleanValue)
	case KindTinyInt:
		return decodeDynamicScalar(r, Int8Codec, TinyIntValue)
	case KindSmallInt:
		return decodeDynamicScalar(r, Int16Codec, SmallIntValue)
	case KindInt:
		return decodeDynamicScalar(r, Int32Codec, IntValue)
	case KindBigInt, KindCounter:
		return decodeDynamicScalar(r, Int64Codec, BigIntValue)
	case KindFloat:
		return decodeDynamicScalar(r, Float32Codec, FloatValue)
	case KindDouble:
		return decodeDynamicScalar(r, Float64Codec, DoubleValue)
	case KindText, KindASCII:
		return decodeDynamicScalar(r, TextCodec, func(t Text) Value { return Value{bytes: t.Bytes(), encoded: true} })
	case KindBlob:
		return decodeDynamicScalar(r, BytesCodec, func(b Bytes) Value { return Value{bytes: b} })
	case KindUUID:
		return decodeDynamicScalar(r, UUIDCodec, UUIDValue)
	case KindTimeUUID:
		return decodeDynamicScalar(r, TimeUUIDCodec, func(u TimeUUID) Value { return Value{uuid: UUID(u)} })
	case KindInet:
		return decodeDynamicScalar(r, InetCodec, func(a netip.Addr) Value { return Value{inet: a} })
	case KindDate:
		return decodeDynamicScalar(r, DateCodec, DateValue)
	case KindTime:
		return decodeDynamicScalar(r, TimeOfDayCodec, func(n TimeOfDay) Value { return Value{bits: uint64(n)} })
	case KindTimestamp:
		return decodeDynamicScalar(r, TimestampCodec, TimestampValue)
	case KindDuration:
		return decodeDynamicScalar(r, DurationCodec, func(d Duration) Value { return Value{duration: d} })
	case KindVarint:
		return decodeDynamicScalar(r, VarintCodec, VarintValue)
	case KindDecimal:
		return decodeDynamicScalar(r, DecimalCodec, DecimalValue)
	default:
		return decodeDynamicAggregate(r, shape)
	}
}

func decodeDynamicAggregate(r ValueReader, shape *streamValidationPlan) (Value, error) {
	v := Value{typ: r.Type(), encoded: true, shape: shape}
	var err error
	switch r.Type().Kind() {
	case KindList, KindSet, KindVector:
		if shape == nil {
			v.bytes, v.count, v.vectorWidth, err = validateSequenceBody(r, r.Type().Kind())
		} else {
			v.bytes, v.count, v.vectorWidth, err = validateSequenceLayout(r, r.Type().Kind(), descriptorOf(r.Type()).Element, shape.width)
		}
	case KindMap:
		v.bytes, v.count, err = validateMapBody(r)
	case KindTuple, KindUDT:
		v.bytes = r.Bytes()
		v.count = descriptorOf(r.Type()).Fields.Len()
		cursor := r.cursorValue()
		for _, field := range columnSlice(descriptorOf(r.Type()).Fields) {
			if cursor.Remaining() == 0 {
				break
			} // Protocol permits omitted trailing members.
			if _, err = cursor.Child(field.Type); err != nil {
				return Value{}, err
			}
		}
		err = cursor.RequireEnd()
	default:
		return Value{}, typeValidation("type", "unsupported dynamic aggregate %s", r.Type())
	}
	if err != nil {
		return Value{}, err
	}
	return v, nil
}
func dynamicCursor(b Bytes) ValueCursor {
	return ValueCursor{cursor: *wire.NewCursor(bytesChain(b)), initialized: true, lease: bytesLease(b), initial: b.Len()}
}
func dynamicField(c *ValueCursor, t DataType, shape *streamValidationPlan) (Value, error) {
	if err := c.check(); err != nil {
		return Value{}, err
	}
	if c.Remaining() == 0 {
		return NullValue(t), nil
	}
	r, err := c.Child(t)
	if err != nil {
		return Value{}, err
	}
	return decodeDynamicWithPlan(r, shape)
}

// Elements returns an iterator over the members of a list, set, vector or
// tuple, in order. It yields each member with a nil error; a failure, such
// as a value of another type or a malformed member, ends the loop with one
// last pair holding the zero Value and the error. Each range over it starts
// from the first member. A decoded member may borrow the response; see
// Clone.
func (v Value) Elements() iter.Seq2[Value, error] {
	return seqOf(v.iterElements)
}

// Entries returns an iterator over the key and value of each entry of a
// map, in encoded order, which ends on a failure as Elements does.
func (v Value) Entries() iter.Seq2[ValueEntry, error] {
	return seqOf(v.iterEntries)
}

// Fields returns an iterator over the fields of a tuple or UDT, in type
// order, which ends on a failure as Elements does. A tuple's fields have no
// names.
func (v Value) Fields() iter.Seq2[ValueField, error] {
	return seqOf(v.iterFields)
}

// seqOf returns a range iterator that walks a fresh iterator from start on
// each range, or yields only start's error.
func seqOf[T any](start func() (*valueIterator[T], error)) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		it, err := start()
		if err != nil {
			var zero T
			yield(zero, err)
			return
		}
		it.All()(yield)
	}
}

// iterElements returns an independent iterator over list, set, vector, or
// tuple members. Tuple fields retain their physical ordinal and complete type.
func (v Value) iterElements() (*valueIterator[Value], error) {
	if err := v.require(KindList, KindSet, KindVector, KindTuple); err != nil {
		return nil, err
	}
	if !v.encoded {
		return newValueIterator(len(v.values), func(i int) (Value, error) { return v.values[i], nil }, func(v Value) Value { return v.Clone() }), nil
	}
	typ := v.typ
	return &valueIterator[Value]{clone: func(v Value) Value { return v.Clone() }, count: v.count, cursor: dynamicCursor(v.bytes), nextCursor: func(c *ValueCursor, i int) (Value, error) {
		if typ.Kind() == KindTuple {
			return dynamicField(c, columnSlice(descriptorOf(typ).Fields)[i].Type, dynamicShapeField(v.shape, i))
		}
		var r ValueReader
		var err error
		if typ.Kind() == KindVector {
			r, err = readVectorElement(c, descriptorOf(typ).Element, v.vectorWidth)
		} else {
			r, err = c.Child(descriptorOf(typ).Element)
		}
		if err != nil {
			return Value{}, err
		}
		return decodeDynamicWithPlan(r, dynamicShapeElement(v.shape))
	}}, nil
}

// iterEntries returns an independent iterator over the key and value of each
// entry of a map, in encoded order.
func (v Value) iterEntries() (*valueIterator[ValueEntry], error) {
	if err := v.require(KindMap); err != nil {
		return nil, err
	}
	if !v.encoded {
		return newValueIterator(len(v.entries), func(i int) (ValueEntry, error) { return v.entries[i], nil }, cloneDynamicEntry), nil
	}
	return &valueIterator[ValueEntry]{clone: cloneDynamicEntry, count: v.count, cursor: dynamicCursor(v.bytes), nextCursor: func(c *ValueCursor, _ int) (ValueEntry, error) {
		key, err := c.Child(descriptorOf(v.typ).Key)
		if err != nil {
			return ValueEntry{}, err
		}
		value, err := c.Child(descriptorOf(v.typ).Element)
		if err != nil {
			return ValueEntry{}, err
		}
		k, err := decodeDynamicWithPlan(key, dynamicShapeKey(v.shape))
		if err != nil {
			return ValueEntry{}, err
		}
		valueDecoded, err := decodeDynamicWithPlan(value, dynamicShapeElement(v.shape))
		if err != nil {
			return ValueEntry{}, err
		}
		return ValueEntry{Key: k, Value: valueDecoded}, nil
	}}, nil
}

// iterFields returns an independent iterator over the fields of a tuple or
// UDT, in type order. A tuple's fields have no names.
func (v Value) iterFields() (*valueIterator[ValueField], error) {
	if err := v.require(KindTuple, KindUDT); err != nil {
		return nil, err
	}
	columns := descriptorOf(v.typ).Fields
	if !v.encoded {
		return newValueIterator(columns.Len(), func(i int) (ValueField, error) {
			field := columnSlice(columns)[i]
			if v.typ.Kind() == KindTuple {
				if i >= len(v.values) {
					return ValueField{}, typeValidation("tuple", "missing field %d", i)
				}
				return ValueField{Name: field.Name, Value: v.values[i]}, nil
			}
			for _, f := range v.fields {
				if f.Name == field.Name {
					return f, nil
				}
			}
			return ValueField{}, typeValidation("fields", "missing UDT field %q", field.Name)
		}, cloneDynamicField), nil
	}
	return &valueIterator[ValueField]{clone: cloneDynamicField, count: columns.Len(), cursor: dynamicCursor(v.bytes), nextCursor: func(c *ValueCursor, i int) (ValueField, error) {
		col := columnSlice(columns)[i]
		value, err := dynamicField(c, col.Type, dynamicShapeField(v.shape, i))
		if err != nil {
			return ValueField{}, err
		}
		return ValueField{Name: col.Name, Value: value}, nil
	}}, nil
}
func encodeDynamicChild(w *ValueWriter, typ DataType, v Value, shape *streamValidationPlan) error {
	v.shape = shape
	return w.Child(typ, func(child *ValueWriter) error { return encodeDynamicChecked(child, v) })
}
func encodeDynamicAggregate(w *ValueWriter, v Value) error {
	typ := w.Type()
	switch typ.Kind() {
	case KindList, KindSet, KindVector, KindTuple:
		if !v.encoded {
			if _, err := dynamicAggregateChecked(v.typ, v.typ.Kind(), v.values); err != nil {
				return err
			}
		}
		iterator, err := v.iterElements()
		if err != nil {
			return err
		}
		count := v.count
		if !v.encoded {
			count = len(v.values)
		}
		switch typ.Kind() {
		case KindList, KindSet:
			if uint64(count) > math.MaxInt32 {
				return typeValidation("count", "too many elements")
			}
			if err := w.Uint32(uint32(count)); err != nil {
				return err
			}
		case KindTuple:
			if count != descriptorOf(typ).Fields.Len() {
				return typeValidation("tuple", "wrong field count")
			}
		case KindVector:
			if count != int(descriptorOf(typ).Dimension) {
				return typeValidation("vector", "wrong element count")
			}
		}
		var width int64
		if typ.Kind() == KindVector {
			if v.shape != nil {
				width = v.shape.width
			} else {
				width, err = vectorFixedWidth(descriptorOf(typ).Element)
			}
			if err != nil {
				return err
			}
		}
		i := 0
		for iterator.Next() {
			value := iterator.Value()
			childType := descriptorOf(typ).Element
			if typ.Kind() == KindTuple {
				childType = columnSlice(descriptorOf(typ).Fields)[i].Type
			}
			if typ.Kind() == KindVector {
				err = writeVectorElement(w, childType, width, checkedDynamicEncoder{typ: childType, shape: dynamicShapeChild(v.shape, childType)}, value)
			} else {
				err = encodeDynamicChild(w, childType, value, dynamicShapeChild(v.shape, childType))
			}
			if err != nil {
				return fmt.Errorf("dynamic element %d: %w", i, err)
			}
			i++
		}
		return iterator.Err()
	case KindMap:
		if !v.encoded {
			if _, err := mapValueChecked(v.typ, v.entries); err != nil {
				return err
			}
		}
		count := v.count
		if !v.encoded {
			count = len(v.entries)
		}
		if uint64(count) > math.MaxInt32 {
			return typeValidation("count", "too many map entries")
		}
		iterator, err := v.iterEntries()
		if err != nil {
			return err
		}
		if err := w.Uint32(uint32(count)); err != nil {
			return err
		}
		for iterator.Next() {
			e := iterator.Value()
			if err := encodeDynamicChild(w, descriptorOf(typ).Key, e.Key, dynamicShapeKey(v.shape)); err != nil {
				return err
			}
			if err := encodeDynamicChild(w, descriptorOf(typ).Element, e.Value, dynamicShapeElement(v.shape)); err != nil {
				return err
			}
		}
		return iterator.Err()
	case KindUDT:
		if !v.encoded {
			if _, err := udtValueChecked(v.typ, v.fields); err != nil {
				return err
			}
		}
		iterator, err := v.iterFields()
		if err != nil {
			return err
		}
		index := 0
		for iterator.Next() {
			f := iterator.Value()
			col := columnSlice(descriptorOf(typ).Fields)[index]
			if f.Name != col.Name {
				return typeValidation("fields", "expected %q, got %q", col.Name, f.Name)
			}
			if err := encodeDynamicChild(w, col.Type, f.Value, dynamicShapeField(v.shape, index)); err != nil {
				return fmt.Errorf("dynamic field %s: %w", f.Name, err)
			}
			index++
		}
		return iterator.Err()
	default:
		return typeValidation("type", "unsupported dynamic aggregate %s", typ)
	}
}

// DynamicParams binds explicitly typed values in physical marker order.
var DynamicParams ParamEncoder[[]Value] = dynamicParamsCodec{}

// NamedDynamicParams maps unique input names to server marker order. Repeated
// server markers reuse one input field; duplicate or unused input names
// fail.
var NamedDynamicParams ParamEncoder[[]ValueField] = namedDynamicParamsCodec{}

type dynamicParamsCodec struct{}
type dynamicParamsPlan struct {
	columns Columns
	shapes  []*streamValidationPlan
}
type namedDynamicParamsCodec struct{}
type namedDynamicParamsPlan struct {
	shapes  []*streamValidationPlan
	columns Columns
	names   []string
	lookup  map[string]int
	ordered []int
}

func checkDynamicColumns(columns Columns) error {
	for i, c := range columnSlice(columns) {
		if err := DynamicCodec.CheckType(c.Type); err != nil {
			return &CodecError{Column: c.Name, Index: i, Stage: CodecStageCompile, Err: err}
		}
	}
	return nil
}
func (dynamicParamsCodec) Compile(columns Columns) (CompiledParams[[]Value], error) {
	if err := checkDynamicColumns(columns); err != nil {
		return nil, err
	}
	shapes, err := compileDynamicColumnShapes(columns)
	if err != nil {
		return nil, err
	}
	return dynamicParamsPlan{columns: columns, shapes: shapes}, nil
}
func (namedDynamicParamsCodec) Compile(columns Columns) (CompiledParams[[]ValueField], error) {
	if err := checkDynamicColumns(columns); err != nil {
		return nil, err
	}
	shapes, err := compileDynamicColumnShapes(columns)
	if err != nil {
		return nil, err
	}
	p := namedDynamicParamsPlan{columns: columns, lookup: make(map[string]int), ordered: make([]int, columns.Len()), shapes: shapes}
	for i, c := range columnSlice(columns) {
		if c.Name == "" {
			return nil, typeValidation("parameters", "marker %d has no name", i)
		}
		index, ok := p.lookup[c.Name]
		if !ok {
			index = len(p.names)
			p.lookup[c.Name] = index
			p.names = append(p.names, c.Name)
		}
		p.ordered[i] = index
	}
	return p, nil
}
func dynamicMetadataMatches(columns Columns, actual Columns, offset int) error {
	if offset < 0 || offset > actual.Len() || columns.Len() > actual.Len()-offset {
		return typeValidation("metadata", "not enough columns for compiled dynamic plan")
	}
	for i, col := range columnSlice(columns) {
		current := columnSlice(actual)[offset+i]
		if col.Name != current.Name || col.Keyspace != current.Keyspace || col.Table != current.Table || !col.Type.Equal(current.Type) {
			return typeValidation("metadata", "column %d differs from compiled dynamic plan", i)
		}
	}
	return nil
}
func (p dynamicParamsPlan) Encode(w *ParamWriter, values []Value) error {
	if w == nil || w.builder == nil {
		return typeValidation("writer", "nil parameter writer")
	}
	if len(values) != p.columns.Len() {
		return typeValidation("parameters", "expected %d values, got %d", p.columns.Len(), len(values))
	}
	if err := dynamicMetadataMatches(p.columns, w.columns, w.index); err != nil {
		return err
	}
	mark, start := w.builder.Mark(), w.index
	for _, v := range values {
		if err := w.WriteParam(checkedDynamicEncoder{typ: columnSlice(p.columns)[w.index-start].Type, shape: p.shapes[w.index-start]}, v); err != nil {
			w.builder.Rollback(mark)
			w.index = start
			return err
		}
	}
	return nil
}
func (p namedDynamicParamsPlan) Encode(w *ParamWriter, fields []ValueField) error {
	if w == nil || w.builder == nil {
		return typeValidation("writer", "nil parameter writer")
	}
	if err := dynamicMetadataMatches(p.columns, w.columns, w.index); err != nil {
		return err
	}
	positions := make([]int, len(p.names))
	for i := range positions {
		positions[i] = -1
	}
	for i, f := range fields {
		index, ok := p.lookup[f.Name]
		if !ok {
			return typeValidation("parameters", "unused input name %q", f.Name)
		}
		if positions[index] >= 0 {
			return typeValidation("parameters", "duplicate input name %q", f.Name)
		}
		positions[index] = i
	}
	for i, position := range positions {
		if position < 0 {
			return typeValidation("parameters", "missing input name %q", p.names[i])
		}
	}
	mark, start := w.builder.Mark(), w.index
	for _, index := range p.ordered {
		if err := w.WriteParam(checkedDynamicEncoder{typ: columnSlice(p.columns)[w.index-start].Type, shape: p.shapes[w.index-start]}, fields[positions[index]].Value); err != nil {
			w.builder.Rollback(mark)
			w.index = start
			return err
		}
	}
	return nil
}

// DynamicRows compiles a lazy row decoder retaining the actual ordered metadata.
var DynamicRows RowDecoder[DynamicRow] = dynamicRowsCodec{}

type dynamicRowsCodec struct{}

func (dynamicRowsCodec) Clone(r DynamicRow) DynamicRow { return r.Clone() }

type dynamicRowsPlan struct {
	lookup  map[string]int
	shapes  []*streamValidationPlan
	columns Columns
}

// DynamicRow holds columns and metadata-driven values. Columns returns the result
// metadata, At and Get return a column's Value by position and name, and
// Clone detaches the row. Rows, Page and Each values may borrow the response;
// One detaches the row before returning.
//
// The zero DynamicRow has no columns.
type DynamicRow struct {
	physical     *dynamicRowIndex
	lookup       map[string]int
	shapes       []*streamValidationPlan
	columns      Columns
	body         Bytes
	allowMissing bool
}

// Columns returns the row's result metadata.
func (r DynamicRow) Columns() Columns { return r.columns }

// Clone returns a copy of the row that holds no borrowed storage. It panics
// with ErrReleased once the row's page has been released.
func (r DynamicRow) Clone() DynamicRow {
	r.body = r.body.Clone()
	r.physical = &dynamicRowIndex{}
	return r
}

type dynamicRowCell struct {
	offset, length int64
	state          CellState
}
type dynamicRowIndex struct {
	err   error
	cells []dynamicRowCell
	once  sync.Once
}

func (r DynamicRow) buildPhysicalIndex() {
	r.physical.once.Do(func() {
		perfDynamicIndex(r.columns.Len())
		cells := make([]dynamicRowCell, r.columns.Len())
		chain := bytesChain(r.body)
		cursor := wire.NewCursor(chain)
		for i, col := range columnSlice(r.columns) {
			if r.allowMissing && cursor.Remaining() == 0 {
				cells[i].state = Null
				continue
			}
			value, e := readFramedValue(cursor, col.Type, bytesLease(r.body), false, false)
			if e != nil {
				r.physical.err = &CodecError{Column: col.Name, Index: i, Stage: CodecStageDecode, Err: e}
				return
			}
			cells[i] = dynamicRowCell{offset: chain.Len() - cursor.Remaining() - value.body.Len(), length: value.body.Len(), state: value.state}
		}
		if e := cursor.RequireEnd(); e != nil {
			r.physical.err = e
			return
		}
		r.physical.cells = cells
	})
}

// At decodes the column at index. A decoded Value may borrow the response.
// It returns ErrReleased once the row's page has been released, and a
// CodecError when the row or the column does not decode.
func (r DynamicRow) At(index int) (Value, error) {
	col, ok := r.columns.At(index)
	if !ok {
		return Value{}, typeValidation("column", "ordinal %d outside %d columns", index, r.columns.Len())
	}
	if e := checkBytesAlive(r.body); e != nil {
		return Value{}, e
	}
	if r.physical == nil {
		return Value{}, typeValidation("row", "uninitialized dynamic row index")
	}
	r.buildPhysicalIndex()
	if r.physical.err != nil {
		return Value{}, r.physical.err
	}
	cell := r.physical.cells[index]
	body, e := bytesChain(r.body).Slice(cell.offset, cell.length)
	if e != nil {
		return Value{}, e
	}
	value := newValueReader(col.Type, cell.state, body, bytesLease(r.body))
	decoded, e := decodeDynamicWithPlan(value, r.shapes[index])
	if e != nil {
		return Value{}, &CodecError{Column: col.Name, Index: index, Stage: CodecStageDecode, Err: e}
	}
	return decoded, nil
}

// Get decodes the column named name, as At does. It returns a
// ValidationError for an unknown name, or for a name the result holds more
// than once.
func (r DynamicRow) Get(name string) (Value, error) {
	index, ok := r.lookup[name]
	if !ok {
		return Value{}, typeValidation("column", "unknown name %q", name)
	}
	if index < 0 {
		return Value{}, typeValidation("column", "duplicate result name %q requires ordinal access", name)
	}
	return r.At(index)
}
func (dynamicRowsCodec) Compile(columns Columns) (CompiledRow[DynamicRow], error) {
	perfCompile()
	perfNameMap()
	if err := checkDynamicColumns(columns); err != nil {
		return nil, err
	}
	p := dynamicRowsPlan{columns: columns, lookup: make(map[string]int, columns.Len()), shapes: make([]*streamValidationPlan, columns.Len())}
	for i, col := range columnSlice(columns) {
		shape, err := compileStreamValidation(col.Type, 0)
		if err != nil {
			return nil, &CodecError{Column: col.Name, Index: i, Stage: CodecStageCompile, Err: err}
		}
		p.shapes[i] = shape
		if _, exists := p.lookup[col.Name]; exists {
			p.lookup[col.Name] = -1
		} else {
			p.lookup[col.Name] = i
		}
	}
	return p, nil
}
func (p dynamicRowsPlan) DecodeInto(r *RowReader, dst *DynamicRow) error {
	if dst == nil {
		return typeValidation("destination", "nil dynamic row destination")
	}
	*dst = DynamicRow{}
	if r == nil || r.cursor == nil {
		return typeValidation("reader", "nil row reader")
	}
	if err := dynamicMetadataMatches(p.columns, r.columns, r.index); err != nil {
		return err
	}
	if !r.lease.IsZero() && !r.lease.Alive() {
		return ErrReleased
	}
	start := *r.cursor
	for range columnSlice(p.columns) {
		if _, err := r.nextValue(); err != nil {
			return err
		}
	}
	body, err := start.Take(start.Remaining() - r.cursor.Remaining())
	if err != nil {
		return err
	}
	dst.shapes = p.shapes
	dst.physical = &dynamicRowIndex{}
	dst.columns, dst.lookup, dst.body, dst.allowMissing = p.columns, p.lookup, bytesView(body, r.lease), r.allowMissing
	return nil
}

type checkedDynamicDecoder struct {
	typ   DataType
	shape *streamValidationPlan
}

func (c checkedDynamicDecoder) CheckType(t DataType) error {
	perfCheckType()

	if !c.typ.Equal(t) {
		return typeValidation("type", "compiled dynamic metadata changed")
	}
	return nil
}
func (c checkedDynamicDecoder) Decode(r ValueReader) (Value, error) {
	if !c.typ.Equal(r.Type()) {
		return Value{}, typeValidation("type", "compiled dynamic metadata changed")
	}
	return decodeDynamicWithPlan(r, c.shape)
}
func (c checkedDynamicDecoder) Clone(v Value) Value { return v.Clone() }
func (dynamicCodec) compileValueDecoder(t DataType) (Decoder[Value], error) {
	if err := DynamicCodec.CheckType(t); err != nil {
		return nil, err
	}
	shape, err := compileStreamValidation(t, 0)
	if err != nil {
		return nil, err
	}
	return checkedDynamicDecoder{typ: t, shape: shape}, nil
}

func cloneDynamicEntry(v ValueEntry) ValueEntry {
	return ValueEntry{Key: v.Key.Clone(), Value: v.Value.Clone()}
}
func cloneDynamicField(v ValueField) ValueField {
	return ValueField{Name: v.Name, Value: v.Value.Clone()}
}

type checkedDynamicEncoder struct {
	typ   DataType
	shape *streamValidationPlan
}

func (c checkedDynamicEncoder) CheckType(t DataType) error {
	perfCheckType()

	if !c.typ.Equal(t) {
		return typeValidation("type", "compiled dynamic metadata changed")
	}
	return nil
}
func (c checkedDynamicEncoder) Encode(w *ValueWriter, v Value) error {
	if w == nil {
		return typeValidation("writer", "nil writer")
	}
	if !c.typ.Equal(w.Type()) {
		return typeValidation("type", "compiled dynamic metadata changed")
	}
	v.shape = c.shape
	return encodeDynamicChecked(w, v)
}
func (dynamicCodec) compileValueEncoder(t DataType) (Encoder[Value], error) {
	if err := DynamicCodec.CheckType(t); err != nil {
		return nil, err
	}
	shape, err := compileStreamValidation(t, 0)
	if err != nil {
		return nil, err
	}
	return checkedDynamicEncoder{typ: t, shape: shape}, nil
}

func dynamicShapeElement(p *streamValidationPlan) *streamValidationPlan {
	if p == nil {
		return nil
	}
	return p.element
}
func dynamicShapeKey(p *streamValidationPlan) *streamValidationPlan {
	if p == nil {
		return nil
	}
	return p.key
}
func dynamicShapeField(p *streamValidationPlan, index int) *streamValidationPlan {
	if p == nil || index < 0 || index >= len(p.fields) {
		return nil
	}
	return p.fields[index]
}
func dynamicShapeChild(p *streamValidationPlan, t DataType) *streamValidationPlan {
	if p == nil {
		return nil
	}
	return p.children[descriptorOf(t)]
}

func compileDynamicColumnShapes(columns Columns) ([]*streamValidationPlan, error) {
	plans := make([]*streamValidationPlan, columns.Len())
	memo := make(map[*dataTypeDescriptor]*streamValidationPlan)
	for i, c := range columnSlice(columns) {
		p, err := compileStreamValidationMemo(c.Type, 0, memo)
		if err != nil {
			return nil, &CodecError{Column: c.Name, Index: i, Stage: CodecStageCompile, Err: err}
		}
		plans[i] = p
	}
	return plans, nil
}

// Dynamic schema plans already select the scalar kind. Built-in codecs use
// their compiled entry while preserving their constant kind/state/lease/content
// guards; custom codecs keep their ordinary Decode/Encode behavior.
type builtinCompiledDecoder[T any] interface{ decodeCompiled(ValueReader) (T, error) }
type builtinCompiledEncoder[T any] interface{ encodeCompiled(*ValueWriter, T) error }

func encodeDynamicScalar[T any](w *ValueWriter, c Encoder[T], value T) error {
	if builtin, ok := c.(builtinCompiledEncoder[T]); ok {
		return builtin.encodeCompiled(w, value)
	}
	return c.Encode(w, value)
}
