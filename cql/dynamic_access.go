// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"encoding/hex"
	"math"
	"strconv"
	"strings"
)

// Len returns the number of elements of a present list, set or vector, of
// entries of a present map, or of fields of a present tuple or UDT. It
// returns 0 for any other value.
func (v Value) Len() int {
	if v.state != Present || descriptorOf(v.typ) == nil {
		return 0
	}
	switch v.typ.Kind() {
	case TypeList, TypeSet, TypeMap, TypeTuple, TypeUDT, TypeVector:
		return v.count
	}
	return 0
}

// At returns the element at index of a present list, set or vector, or the
// field at index of a present tuple or UDT. A decoded value is read from its
// start up to index, so a loop over every member should use Elements or
// Fields. A decoded member may borrow the response; see Clone.
func (v Value) At(index int) (Value, error) {
	if err := v.require(TypeList, TypeSet, TypeVector, TypeTuple, TypeUDT); err != nil {
		return Value{}, err
	}
	if index < 0 || index >= v.count {
		return Value{}, typeValidation("index", "%d outside %d members", index, v.count)
	}
	if v.typ.Kind() == TypeUDT {
		fields, err := v.Fields()
		if err != nil {
			return Value{}, err
		}
		f, err := nth(fields, index)
		return f.Value, err
	}
	if !v.encoded {
		return v.values[index], nil
	}
	elements, err := v.Elements()
	if err != nil {
		return Value{}, err
	}
	return nth(elements, index)
}

// Field returns the field named name of a present UDT. It returns a
// ValidationError for a name the UDT does not have. A decoded field may
// borrow the response; see Clone.
func (v Value) Field(name string) (Value, error) {
	if err := v.require(TypeUDT); err != nil {
		return Value{}, err
	}
	for i, f := range columnSlice(descriptorOf(v.typ).Fields) {
		if f.Name == name {
			return v.At(i)
		}
	}
	return Value{}, typeValidation("field", "%s has no field %q", v.typ, name)
}

// nth advances it to the member at index and returns it.
func nth[T any](it *ValueIterator[T], index int) (T, error) {
	for i := 0; it.Next(); i++ {
		if i == index {
			return it.Value(), nil
		}
	}
	var zero T
	if err := it.Err(); err != nil {
		return zero, err
	}
	return zero, typeValidation("index", "%d outside the members read", index)
}

// String returns the value as a CQL literal, such as 'text', 0x0102,
// [1, 2], {'k': 1} or (1, 'a'), for logs and debugging. A NULL is null, an
// UNSET is unset, an empty value is empty, and the zero Value is <invalid>.
// A member that does not decode, or a view whose page was released, prints
// as <error: ...> instead of failing.
func (v Value) String() string {
	var b strings.Builder
	v.format(&b)
	return b.String()
}

func (v Value) format(b *strings.Builder) {
	if descriptorOf(v.typ) == nil {
		b.WriteString("<invalid>")
		return
	}
	switch v.state {
	case Present:
	case Null:
		b.WriteString("null")
		return
	case Unset:
		b.WriteString("unset")
		return
	case EmptyCell:
		b.WriteString("empty")
		return
	default:
		b.WriteString("<invalid>")
		return
	}
	fail := func(err error) {
		b.WriteString("<error: ")
		b.WriteString(err.Error())
		b.WriteString(">")
	}
	quoted := func(s string) {
		b.WriteByte('\'')
		b.WriteString(strings.ReplaceAll(s, "'", "''"))
		b.WriteByte('\'')
	}
	if err := checkBytesAlive(v.bytes); err != nil {
		fail(err)
		return
	}
	switch v.typ.Kind() {
	case TypeBoolean:
		b.WriteString(strconv.FormatBool(v.bits != 0))
	case TypeTinyInt, TypeSmallInt, TypeInt, TypeBigInt, TypeCounter:
		b.WriteString(strconv.FormatInt(int64(v.bits), 10))
	case TypeFloat:
		b.WriteString(strconv.FormatFloat(float64(math.Float32frombits(uint32(v.bits))), 'g', -1, 32))
	case TypeDouble:
		b.WriteString(strconv.FormatFloat(math.Float64frombits(v.bits), 'g', -1, 64))
	case TypeText, TypeASCII:
		quoted(textView(v.bytes).String())
	case TypeBlob:
		b.WriteString("0x")
		b.WriteString(hex.EncodeToString(v.bytes.OwnedBytes()))
	case TypeUUID, TypeTimeUUID:
		b.WriteString(v.uuid.String())
	case TypeInet:
		quoted(v.inet.String())
	case TypeDate:
		quoted(Date(int32(v.bits)).String())
	case TypeTime:
		quoted(TimeOfDay(int64(v.bits)).String())
	case TypeTimestamp:
		quoted(Timestamp(int64(v.bits)).String())
	case TypeDuration:
		b.WriteString(v.duration.String())
	case TypeVarint:
		b.WriteString(varintView(v.bytes).String())
	case TypeDecimal:
		b.WriteString(Decimal{Scale: v.scale, Unscaled: varintView(v.bytes)}.String())
	case TypeList, TypeVector, TypeSet, TypeTuple:
		open, end := "[", "]"
		switch v.typ.Kind() {
		case TypeSet:
			open, end = "{", "}"
		case TypeTuple:
			open, end = "(", ")"
		}
		b.WriteString(open)
		elements, err := v.Elements()
		if err != nil {
			fail(err)
			return
		}
		sep := ""
		for e, err := range elements.All() {
			b.WriteString(sep)
			sep = ", "
			if err != nil {
				fail(err)
				break
			}
			e.format(b)
		}
		b.WriteString(end)
	case TypeMap:
		b.WriteString("{")
		entries, err := v.Entries()
		if err != nil {
			fail(err)
			return
		}
		sep := ""
		for e, err := range entries.All() {
			b.WriteString(sep)
			sep = ", "
			if err != nil {
				fail(err)
				break
			}
			e.Key.format(b)
			b.WriteString(": ")
			e.Value.format(b)
		}
		b.WriteString("}")
	case TypeUDT:
		b.WriteString("{")
		fields, err := v.Fields()
		if err != nil {
			fail(err)
			return
		}
		sep := ""
		for f, err := range fields.All() {
			b.WriteString(sep)
			sep = ", "
			if err != nil {
				fail(err)
				break
			}
			b.WriteString(f.Name)
			b.WriteString(": ")
			f.Value.format(b)
		}
		b.WriteString("}")
	default:
		b.WriteString("<invalid>")
	}
}
