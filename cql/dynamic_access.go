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
	if v.state != CellPresent || descriptorOf(v.typ) == nil {
		return 0
	}
	switch v.typ.Kind() {
	case KindList, KindSet, KindMap, KindTuple, KindUDT, KindVector:
		return v.count
	}
	return 0
}

// Field returns the field named name of a present UDT. It returns a
// ValidationError for a name the UDT does not have. A decoded UDT is read
// from its start up to the field, so a loop over every field should use
// Fields. A decoded field may borrow the response; see Clone.
func (v Value) Field(name string) (Value, error) {
	if err := v.require(KindUDT); err != nil {
		return Value{}, err
	}
	index := -1
	for i, f := range columnSlice(descriptorOf(v.typ).Fields) {
		if f.Name == name {
			index = i
			break
		}
	}
	if index < 0 {
		return Value{}, typeValidation("field", "%s has no field %q", v.typ, name)
	}
	fields, err := v.iterFields()
	if err != nil {
		return Value{}, err
	}
	for i := 0; fields.Next(); i++ {
		if i == index {
			return fields.Value().Value, nil
		}
	}
	if err := fields.Err(); err != nil {
		return Value{}, err
	}
	return Value{}, typeValidation("field", "%s has no field %q", v.typ, name)
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
	if untypedMarker(v) {
		if v.state == CellNull {
			b.WriteString("null")
		} else {
			b.WriteString("unset")
		}
		return
	}
	if descriptorOf(v.typ) == nil {
		b.WriteString("<invalid>")
		return
	}
	switch v.state {
	case CellPresent:
	case CellNull:
		b.WriteString("null")
		return
	case CellUnset:
		b.WriteString("unset")
		return
	case CellEmpty:
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
	case KindBoolean:
		b.WriteString(strconv.FormatBool(v.bits != 0))
	case KindTinyInt, KindSmallInt, KindInt, KindBigInt, KindCounter:
		b.WriteString(strconv.FormatInt(int64(v.bits), 10))
	case KindFloat:
		b.WriteString(strconv.FormatFloat(float64(math.Float32frombits(uint32(v.bits))), 'g', -1, 32))
	case KindDouble:
		b.WriteString(strconv.FormatFloat(math.Float64frombits(v.bits), 'g', -1, 64))
	case KindText, KindASCII:
		quoted(textView(v.bytes).string())
	case KindBlob:
		b.WriteString("0x")
		b.WriteString(hex.EncodeToString(v.bytes.ownedBytes()))
	case KindUUID, KindTimeUUID:
		b.WriteString(v.uuid.String())
	case KindInet:
		quoted(v.inet.String())
	case KindDate:
		quoted(Date(int32(v.bits)).String())
	case KindTime:
		quoted(TimeOfDay(int64(v.bits)).String())
	case KindTimestamp:
		quoted(Timestamp(int64(v.bits)).String())
	case KindDuration:
		b.WriteString(v.duration.String())
	case KindVarint, KindDecimal:
		n, err := varintOf(v.bytes)
		if err != nil {
			fail(err)
			return
		}
		if v.typ.Kind() == KindVarint {
			b.WriteString(n.String())
		} else {
			b.WriteString(Decimal{Scale: v.scale, Unscaled: n}.String())
		}
	case KindList, KindVector, KindSet, KindTuple:
		open, end := "[", "]"
		switch v.typ.Kind() {
		case KindSet:
			open, end = "{", "}"
		case KindTuple:
			open, end = "(", ")"
		}
		b.WriteString(open)
		sep := ""
		for e, err := range v.Elements() {
			b.WriteString(sep)
			sep = ", "
			if err != nil {
				fail(err)
				break
			}
			e.format(b)
		}
		b.WriteString(end)
	case KindMap:
		b.WriteString("{")
		sep := ""
		for e, err := range v.Entries() {
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
	case KindUDT:
		b.WriteString("{")
		sep := ""
		for f, err := range v.Fields() {
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
