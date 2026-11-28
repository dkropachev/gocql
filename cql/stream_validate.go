/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cql

import (
	"fmt"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// streamValidationPlan is compiled once per physical shape. Ignored columns and
// explicit encoded output share these exact incremental validation operations.

type streamValidationPlan struct {
	typ          DataType
	scalar       func(*StreamValueReader) error
	element, key *streamValidationPlan
	children     map[*dataTypeDescriptor]*streamValidationPlan
	fields       []*streamValidationPlan
	width        int64
	height       int
	building     bool
}

func compileStreamValidation(t DataType, depth int) (*streamValidationPlan, error) {
	return compileStreamValidationMemo(t, depth, make(map[*dataTypeDescriptor]*streamValidationPlan))
}
func compileStreamValidationMemo(t DataType, depth int, memo map[*dataTypeDescriptor]*streamValidationPlan) (*streamValidationPlan, error) {
	if err := validateDataType(t); err != nil {
		return nil, err
	}
	if depth > 128 {
		return nil, typeValidation("type", "streaming type nesting exceeds 128")
	}
	if p := memo[descriptorOf(t)]; p != nil {
		if p.building {
			return nil, typeValidation("type", "cyclic streaming descriptor")
		}
		if depth+p.height > 128 {
			return nil, typeValidation("type", "streaming type nesting exceeds 128")
		}
		return p, nil
	}
	p := &streamValidationPlan{typ: t, building: true}
	memo[descriptorOf(t)] = p
	d := descriptorOf(t)
	child := func(t DataType) (*streamValidationPlan, error) {
		return compileStreamValidationMemo(t, depth+1, memo)
	}
	var err error
	switch t.Kind() {
	case KindList, KindSet, KindVector:
		p.element, err = child(d.Element)
		if err != nil {
			return nil, err
		}
		if t.Kind() == KindVector {
			if d.Dimension == 0 {
				return nil, typeValidation("dimension", "zero vector dimension")
			}
			p.width, err = vectorFixedWidth(d.Element)
		}
	case KindMap:
		p.key, err = child(d.Key)
		if err == nil {
			p.element, err = child(d.Element)
		}
	case KindTuple, KindUDT:
		if t.Kind() == KindUDT && (d.Keyspace == "" || d.Name == "") {
			return nil, typeValidation("UDT identity", "incomplete descriptor")
		}
		p.fields = make([]*streamValidationPlan, d.Fields.Len())
		for i, c := range columnSlice(d.Fields) {
			p.fields[i], err = child(c.Type)
			if err != nil {
				return nil, err
			}
		}
	default:
		err = validateScalarKind(t.Kind())
		if err == nil {
			p.scalar, err = compileStreamScalarValidation(t)
		}
	}
	if err != nil {
		return nil, err
	}
	if p.element != nil || p.key != nil || len(p.fields) > 0 {
		p.children = make(map[*dataTypeDescriptor]*streamValidationPlan)
	}
	if p.element != nil {
		p.children[descriptorOf(p.element.typ)] = p.element
	}
	if p.key != nil {
		p.children[descriptorOf(p.key.typ)] = p.key
	}
	for _, child := range p.fields {
		p.children[descriptorOf(child.typ)] = child
	}
	for _, child := range p.children {
		p.height = max(p.height, child.height+1)
	}
	if depth+p.height > 128 {
		return nil, typeValidation("type", "streaming type nesting exceeds 128")
	}
	p.building = false
	return p, nil
}
func (p *streamValidationPlan) skip(r *StreamValueReader, allowNull bool) error {
	if err := r.check(); err != nil {
		return err
	}
	if !r.matches(p.typ) {
		return r.fail(typeValidation("type", "validation metadata changed"))
	}
	if r.state == CellNull && allowNull {
		return r.RequireEnd()
	}
	if err := r.present(); err != nil {
		return err
	}
	if r.Remaining() == 0 && emptySupported(r.typ) {
		return nil
	}
	switch p.typ.Kind() {
	case KindBlob, KindVarint, KindDecimal:
		minimum := int64(0)
		if p.typ.Kind() == KindVarint {
			minimum = 1
		}
		if p.typ.Kind() == KindDecimal {
			minimum = 5
		}
		if r.Remaining() < minimum {
			return r.fail(typeValidation("length", "invalid %s length", p.typ))
		}
		return r.rawSkip(nil)
	case KindText, KindASCII:
		validator := newTextValidation(p.typ.Kind() == KindASCII)
		if err := r.rawSkip(validator.write); err != nil {
			return err
		}
		return r.failIf(validator.finish())
	case KindList, KindSet, KindMap:
		minimum := int64(4)
		if p.typ.Kind() == KindMap {
			minimum = 8
		}
		count, err := r.count(minimum)
		if err != nil {
			return err
		}
		for i := 0; i < count; i++ {
			if p.key != nil {
				if err := r.child(p.key.typ, p.key, false, func(c *StreamValueReader) error { return p.key.skip(c, false) }); err != nil {
					return streamFailure("map key", err)
				}
			}
			if err := r.child(p.element.typ, p.element, false, func(c *StreamValueReader) error { return p.element.skip(c, false) }); err != nil {
				return streamFailure("element", err)
			}
		}
	case KindTuple, KindUDT:
		for i, f := range p.fields {
			if r.Remaining() == 0 {
				break
			} // Missing suffix is null in protocol records.
			if err := r.child(f.typ, f, true, func(c *StreamValueReader) error { return f.skip(c, true) }); err != nil {
				return fmt.Errorf("gocql: record field %d: %w", i, err)
			}
		}
	case KindVector:
		count := int(descriptorOf(p.typ).Dimension)
		if p.width > 0 && r.Remaining() != int64(count)*p.width {
			return r.fail(typeValidation("dimension", "incorrect fixed vector body width"))
		}
		if p.width == 0 && r.Remaining() < int64(count) {
			return r.fail(wire.ErrLimit)
		}
		for i := 0; i < count; i++ {
			size := p.width
			if size == 0 {
				var err error
				size, err = r.vectorLength()
				if err != nil {
					return err
				}
			}
			if err := r.childBody(p.element.typ, CellPresent, size, p.element, func(c *StreamValueReader) error { return p.element.skip(c, false) }); err != nil {
				return fmt.Errorf("gocql: vector element %d: %w", i, err)
			}
		}
	default:
		if p.scalar == nil {
			return r.fail(typeValidation("type", "missing scalar validation plan"))
		}
		if err := p.scalar(r); err != nil {
			return err
		}

	}
	return r.RequireEnd()
}
func (r *StreamValueReader) failIf(err error) error {
	if err != nil {
		return r.fail(err)
	}
	return nil
}

// StreamCell is a validated cell of any type decoded into caller storage:
// its type, state and body bytes.
//
// The driver fills it; its zero value is a present cell with no bytes and the
// invalid zero DataType.
type StreamCell struct {
	// Type is the cell's type.
	Type DataType
	// Bytes holds the cell's body, written into caller storage.
	Bytes []byte
	// State is the cell's state.
	State CellState
}
type streamEncodedCodec struct{}
type streamEncodedPlan struct{ validation *streamValidationPlan }

// StreamEncoded decodes a cell of any type, validated against its metadata,
// into a StreamCell.
var StreamEncoded StreamDecoder[StreamCell] = streamEncodedCodec{}

func (streamEncodedCodec) Compile(t DataType) (CompiledStreamValue[StreamCell], error) {
	p, e := compileStreamValidation(t, 0)
	if e != nil {
		return nil, e
	}
	return streamEncodedPlan{p}, nil
}
func (streamEncodedPlan) Reset(dst *StreamCell) {
	if dst != nil {
		dst.Type = DataType{}
		dst.State = CellPresent
		dst.Bytes = dst.Bytes[:0]
	}
}
func (p streamEncodedPlan) DecodeInto(r *StreamValueReader, dst *StreamCell) error {
	if dst == nil {
		return r.fail(typeValidation("destination", "nil encoded destination"))
	}
	if err := r.check(); err != nil {
		return err
	}
	if !r.matches(p.validation.typ) {
		return r.fail(typeValidation("type", "encoded metadata changed"))
	}
	if err := r.capacity(r.Remaining(), cap(dst.Bytes)); err != nil {
		return err
	}
	dst.Bytes = dst.Bytes[:int(r.Remaining())]
	previous := r.capture
	r.captureState = streamCapture{dst: dst.Bytes}
	r.capture = &r.captureState
	defer func() { r.capture = previous; r.captureState = streamCapture{} }()
	if err := p.validation.skip(r, true); err != nil {
		return err
	}
	dst.Type, dst.State = r.typ, r.state
	if r.state == CellPresent && len(dst.Bytes) == 0 && emptySupported(r.typ) {
		dst.State = CellEmpty
	}
	return nil
}

func (streamEncodedCodec) Reset(dst *StreamCell) {
	if dst != nil {
		dst.Type = DataType{}
		dst.State = CellPresent
		dst.Bytes = dst.Bytes[:0]
	}
}

func streamScalarValidation[T any](typ DataType, codec StreamDecoder[T]) (func(*StreamValueReader) error, error) {
	plan, err := codec.Compile(typ)
	if err != nil {
		return nil, err
	}
	native, ok := plan.(streamScalarPlan[T])
	if !ok {
		return nil, typeValidation("stream scalar", "requires native scalar validation")
	}
	return func(r *StreamValueReader) error { var value T; return native.DecodeInto(r, &value) }, nil
}
func compileStreamScalarValidation(t DataType) (func(*StreamValueReader) error, error) {
	switch t.Kind() {
	case KindBoolean:
		return streamScalarValidation(t, StreamBool)
	case KindTinyInt:
		return streamScalarValidation(t, StreamInt8)
	case KindSmallInt:
		return streamScalarValidation(t, StreamInt16)
	case KindInt:
		return streamScalarValidation(t, StreamInt32)
	case KindBigInt, KindCounter:
		return streamScalarValidation(t, StreamInt64)
	case KindFloat:
		return streamScalarValidation(t, StreamFloat32)
	case KindDouble:
		return streamScalarValidation(t, StreamFloat64)
	case KindDate:
		return streamScalarValidation(t, StreamDate)
	case KindTime:
		return streamScalarValidation(t, StreamTimeOfDay)
	case KindTimestamp:
		return streamScalarValidation(t, StreamTimestamp)
	case KindDuration:
		return streamScalarValidation(t, StreamDuration)
	case KindUUID:
		return streamScalarValidation(t, StreamUUID)
	case KindTimeUUID:
		return streamScalarValidation(t, StreamTimeUUID)
	case KindInet:
		return streamScalarValidation(t, StreamInet)
	// Variable-byte types have specialized incremental validation in skip.
	case KindBlob, KindVarint, KindDecimal, KindText, KindASCII:
		return nil, nil
	default:
		return nil, typeValidation("type", "unsupported scalar validation %s", t)
	}
}
