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
	"errors"
)

type customStreamPair struct {
	A int32
	B []byte
}
type customStreamDecoder struct{ checks *int }
type customStreamPlan struct {
	integer           CompiledStreamValue[int32]
	blob              CompiledStreamValue[[]byte]
	intType, blobType DataType
}

func (c customStreamDecoder) Compile(t DataType) (CompiledStreamValue[customStreamPair], error) {
	*c.checks++
	fields := t.Fields()
	if fields.Len() != 2 {
		return nil, errors.New("expected two fields")
	}
	a, _ := fields.At(0)
	b, _ := fields.At(1)
	integer, err := StreamInt32.Compile(a.Type)
	if err != nil {
		return nil, err
	}
	blob, err := StreamByteSlice.Compile(b.Type)
	if err != nil {
		return nil, err
	}
	return customStreamPlan{integer, blob, a.Type, b.Type}, nil
}
func (customStreamDecoder) Reset(dst *customStreamPair) { *dst = customStreamPair{B: dst.B[:0]} }
func (p customStreamPlan) Reset(dst *customStreamPair)  { p.integer.Reset(&dst.A); p.blob.Reset(&dst.B) }
func (p customStreamPlan) DecodeInto(r *StreamValueReader, dst *customStreamPair) error {
	if r.Context() == nil {
		return errors.New("missing context")
	}
	if err := r.Child(p.intType, func(child *StreamValueReader) error { return p.integer.DecodeInto(child, &dst.A) }); err != nil {
		return err
	}
	return r.Child(p.blobType, func(child *StreamValueReader) error { return p.blob.DecodeInto(child, &dst.B) })
}

type nilStreamValue struct{}

func (nilStreamValue) Compile(DataType) (CompiledStreamValue[int32], error) { return nil, nil }
func (nilStreamValue) Reset(*int32)                                         {}

type nilStreamRows struct{}

func (nilStreamRows) Compile(Columns) (CompiledStreamRow[int32], error) { return nil, nil }

type nilBufferedRows struct{}

func (nilBufferedRows) Compile(Columns) (CompiledRow[int32], error) { return nil, nil }
func (nilBufferedRows) Clone(v int32) int32                         { return v }

type nilBufferedParams struct{}

func (nilBufferedParams) Compile(Columns) (CompiledParams[int32], error) { return nil, nil }

type customStreamResettable struct{}

func (customStreamResettable) Compile(t DataType) (CompiledStreamValue[int32], error) {
	return StreamInt32.Compile(t)
}
func (customStreamResettable) Reset(dst *int32) { *dst = 0 }

// streamWords records the reader's view of a cell: its type and state and,
// for a present cell, a big-endian uint16 followed by a uint64.
type streamWords struct {
	Type  DataType
	State CellState
	Short uint16
	Long  uint64
}
type streamWordsDecoder struct{}

func (streamWordsDecoder) Compile(t DataType) (CompiledStreamValue[streamWords], error) {
	if t.Kind() != KindBlob {
		return nil, errors.New("expected blob")
	}
	return streamWordsDecoder{}, nil
}
func (streamWordsDecoder) Reset(dst *streamWords) { *dst = streamWords{} }
func (streamWordsDecoder) DecodeInto(r *StreamValueReader, dst *streamWords) error {
	dst.Type, dst.State = r.Type(), r.State()
	if dst.State != CellPresent {
		return nil
	}
	var err error
	if dst.Short, err = r.Uint16(); err != nil {
		return err
	}
	if dst.Long, err = r.Uint64(); err != nil {
		return err
	}
	return r.RequireEnd()
}
