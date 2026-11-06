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

package codec

import (
	"errors"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

type customStreamPair struct {
	A int32
	B []byte
}
type customStreamDecoder struct{ checks *int }
type customStreamPlan struct {
	integer           StreamValuePlan[int32]
	blob              StreamValuePlan[[]byte]
	intType, blobType cqltype.DataType
}

func (c customStreamDecoder) Compile(t cqltype.DataType) (StreamValuePlan[customStreamPair], error) {
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
	blob, err := StreamBlob.Compile(b.Type)
	if err != nil {
		return nil, err
	}
	return customStreamPlan{integer, blob, a.Type, b.Type}, nil
}
func (p customStreamPlan) Reset(dst *customStreamPair) { p.integer.Reset(&dst.A); p.blob.Reset(&dst.B) }
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

func (nilStreamValue) Compile(cqltype.DataType) (StreamValuePlan[int32], error) { return nil, nil }

type nilStreamRows struct{}

func (nilStreamRows) Compile(cqltype.Columns) (StreamRowPlan[int32], error) { return nil, nil }

type nilBufferedRows struct{}

func (nilBufferedRows) Compile(cqltype.Columns) (RowPlan[int32], error) { return nil, nil }
func (nilBufferedRows) Clone(v int32) int32                             { return v }

type nilBufferedParams struct{}

func (nilBufferedParams) Compile(cqltype.Columns) (ParamPlan[int32], error) { return nil, nil }

type customStreamNoReset struct{}

func (customStreamNoReset) Compile(t cqltype.DataType) (StreamValuePlan[int32], error) {
	return StreamInt32.Compile(t)
}

type customStreamResettable struct{ customStreamNoReset }

func (customStreamResettable) Reset(dst *int32) { *dst = 0 }
