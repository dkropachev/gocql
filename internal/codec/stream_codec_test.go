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
	"context"
	"encoding/binary"
	"io"
	"reflect"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type streamChunkSource struct {
	data                []byte
	offset, step, reads int
	record              func([]byte, int)
}

func (s *streamChunkSource) Read(p []byte) (int, error) {
	if s.offset == len(s.data) {
		return 0, io.EOF
	}
	n := min(len(p), len(s.data)-s.offset)
	if s.step > 0 {
		n = min(n, s.step)
	}
	if s.record != nil {
		s.record(p[:n], s.offset)
	}
	copy(p, s.data[s.offset:s.offset+n])
	s.offset += n
	s.reads++
	return n, nil
}
func StreamCell(body []byte) []byte {
	b := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(b, uint32(len(body)))
	copy(b[4:], body)
	return b
}
func streamEncodedFixture[T any](t testing.TB, typ cqltype.DataType, encoder Encoder[T], value T) []byte {
	t.Helper()
	v, err := EncodeValueBorrowed(typ, encoder, value)
	if err != nil {
		t.Fatal(err)
	}
	if v.state == cqltype.Null {
		return []byte{255, 255, 255, 255}
	}
	return StreamCell(v.Bytes().OwnedBytes())
}
func streamReaderFixture(t testing.TB, columns cqltype.Columns, source io.Reader, size int) (*StreamRowReader, *wire.Stream) {
	t.Helper()
	scope, err := wire.NewStream(context.Background(), source, int64(size), wire.StreamLimits{MaxDepth: 130, DiscardBytes: 127})
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewStreamRowReader(columns, scope)
	if err != nil {
		t.Fatal(err)
	}
	return r, scope
}
func StreamDecodeFixture[T any](t testing.TB, typ cqltype.DataType, decoder StreamDecoder[T], data []byte, dst *T, step int) error {
	t.Helper()
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: typ})
	r, scope := streamReaderFixture(t, columns, &streamChunkSource{data: data, step: step}, len(data))
	plan, err := StreamOneColumn(decoder).Compile(columns)
	if err != nil {
		return err
	}
	plan.Reset(dst)
	if err := plan.DecodeInto(r, dst); err != nil {
		return err
	}
	if err := r.RequireEnd(); err != nil {
		return err
	}
	return scope.RequireEnd()
}
func streamScalarCase[T any](t *testing.T, kind cqltype.Type, encoder Encoder[T], decoder StreamDecoder[T], value T) {
	t.Helper()
	typ := FoundationScalar(t, kind)
	data := streamEncodedFixture(t, typ, encoder, value)
	for step := 1; step <= len(data); step++ {
		var dst T
		if err := StreamDecodeFixture(t, typ, decoder, data, &dst, step); err != nil {
			t.Fatalf("%s split %d: %v", kind, step, err)
		}
		if !reflect.DeepEqual(dst, value) {
			t.Fatalf("%s: %#v != %#v", kind, dst, value)
		}
	}
}
