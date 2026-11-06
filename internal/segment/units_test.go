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

package segment

import (
	"bytes"
	"context"

	"io"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type shortUnitReader struct {
	source io.Reader
	chunk  int
	read   int
}

func (r *shortUnitReader) Read(p []byte) (int, error) {
	n, e := r.source.Read(p[:min(len(p), r.chunk)])
	r.read += n
	return n, e
}
func encodedUnit(t testing.TB, payload []byte, compressed, self bool) []byte {
	t.Helper()
	c, _ := wire.NewChain(wire.Borrow(payload))
	encoded, e := EncodeSpans(c, self, compressed)
	if e != nil {
		t.Fatal(e)
	}
	out := make([]byte, encoded.Len())
	if _, e = encoded.CopyTo(out); e != nil {
		t.Fatal(e)
	}
	return out
}

type wrongStorageDecoder struct{}

func (wrongStorageDecoder) AppendCompressed(dst, src []byte) ([]byte, error) { panic("unused") }
func (wrongStorageDecoder) AppendDecompressed(dst, src []byte, n uint32) ([]byte, error) {
	out, e := (independentLZ4{}).AppendDecompressed(nil, src, n)
	return out, e
}
func FuzzValidatedUnits(f *testing.F) {
	f.Add([]byte("split and validate"), uint8(1), false)
	f.Add(bytes.Repeat([]byte("long pattern"), 100), uint8(3), true)
	f.Fuzz(func(t *testing.T, payload []byte, chunk uint8, compressed bool) {
		if len(payload) > MaxPayloadSize {
			t.Skip()
		}
		encoded := encodedUnit(t, payload, compressed, false)
		var comp Compressor
		if compressed {
			comp = independentLZ4{}
		}
		source := &shortUnitReader{source: bytes.NewReader(encoded), chunk: int(chunk) + 1}
		r, _ := NewUnitReader(context.Background(), source, comp, UnitLimits{})
		_, out, e := r.Next()
		if e != nil || !bytes.Equal(out, payload) {
			t.Fatalf("round trip: %v", e)
		}
		if len(encoded) > HeaderSize(compressed)+4 {
			encoded[HeaderSize(compressed)] ^= 1
			r, _ = NewUnitReader(context.Background(), bytes.NewReader(encoded), comp, UnitLimits{})
			if _, out, e := r.Next(); e == nil || out != nil {
				t.Fatal("corrupt payload published")
			}
		}
	})
}

type storedUnitCodec struct{}

func (storedUnitCodec) AppendCompressed(dst, src []byte) ([]byte, error) {
	return append(dst, src...), nil
}
func (storedUnitCodec) AppendDecompressed(dst, src []byte, n uint32) ([]byte, error) { panic("unused") }
