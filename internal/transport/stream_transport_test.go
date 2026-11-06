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

package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"testing"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/testkit"
	lz4 "github.com/scylladb/gocql/v2/internal/wirecompress"
)

func TestStreamTransport(t *testing.T) {
	t.Run("ExclusiveIO", func(t *testing.T) {
		for _, gate := range []string{"closing", "closed", "connection"} {
			for _, cause := range []error{nil, context.Canceled, context.DeadlineExceeded} {
				ctx, cancel := context.WithCancel(context.Background())
				if gate == "connection" {
					cancel()
				}
				r := &InternalExclusiveConn{InternalConn: &Conn{InternalCtx: ctx}, terminalCause: cause}
				switch gate {
				case "closing":
					r.InternalClosing.Store(true)
				case "closed":
					r.closed = true
				}
				called := false
				err := r.InternalWithIO(context.Background(), func() error { called = true; return nil })
				cancel()
				want := cause
				if want == nil {
					want = ErrClosed
				}
				if called || err != want {
					t.Fatalf("%s cause=%v: called=%v err=%v, want %v", gate, cause, called, err, want)
				}
			}
		}
		// An explicit manual close keeps its terminal error even if the supplied
		// page context is also canceled; only a recorded winning cause takes priority.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		r := &InternalExclusiveConn{}
		r.InternalClosing.Store(true)
		if err := r.InternalWithIO(ctx, func() error { return nil }); err != ErrClosed {
			t.Fatal("manual close precedence changed", err)
		}
	})
	// Fixes the result bytes/cells/rows while independently changing segment count.
	// Source unit and application output are established separately and reported.
	// HeapAlloc samples are diagnostics, and actual driver byte capacities remain
	// the deterministic bound; GC samples include runtime metadata and parser plans.
	t.Run("SegmentCountMemory", func(t *testing.T) {
		for _, compressed := range []bool{false, true} {
			for _, width := range []int{512, 8192, 130000} {
				t.Run(fmt.Sprintf("compressed%t/width%d", compressed, width), func(t *testing.T) {
					const size = 1 << 20
					source := &testUnits{input: testRows(5, 1, size), width: width, compressed: compressed}
					memory := &generatedConn{memoryConn: &memoryConn{}, input: source}
					var comp protocol.Compressor
					if compressed {
						comp = lz4.LZ4Compressor{}
					}
					dst := make([]byte, 0, size)
					backing := unsafe.SliceData(dst[:cap(dst)])
					runtime.GC()
					var idle runtime.MemStats
					runtime.ReadMemStats(&idle)
					r := memoryReservation(5, comp, memory)
					defer r.InternalClose(false)
					response, e := r.InternalExchange(context.Background(), &model.AttemptRequest{Statement: "SELECT value", Consistency: cqltype.One})
					if e != nil {
						t.Fatal(e)
					}
					defer response.InternalReleaseMetadata()
					if e = response.InternalInstallColumns(response.InternalColumns); e != nil {
						t.Fatal(e)
					}
					plan := codecTestMust(codec.StreamOneColumn(codec.StreamBlob).Compile(response.InternalColumns))
					var peak uint64
					var peakCapacity int64
					sample := func() {
						peakCapacity = max(peakCapacity, response.InternalWorkingStorageBytes())
						runtime.GC()
						var m runtime.MemStats
						runtime.ReadMemStats(&m)
						peak = max(peak, m.HeapAlloc)
					}
					r.InternalConn.InternalR.(*InternalConnReader).InternalConn = &observedConn{Conn: memory, observe: sample}
					if ok, e := InternalDecodeStreamRow(response, plan, &dst); !ok || e != nil {
						t.Fatal(ok, e)
					}
					if unsafe.SliceData(dst) != backing || len(dst) != size || bytes.Count(dst, []byte{0x71}) != size {
						t.Fatal("output changed")
					}
					sample()
					if peakCapacity > 320<<10 {
						t.Fatal("segment count grew driver storage", peakCapacity)
					}
					t.Logf("segments=%d source_unit_capacity=%d final_output_capacity=%d peak_driver_byte_capacity=%d idle_heap=%d peak_postGC_heap=%d transform_bytes=%d unit_to_parser_bytes=%d", source.units, source.sourceCapacity, cap(dst), peakCapacity, idle.HeapAlloc, peak, source.transformed, r.InternalUnits.InternalCopies)
				})
			}
		}
	})
	t.Run("ReadTargetsAndWorkingMemory", func(t *testing.T) {
		for _, mode := range []struct {
			version    byte
			compressor protocol.Compressor
		}{{4, nil}, {5, nil}, {5, lz4.LZ4Compressor{}}} {
			var storage int64
			for _, size := range []int{400 << 10, 8 << 20} {
				payload := make([]byte, size)
				rand.New(rand.NewSource(42)).Read(payload)
				encoded := encodedResponse(mode.version, mode.compressor != nil, payload)
				source := &memoryConn{source: bytes.NewReader(encoded)}
				r := memoryReservation(mode.version, mode.compressor, source)
				dst := make([]byte, 0, size)
				source.start = uintptr(unsafe.Pointer(&dst[:cap(dst)][0]))
				source.end = source.start + uintptr(size)
				response, err := r.InternalExchange(context.Background(), &model.AttemptRequest{Statement: "SELECT value", Consistency: cqltype.One})
				if err != nil {
					t.Fatal(err)
				}
				if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
					t.Fatal(err)
				}
				plan := codecTestMust(codec.StreamOneColumn(codec.StreamBlob).Compile(response.InternalColumns))
				if ok, err := InternalDecodeStreamRow(response, plan, &dst); !ok || err != nil || !bytes.Equal(dst, payload) {
					t.Fatal(ok, err)
				}
				if mode.version == 4 {
					// A native body may read ahead one bounded prefix; bulk
					// payload still targets the final caller ranges.
					if source.directBytes < int64(size)-(4<<10) || source.directBytes > int64(size) || source.otherBulk > 4<<10 {
						t.Fatal("raw payload did not target final ranges", source.directBytes, source.otherBulk)
					}
				} else {
					now := r.InternalUnits.InternalUnits.StorageBytes()
					if now > r.InternalLimits.MaxTransformBytes || storage != 0 && now != storage {
						t.Fatal("working memory grew with response bytes", storage, now)
					}
					storage = now
				}
				if response.InternalBudget.Used >= 16<<10 {
					t.Fatal("metadata allocations grew with cell", response.InternalBudget.Used)
				}
				response.InternalReleaseMetadata()
				r.InternalClose(true)
			}
		}
	})
}

type testUnits struct {
	input              io.Reader
	width              int
	compressed         bool
	pending            []byte
	units, transformed int64
	sourceCapacity     int
}

func (s *testUnits) Read(p []byte) (int, error) {
	if len(s.pending) == 0 {
		data := make([]byte, s.width)
		n, e := io.ReadFull(s.input, data)
		if n == 0 {
			return 0, e
		}
		if e != nil && e != io.ErrUnexpectedEOF {
			return 0, e
		}
		s.pending = testkit.Unit(data[:n], s.compressed, false)
		s.sourceCapacity = max(s.sourceCapacity, cap(data)+cap(s.pending))
		s.units++
		s.transformed += int64(n)
	}
	n := copy(p, s.pending)
	s.pending = s.pending[n:]
	return n, nil
}
