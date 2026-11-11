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

package session

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/paging"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func BenchmarkPaging(b *testing.B) {
	b.Run("CallerDestination", func(b *testing.B) {
		type row struct {
			Value   cqltype.Bytes
			Padding [128]int64
		}
		typ := cqltype.BlobType
		columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: typ})
		decoder := codec.Row(codec.Column("value", codec.BlobCodec, func(r *row) *cqltype.Bytes { return &r.Value }))
		for _, fragments := range []int{1, 1024} {
			b.Run(fmt.Sprintf("fragments=%d", fragments), func(b *testing.B) {
				data := binary.BigEndian.AppendUint32(nil, 512000)
				data = append(data, make([]byte, 512000)...)
				spans := make([]wire.Span, 0, fragments)
				for i := 0; i < fragments; i++ {
					lo, hi := len(data)*i/fragments, len(data)*(i+1)/fragments
					spans = append(spans, wire.Borrow(data[lo:hi]))
				}
				chain := codecTestMust(wire.NewChain(spans...))
				owner := wire.NewLease(chain, nil)
				defer owner.Release()
				p := func() *Page[row] {
					opaqueStorageValuexx := new(Page[row])
					PageData(opaqueStorageValuexx).response = &transport.InternalResponsePage{ResponseData: model.ResponseData{Columns: columns, Rows: chain, RowCount: 1, Owner: owner}}
					PageData(opaqueStorageValuexx).plan = codecTestMust(decoder.Compile(columns))
					PageData(opaqueStorageValuexx).InternalCursor = wire.CursorOver(chain)
					return opaqueStorageValuexx
				}()
				cursor := PageData(p).InternalCursor
				var dst row
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					PageData(p).InternalCursor = cursor
					PageData(p).index = 0
					PageData(p).exhausted = false
					if !p.NextInto(&dst) {
						b.Fatal(p.Err())
					}
				}
			})
		}
	})

	b.Run("HistoryLookup", func(b *testing.B) {
		for _, count := range []int{1, 1024, 65536} {
			b.Run(fmt.Sprint(count), func(b *testing.B) {
				h := paging.InternalPagingHistory{InternalMaxCount: count, InternalMaxBytes: 16 << 20}
				var state model.PageState
				for i := 0; i < count; i++ {
					state = model.PageStateFromToken(binary.LittleEndian.AppendUint64(nil, uint64(i)))
					if err := h.InternalAdd(state); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if h.InternalAdd(state) != runtimeerrors.ErrPagingNoProgress {
						b.Fatal("history lost state")
					}
				}
			})
		}
	})

	b.Run("ResultPlanExactEntry", func(b *testing.B) {
		columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: cqltype.BlobType})
		entry := &model.ResultSchemaIdentity{InternalColumns: columns}
		response := &transport.InternalResponsePage{InternalMetadata: entry, ResponseData: model.ResponseData{Columns: columns}}
		cache := &InternalResultPlanCache[cqltype.Bytes]{}
		decoder := codec.OneColumn(codec.BlobCodec)
		if _, err := cache.InternalSelectPlan(response, decoder); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := cache.InternalSelectPlan(response, decoder); err != nil {
				b.Fatal(err)
			}
		}
	})
}
