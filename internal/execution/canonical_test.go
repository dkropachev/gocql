// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type testWriter interface {
	InternalWriteContext(context.Context, []byte) (int, error)
	InternalSetWriteTimeout(time.Duration)
}
type canonicalWriter struct {
	testWriter
	payload []byte
	bytes   atomic.Int64
}

func (w *canonicalWriter) InternalWriteSpans(ctx context.Context, chain wire.Chain) (int64, error) {
	if e := chain.Each(func(s wire.Span) error {
		p := s.Bytes()
		if len(p) == len(w.payload) && bytes.Equal(p, w.payload) {
			if unsafe.SliceData(p) != unsafe.SliceData(w.payload) {
				return fmt.Errorf("canonical payload was copied")
			}
			w.bytes.Add(int64(len(p)))
		}
		return nil
	}); e != nil {
		return 0, e
	}
	return w.testWriter.(interface {
		InternalWriteSpans(context.Context, wire.Chain) (int64, error)
	}).InternalWriteSpans(ctx, chain)
}
func TestCanonicalAssembly(t *testing.T) {
	for _, size := range []int{0, 32, 4096, 500 << 10} {
		for _, proto := range []byte{3, 4, 5} {
			for _, owned := range []bool{false, true} {
				t.Run(fmt.Sprintf("bytes%d/v%d/owned%t", size, proto, owned), func(t *testing.T) {
					typ := cqltype.BlobType
					cols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: typ})
					calls := 0
					input := bytes.Repeat([]byte{0x71}, size)
					statement := binding.Statement[[]byte, codec.EmptyRecord]{Kind: cqltype.OrdinaryStatement, CQL: "INSERT INTO ks.tbl(v)VALUES(?)", Params: codec.OneParam[[]byte](countingBlobEncoder{calls: &calls}), Rows: codec.EmptyRowDecoder}
					var bound *Bound[codec.EmptyRecord]
					if owned {
						bound = codecTestMust(BindUnpreparedOwned(statement, cols, input))
					} else {
						bound = codecTestMust(BindUnpreparedBorrowed(statement, cols, input))
					}
					defer bound.Close()
					canonical := BoundData(bound).InternalRequest.InternalValues
					c := &transport.Conn{InternalVersion: proto}
					c.InternalInitFramerCache()
					for _, batch := range []bool{false, true} {
						for attempt := 0; attempt < 4; attempt++ {
							request := &model.AttemptRequest{Statement: statement.CQL, Values: canonical.Framed, ValueCount: 1, Consistency: cqltype.One}
							if batch {
								request.Batch = &model.TypedBatchRequest{Kind: cqltype.LoggedBatch, Items: []model.TypedBatchItem{{Statement: statement.CQL, Values: canonical.Framed, ValueCount: 1}, {Statement: statement.CQL, Values: canonical.Framed, ValueCount: 1}}}
							}
							frame := codecTestMust(c.InternalBuildTypedRequest(request, attempt))
							retained := int64(0)
							frame.Each(func(s wire.Span) error {
								p := s.Bytes()
								if len(p) == 0 {
									return nil
								}
								start := uintptr(unsafe.Pointer(unsafe.SliceData(p)))
								canonical.Cells[0].Body.Each(func(original wire.Span) error {
									o := original.Bytes()
									if len(o) > 0 {
										lo := uintptr(unsafe.Pointer(unsafe.SliceData(o)))
										if start >= lo && start+uintptr(len(p)) <= lo+uintptr(len(o)) {
											retained += int64(len(p))
										}
									}
									return nil
								})
								return nil
							})
							want := int64(size)
							if batch {
								want *= 2
							}
							if retained != want {
								t.Fatalf("attempt=%d batch=%t canonical bytes=%d want=%d", attempt, batch, retained, want)
							}
						}
					}
					if calls != 1 {
						t.Fatal("secondary encode", calls)
					}
					t.Logf("encoder_calls=1 attempt_envelopes=8 payload_backing_reused bytes=%d", size)
				})
			}
		}
	}
}
