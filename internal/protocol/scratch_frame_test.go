// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"bytes"
	"testing"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// A frame built in pooled scratch is byte-identical to an ordinary frame and
// still references borrowed values instead of copying them.
func TestScratchFrameMatchesAndBorrowsValues(t *testing.T) {
	value := []byte{0, 0, 0, 5, 'v', 'a', 'l', 'u', 'e'}
	values, _ := wire.NewChain(wire.Borrow(value))
	now := int32(7)
	shapes := map[string]func(v byte) *model.AttemptRequest{
		"query": func(byte) *model.AttemptRequest {
			return &model.AttemptRequest{Statement: "SELECT v FROM ks.t WHERE k=?", Values: values, ValueCount: 1, Consistency: cqltype.Quorum, PageSize: 100, PagingState: []byte("state"), Timestamp: 42, DefaultTimestamp: true}
		},
		"execute": func(v byte) *model.AttemptRequest {
			r := &model.AttemptRequest{PreparedID: []byte("prepared-id"), ResultMetadataID: []byte("md"), Values: values, ValueCount: 1, Consistency: cqltype.One, TabletVersionBlock: 3}
			if v == 5 {
				r.Keyspace, r.HasNowInSeconds, r.NowInSeconds = "ks", true, now
			}
			return r
		},
		"batch": func(byte) *model.AttemptRequest {
			return &model.AttemptRequest{Consistency: cqltype.One, Batch: &model.TypedBatchRequest{Items: []model.TypedBatchItem{{Statement: "INSERT", Values: values, ValueCount: 1}}}}
		},
	}
	scratch := wire.NewBuilderScratch(256, 16)
	for _, version := range []byte{3, 4, 5} {
		for name, shape := range shapes {
			settings := protocolSettings(version)
			settings.TabletsRoutingV2 = true
			req := shape(version)
			plain, err := settings.BuildTypedRequest(req, 9)
			if err != nil {
				t.Fatal(version, name, err)
			}
			want := typedTestBytes(t, plain)
			scratch.Reset()
			got, used, err := settings.BuildTypedRequestInto(req, 9, scratch)
			if err != nil || !used {
				t.Fatal(version, name, used, err)
			}
			if !bytes.Equal(typedTestBytes(t, got), want) {
				t.Fatalf("v%d %s: scratch frame differs", version, name)
			}
			if version == 5 {
				continue // segments re-frame spans; covered by the segment tests
			}
			borrowed := false
			_ = got.Each(func(s wire.Span) error {
				if p := s.Bytes(); len(p) > 0 && unsafe.SliceData(p) == unsafe.SliceData(value) {
					borrowed = true
				}
				return nil
			})
			if !borrowed {
				t.Fatalf("v%d %s: borrowed value was copied", version, name)
			}
		}
	}
}
