// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	. "github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestTypedFrame(t *testing.T) {
	t.Run("BuildTypedRequest", func(t *testing.T) {
		t.Run("EmptyContinuationIsPresent", func(t *testing.T) {
			c := protocolSettings(4)

			frame, err := c.BuildTypedRequest(&model.AttemptRequest{Statement: "q", Consistency: One, PagingState: []byte{}}, 2)
			if err != nil {
				t.Fatal(err)
			}
			packet := typedTestBytes(t, frame)
			if packet[16]&byte(frm.FlagWithPagingState) == 0 || len(packet) != 21 {
				t.Fatalf("empty continuation omitted: %x", packet)
			}
			e := NewTypedEnvelope()
			e.U32(frm.ResultKindRows)
			e.U32(uint32(frm.FlagHasMorePages))
			e.U32(0)
			e.U32(0)
			e.U32(0)
			body, _ := e.Freeze()
			owner := wire.NewLease(body, nil)
			defer owner.Release()
			page, err := c.ParseTypedResponse(frm.FrameHeader{Version: 0x84, Op: frm.OpResult}, body, owner)
			if err != nil {
				t.Fatal(err)
			}
			if page.PagingState == nil || len(page.PagingState) != 0 {
				t.Fatal("present empty continuation collapsed into completion")
			}
		})
		t.Run("PreservesBodyLimitAndErrors", func(t *testing.T) {
			for _, version := range []byte{3, 4, 5} {
				t.Run(string(rune('0'+version)), func(t *testing.T) {
					settings := protocolSettings(version)
					// QUERY contains a four-byte string length, one CQL byte, two-byte
					// consistency and one-byte flags (four bytes for native v5).
					bodySize := int64(8)
					if version == 5 {
						bodySize += 3
					}
					settings.MaxFrameBytes = bodySize
					request := &model.AttemptRequest{Statement: "q", Consistency: cqltype.One, Trace: true}
					chain, err := settings.BuildTypedRequest(request, 23)
					if err != nil {
						t.Fatalf("body exactly at limit: %v", err)
					}
					native := typedTestBytes(t, chain)
					if version == 5 {
						reader := bytes.NewReader(native)
						header, err := segment.ReadHeader(reader, false)
						if err != nil {
							t.Fatal(err)
						}
						owner, err := segment.ReadOwnedPayload(reader, header, nil)
						if err != nil {
							t.Fatal(err)
						}
						native = typedTestBytes(t, owner.Chain())
						owner.Release()
					}
					header, err := frm.DecodeHeader(native)
					if err != nil || header.Length != int(bodySize) || len(native) != HeadSize+int(bodySize) || header.Stream != 23 || header.Op != frm.OpQuery || header.Flags != frm.FlagTracing {
						t.Fatalf("native header = %+v, bytes=%d, error=%v", header, len(native), err)
					}
					settings.MaxFrameBytes--
					if _, err := settings.BuildTypedRequest(request, 23); !errors.Is(err, ErrFrameTooBig) {
						t.Fatalf("body above limit: %v", err)
					}
					// An earlier encoding error takes precedence over the later size
					// check, including when the rest of the request exceeds the limit.
					request.PreparedID = make([]byte, 1<<16)
					request.PagingState = make([]byte, 32)
					if _, err := settings.BuildTypedRequest(request, 23); err == nil || !strings.Contains(err.Error(), "oversized short bytes") {
						t.Fatalf("sticky encoding error lost: %v", err)
					}
				})
			}
		})
	})
	// A frame built in pooled scratch is byte-identical to an ordinary frame and
	// still references borrowed values instead of copying them.
	t.Run("BuildTypedRequestInto", func(t *testing.T) {
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
	})
	// The incremental parser must charge its column descriptors before reading any
	// column strings or types, including when its cursor is supplied by transport.
	t.Run("MetadataBudget", func(t *testing.T) {
		body := binary.BigEndian.AppendUint32(nil, 0)
		body = binary.BigEndian.AppendUint32(body, 1)
		body = append(body, make([]byte, 16)...)
		source := &countedMetadataSource{Reader: bytes.NewReader(body)}
		scope, err := wire.NewStream(context.Background(), source, int64(len(body)), wire.StreamLimits{})
		if err != nil {
			t.Fatal(err)
		}
		budget := &StreamMetadataBudget{Limit: 95}
		cursor := &StreamMetadataCursor{Scope: scope, Budget: budget}
		parser := &TypedParser{Cursor: cursor, Proto: 4, MaxDepth: 64}
		var metadata ParsedMetadata
		parser.MetadataFields(&metadata, false, 0)
		if !errors.Is(parser.Err, wire.ErrLimit) || source.consumed != 8 || budget.Bytes() != 0 {
			t.Fatalf("budget boundary: err=%v consumed=%d live=%d", parser.Err, source.consumed, budget.Bytes())
		}
		cursor.Release()
		if budget.Bytes() != 0 {
			t.Fatal("failed metadata retained a charge")
		}
	})
	t.Run("MetadataReuse", func(t *testing.T) {
		t.Run("IdentityAndChangedValidation", func(t *testing.T) {
			settings := protocolSettings(4)
			settings.MetadataReuse = new(MetadataReuse)
			parse := func(data []byte) (cqltype.Columns, error) {
				// Fragment every byte to exercise comparison across span boundaries.
				spans := make([]wire.Span, len(data))
				for i := range data {
					spans[i] = wire.Borrow(data[i : i+1])
				}
				chain, _ := wire.NewChain(spans...)
				lease := wire.NewLease(chain, nil)
				defer lease.Release()
				page, err := settings.ParseTypedResponse(frm.FrameHeader{Version: 0x84, Op: frm.OpResult}, chain, lease)
				if page == nil {
					return cqltype.Columns{}, err
				}
				return page.Columns, err
			}
			original := reuseMetadataBody("value", uint16(cqltype.TypeBlob))
			first, err := parse(original)
			if err != nil {
				t.Fatal(err)
			}
			second, err := parse(bytes.Clone(original))
			if err != nil {
				t.Fatal(err)
			}
			if &cqltype.ColumnSlice(first)[0] != &cqltype.ColumnSlice(second)[0] {
				t.Fatal("matching validated metadata was reallocated")
			}
			changed, err := parse(reuseMetadataBody("other", uint16(cqltype.TypeInt)))
			if err != nil {
				t.Fatal(err)
			}
			if changed.Equal(first) {
				t.Fatal("changed metadata reused the old shape")
			}
			if _, err := parse(reuseMetadataBody("other", 0xffff)); err == nil {
				t.Fatal("invalid changed type bypassed validation")
			}
			for n := 0; n < len(original); n++ {
				if _, err := parse(original[:n]); err == nil {
					t.Fatalf("truncated metadata at %d accepted", n)
				}
			}
		})
		t.Run("ReturnsExpectedColumns", func(t *testing.T) {
			settings := protocolSettings(4)
			settings.MetadataReuse = new(MetadataReuse)
			parse := func(data []byte, expect cqltype.Columns) cqltype.Columns {
				t.Helper()
				chain, _ := wire.NewChain(wire.Borrow(data))
				lease := wire.NewLease(chain, nil)
				defer lease.Release()
				page := &model.ResponseData{Columns: expect}
				if _, err := settings.ParseTypedResponseInto(frm.FrameHeader{Version: 0x84, Op: frm.OpResult}, chain, lease, page); err != nil {
					t.Fatal(err)
				}
				return page.Columns
			}
			body := reuseMetadataBody("value", uint16(cqltype.TypeBlob))
			parsed := parse(body, cqltype.Columns{})
			blob := cqltype.BlobType
			expect := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "table", Name: "value", Type: blob})
			if parsed.Same(expect) || !parsed.Equal(expect) {
				t.Fatal("setup: expected columns must be equal, not the same")
			}
			// Equal metadata comes back as the expected columns themselves, also
			// once they are remembered.
			for i := 0; i < 2; i++ {
				if got := parse(body, expect); !got.Same(expect) {
					t.Fatal("equal metadata did not return the expected columns", i)
				}
			}
			// Without expectations, or expecting other columns, the parsed ones return.
			if got := parse(body, cqltype.Columns{}); !got.Same(parsed) {
				t.Fatal("metadata without expectation returned other columns")
			}
			other := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "table", Name: "other", Type: blob})
			if got := parse(body, other); !got.Same(parsed) {
				t.Fatal("unequal expected columns were returned")
			}
			// Changed metadata forgets the expected columns it matched before.
			changed := parse(reuseMetadataBody("other", uint16(cqltype.TypeInt)), expect)
			if changed.Equal(expect) {
				t.Fatal("changed metadata returned the expected columns")
			}
		})
	})
	// A request encoded from its parameters template is byte-identical to one
	// encoded field by field, for every combination of options, and a template
	// for other options is ignored.
	t.Run("ParamsTemplate", func(t *testing.T) {
		value := []byte{0, 0, 0, 5, 'v', 'a', 'l', 'u', 'e'}
		values, _ := wire.NewChain(wire.Borrow(value))
		now := int32(7)
		cases := 0
		for _, version := range []byte{3, 4, 5} {
			settings := protocolSettings(version)
			for mask := 0; mask < 1<<9; mask++ {
				bit := func(i int) bool { return mask&(1<<i) != 0 }
				if version < 5 && (bit(5) || bit(6)) {
					continue // keyspace and now_in_seconds require v5
				}
				r := &model.AttemptRequest{Statement: "SELECT v FROM ks.t WHERE k=?", Consistency: cqltype.LocalQuorum, Timestamp: 0x0102030405060708}
				if bit(0) {
					r.Statement, r.PreparedID, r.ResultMetadataID = "", []byte("prepared-id"), []byte("md")
				}
				if bit(1) {
					r.Values, r.ValueCount = values, 1
				}
				if bit(2) {
					r.PageSize = 100
				}
				if bit(3) {
					r.SerialConsistency = cqltype.LocalSerial
				}
				if bit(4) {
					r.DefaultTimestamp = true
				}
				if bit(5) {
					r.Keyspace = "ks"
				}
				if bit(6) {
					r.HasNowInSeconds, r.NowInSeconds = true, now
				}
				if bit(7) {
					r.PagingState = []byte("state")
				}
				if bit(8) {
					r.SkipMetadata = true
				}
				plain, err := settings.BuildTypedRequest(r, 9)
				if err != nil {
					t.Fatal(version, mask, err)
				}
				want := typedTestBytes(t, plain)

				template, err := NewParamsTemplate(r, version)
				if err != nil {
					t.Fatal(version, mask, err)
				}
				if !template.Matches(r, version) {
					t.Fatalf("v%d mask %#x: template does not match its own request", version, mask)
				}
				r.Params = template
				got, err := settings.BuildTypedRequest(r, 9)
				if err != nil {
					t.Fatal(version, mask, err)
				}
				if !bytes.Equal(typedTestBytes(t, got), want) {
					t.Fatalf("v%d mask %#x: templated frame differs\n got %x\nwant %x", version, mask, typedTestBytes(t, got), want)
				}

				// A template for another page size, or another protocol, does
				// not match, so the caller does not attach it.
				other := *r
				other.PageSize = r.PageSize + 7
				if stale, _ := NewParamsTemplate(&other, version); stale.Matches(r, version) {
					t.Fatalf("v%d mask %#x: template for another page size matches", version, mask)
				}
				if template.Matches(r, version^1) {
					t.Fatalf("v%d mask %#x: template matches another protocol", version, mask)
				}
				cases++
			}
		}
		if cases == 0 {
			t.Fatal("no cases ran")
		}
	})
	t.Run("ParseTypedResponse", func(t *testing.T) {
		t.Run("MetadataAtEverySpanSplit", func(t *testing.T) {
			value := []byte("fragmented row bytes")
			body := typedTestRowsBody(value)
			for split := 0; split <= len(body); split++ {
				chain := typedTestChain(t, body[:split], body[split:])
				owner := wire.NewLease(chain, nil)
				c := protocolSettings(5)
				page, err := c.ParseTypedResponse(frm.FrameHeader{Version: 0x85, Op: frm.OpResult}, chain, owner)
				if err != nil {
					t.Fatalf("split %d: %v", split, err)
				}
				if page.RowCount != 1 || page.Columns.Len() != 1 {
					t.Fatalf("split %d: wrong metadata", split)
				}
				cursor := wire.NewCursor(page.Rows)
				n, _ := cursor.Uint32()
				cell, err := cursor.Take(int64(n))
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(typedTestBytes(t, cell), value) {
					t.Fatal("row changed")
				}
				offset := len(body) - len(value)
				_ = cell.Each(func(s wire.Span) error {
					b := s.Bytes()
					if &b[0] != &body[offset] {
						t.Fatal("parser copied a cell")
					}
					offset += len(b)
					return nil
				})
				owner.Release()
			}
			for n := 0; n < len(body)-len(value); n++ {
				chain := typedTestChain(t, body[:n])
				owner := wire.NewLease(chain, nil)
				_, err := (protocolSettings(5)).ParseTypedResponse(frm.FrameHeader{Version: 0x85, Op: frm.OpResult}, chain, owner)
				owner.Release()
				if err == nil {
					t.Fatalf("accepted truncated metadata at %d", n)
				}
			}
		})
		t.Run("MetadataIDsAndPagingOrder", func(t *testing.T) {
			e := NewTypedEnvelope()
			e.U32(frm.ResultKindRows)
			e.U32(uint32(frm.FlagGlobalTableSpec | frm.FlagHasMorePages | frm.FlagMetaDataChanged))
			e.U32(1)
			e.Bytes([]byte("page-state"), false)
			e.Bytes([]byte("new-metadata-id"), true)
			e.Str("ks", false)
			e.Str("tbl", false)
			e.Str("v", false)
			e.U16(uint16(TypeInt))
			e.U32(1)
			e.U32(4)
			e.U32(19)
			body, err := e.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			bytesBody := typedTestBytes(t, body)
			var parts [][]byte
			for i := range bytesBody {
				parts = append(parts, bytesBody[i:i+1])
			}
			body = typedTestChain(t, parts...)
			owner := wire.NewLease(body, nil)
			defer owner.Release()
			page, err := (protocolSettings(5)).ParseTypedResponse(frm.FrameHeader{Version: 0x85, Op: frm.OpResult}, body, owner)
			if err != nil {
				t.Fatal(err)
			}
			if string(page.PagingState) != "page-state" || string(page.MetadataID) != "new-metadata-id" {
				t.Fatal("paging state and metadata ID were swapped")
			}
			if page.ColumnCount != 1 || page.NoMetadata {
				t.Fatal("wrong metadata flags")
			}
		})
		t.Run("RejectsChangedMetadataWithoutColumns", func(t *testing.T) {
			e := NewTypedEnvelope()
			e.U32(frm.ResultKindRows)
			e.U32(uint32(frm.FlagNoMetaData | frm.FlagMetaDataChanged))
			e.U32(1)
			e.Bytes(nil, true)
			e.U32(0)
			body, _ := e.Freeze()
			owner := wire.NewLease(body, nil)
			defer owner.Release()
			if _, err := (protocolSettings(5)).ParseTypedResponse(frm.FrameHeader{Version: 0x85, Op: frm.OpResult}, body, owner); err == nil {
				t.Fatal("accepted changed metadata without column specifications")
			}
		})
	})
}
