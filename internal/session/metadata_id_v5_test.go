// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cluster"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/segment"
)

// v5Request is one request frame a protocol 5 fake server read.
type v5Request struct {
	op     byte
	stream int
	body   []byte
}

// readV5Requests reads one uncompressed v5 segment from peer and returns the
// request frames it carries.
func readV5Requests(peer net.Conn) ([]v5Request, error) {
	h, err := segment.ReadHeader(peer, false)
	if err != nil {
		return nil, err
	}
	var scratch segment.Scratch
	payload, err := segment.ReadPayload(peer, h, nil, &scratch)
	if err != nil {
		return nil, err
	}
	var out []v5Request
	for len(payload) > 0 {
		h, err := frm.DecodeHeader(payload)
		if err != nil {
			return nil, err
		}
		end := frm.HeaderSize + h.Length
		if len(payload) < end {
			return nil, fmt.Errorf("frame body exceeds segment")
		}
		out = append(out, v5Request{op: byte(h.Op), stream: h.Stream, body: bytes.Clone(payload[frm.HeaderSize:end])})
		payload = payload[end:]
	}
	return out, nil
}

// v5ShortBytes splits a [short bytes] value off the front of b, reporting
// whether b held one.
func v5ShortBytes(b []byte) ([]byte, []byte, bool) {
	if len(b) < 2 || len(b) < 2+int(binary.BigEndian.Uint16(b)) {
		return nil, nil, false
	}
	n := int(binary.BigEndian.Uint16(b))
	return b[2 : 2+n], b[2+n:], true
}

func appendShortBytes(b, v []byte) []byte {
	b = binary.BigEndian.AppendUint16(b, uint16(len(v)))
	return append(b, v...)
}

// TestResultMetadataIDV5 drives a prepared statement over a protocol 5
// connection (segment framing, no handshake) against a fake server: a
// METADATA_CHANGED response's new result-metadata ID must be the one the next
// EXECUTE sends, and the driver must keep skipping metadata with it. One and
// RunPage read a whole response, RunStream reads it incrementally.
func TestResultMetadataIDV5(t *testing.T) {
	for _, mode := range []struct {
		name string
		run  func(context.Context, *cluster.Core, *Prepared[int32, []byte]) (string, error)
	}{
		{"One", func(ctx context.Context, s *cluster.Core, p *Prepared[int32, []byte]) (string, error) {
			var dst []byte
			found, _, err := p.One(ctx, 1, &dst, CompiledOptions{})
			if err == nil && !found {
				err = fmt.Errorf("no row")
			}
			return string(dst), err
		}},
		{"RunPage", func(ctx context.Context, s *cluster.Core, p *Prepared[int32, []byte]) (string, error) {
			page, err := p.RunPage(ctx, 1, cql.PageState{}, 0, CompiledOptions{})
			if err != nil {
				return "", err
			}
			defer page.Close()
			var dst []byte
			if !page.NextInto(&dst) {
				return "", fmt.Errorf("no row")
			}
			return string(dst), nil
		}},
		{"RunStream", func(ctx context.Context, s *cluster.Core, p *Prepared[int32, []byte]) (string, error) {
			bound, err := BindOwned(p, 1)
			if err != nil {
				return "", err
			}
			defer bound.Close()
			stream, err := RunStream(ctx, s, bound, cql.StreamOneColumn(cql.StreamBlob), StreamOptions{Incremental: true})
			if err != nil {
				return "", err
			}
			defer stream.Close()
			dst := make([]byte, 0, 16) // StreamBlob decodes into caller capacity
			if !stream.NextInto(&dst) {
				return "", fmt.Errorf("no row: %v", stream.Err())
			}
			return string(dst), nil
		}},
	} {
		t.Run(mode.name, func(t *testing.T) { testResultMetadataIDV5(t, mode.run) })
	}
}

func testResultMetadataIDV5(t *testing.T, run func(context.Context, *cluster.Core, *Prepared[int32, []byte]) (string, error)) {
	s, c, peer := rowStreamSession(t, 5, nil)
	if !c.InternalTracksResultMetadataID() {
		t.Fatal("a protocol 5 connection does not track result metadata IDs")
	}
	preparedID := []byte("stmt")
	firstID, secondID := []byte("meta-1"), []byte("meta-2")

	// The value column, as the prepared result and the full ROWS responses
	// carry it: GLOBAL_TABLES_SPEC, ks.tbl, one blob column.
	columnSpec := func(b []byte) []byte {
		for _, s := range []string{"ks", "tbl", "value"} {
			b = binary.BigEndian.AppendUint16(b, uint16(len(s)))
			b = append(b, s...)
		}
		return binary.BigEndian.AppendUint16(b, uint16(cql.KindBlob))
	}
	prepared := binary.BigEndian.AppendUint32(nil, uint32(frm.ResultKindPrepared))
	prepared = appendShortBytes(prepared, preparedID)
	prepared = appendShortBytes(prepared, firstID)
	// <metadata>: one int bind marker, partition key index 0.
	prepared = binary.BigEndian.AppendUint32(prepared, uint32(frm.FlagGlobalTableSpec))
	prepared = binary.BigEndian.AppendUint32(prepared, 1)
	prepared = binary.BigEndian.AppendUint32(prepared, 1)
	prepared = binary.BigEndian.AppendUint16(prepared, 0)
	for _, s := range []string{"ks", "tbl", "k"} {
		prepared = binary.BigEndian.AppendUint16(prepared, uint16(len(s)))
		prepared = append(prepared, s...)
	}
	prepared = binary.BigEndian.AppendUint16(prepared, uint16(cql.KindInt))
	// <result_metadata>
	prepared = binary.BigEndian.AppendUint32(prepared, uint32(frm.FlagGlobalTableSpec))
	prepared = binary.BigEndian.AppendUint32(prepared, 1)
	prepared = columnSpec(prepared)

	rows := func(flags int, newID []byte, value string) []byte {
		b := binary.BigEndian.AppendUint32(nil, uint32(frm.ResultKindRows))
		b = binary.BigEndian.AppendUint32(b, uint32(flags))
		b = binary.BigEndian.AppendUint32(b, 1)
		if newID != nil {
			b = appendShortBytes(b, newID)
		}
		if flags&frm.FlagNoMetaData == 0 {
			b = columnSpec(b)
		}
		b = binary.BigEndian.AppendUint32(b, 1)
		b = binary.BigEndian.AppendUint32(b, uint32(len(value)))
		return append(b, value...)
	}

	type execute struct {
		preparedID, metadataID []byte
		skipMetadata           bool
	}
	executes := make(chan execute, 8)
	// responses holds the body sent for each EXECUTE, in order.
	responses := [][]byte{
		rows(frm.FlagGlobalTableSpec|frm.FlagMetaDataChanged, secondID, "first"),
		rows(frm.FlagNoMetaData, nil, "second"),
		rows(frm.FlagNoMetaData, nil, "third"),
	}
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		defer close(executes)
		defer peer.Close() // a server error ends the client's wait at once
		for {
			requests, err := readV5Requests(peer)
			if err != nil {
				return
			}
			for _, r := range requests {
				var body []byte
				switch r.op {
				case byte(frm.OpPrepare):
					body = prepared
				case byte(frm.OpExecute):
					id, rest, ok := v5ShortBytes(r.body)
					metaID, rest, ok2 := v5ShortBytes(rest)
					if !ok || !ok2 || len(rest) < 6 {
						t.Errorf("malformed EXECUTE body %x", r.body)
						return
					}
					flags := binary.BigEndian.Uint32(rest[2:]) // after <consistency>
					executes <- execute{preparedID: bytes.Clone(id), metadataID: bytes.Clone(metaID), skipMetadata: flags&frm.FlagSkipMetaData != 0}
					if len(responses) == 0 {
						t.Errorf("unexpected EXECUTE")
						return
					}
					body, responses = responses[0], responses[1:]
				default:
					t.Errorf("unexpected opcode 0x%x", r.op)
					return
				}
				if _, err := peer.Write(testResponse(t, c, r.stream, body)); err != nil {
					return
				}
			}
		}
	}()
	t.Cleanup(func() { peer.Close(); <-serverDone })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, err := Prepare(ctx, s, oneShotStatement(), PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}

	want := []struct {
		metadataID []byte
		skip       bool
		value      string
	}{
		{firstID, true, "first"},
		{secondID, true, "second"},
		{secondID, true, "third"},
	}
	for i, w := range want {
		value, err := run(ctx, s, p)
		if err != nil {
			t.Fatalf("execution %d: %v", i+1, err)
		}
		if value != w.value {
			t.Fatalf("execution %d decoded %q, want %q", i+1, value, w.value)
		}
		got := <-executes
		if !bytes.Equal(got.preparedID, preparedID) {
			t.Fatalf("execution %d sent prepared ID %q, want %q", i+1, got.preparedID, preparedID)
		}
		if !bytes.Equal(got.metadataID, w.metadataID) {
			t.Fatalf("execution %d sent result metadata ID %q, want %q", i+1, got.metadataID, w.metadataID)
		}
		if got.skipMetadata != w.skip {
			t.Fatalf("execution %d SKIP_METADATA=%v, want %v", i+1, got.skipMetadata, w.skip)
		}
	}
}
