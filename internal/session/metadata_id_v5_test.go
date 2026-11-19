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
		if len(payload) < 9 {
			return nil, fmt.Errorf("truncated frame header in segment")
		}
		n := int(binary.BigEndian.Uint32(payload[5:9]))
		if len(payload) < 9+n {
			return nil, fmt.Errorf("frame body exceeds segment")
		}
		out = append(out, v5Request{op: payload[4], stream: int(binary.BigEndian.Uint16(payload[2:4])), body: bytes.Clone(payload[9 : 9+n])})
		payload = payload[9+n:]
	}
	return out, nil
}

// v5ShortBytes splits a [short bytes] value off the front of b.
func v5ShortBytes(b []byte) ([]byte, []byte) {
	n := int(binary.BigEndian.Uint16(b))
	return b[2 : 2+n], b[2+n:]
}

func appendShortBytes(b, v []byte) []byte {
	b = binary.BigEndian.AppendUint16(b, uint16(len(v)))
	return append(b, v...)
}

// TestResultMetadataIDV5 drives a prepared statement over a real protocol 5
// connection: a METADATA_CHANGED response's new result-metadata ID must be the
// one the next EXECUTE sends, and the driver must keep skipping metadata with it.
func TestResultMetadataIDV5(t *testing.T) {
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
		return binary.BigEndian.AppendUint16(b, 0x0003) // blob
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
	prepared = binary.BigEndian.AppendUint16(prepared, 0x0009) // int
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
					id, rest := v5ShortBytes(r.body)
					metaID, rest := v5ShortBytes(rest)
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
		var dst []byte
		found, _, err := p.One(ctx, 1, &dst, CompiledOptions{})
		if err != nil || !found {
			t.Fatalf("execution %d: found=%v err=%v", i+1, found, err)
		}
		if string(dst) != w.value {
			t.Fatalf("execution %d decoded %q, want %q", i+1, dst, w.value)
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
