// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
)

// oneShotRequest is an EXECUTE the fake server received.
type oneShotRequest struct {
	stream      int
	consistency cqltype.Consistency
	pageSize    int32 // -1 without a page size
	key         int32
}

// oneShotServer answers PREPARE on peer with a statement of one int
// parameter and one blob column, and passes each EXECUTE to requests. It
// returns when peer closes.
func oneShotServer(t testing.TB, peer net.Conn, requests chan<- oneShotRequest) {
	key := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "k", Type: cqltype.IntType})
	value := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: cqltype.BlobType})
	prepared := preparedBody([]byte("oneshot"), key, value, 0)
	go func() {
		defer close(requests)
		for {
			op, stream, body, err := readRequest(peer)
			if err != nil {
				return
			}
			if op == byte(frm.OpPrepare) {
				if _, err := peer.Write(typedTestResponse(t, 4, stream, prepared)); err != nil {
					return
				}
				continue
			}
			requests <- parseOneShotExecute(stream, body)
		}
	}()
}

// parseOneShotExecute decodes a v4 EXECUTE body of one int value.
func parseOneShotExecute(stream int, b []byte) oneShotRequest {
	r := oneShotRequest{stream: stream, pageSize: -1}
	b = b[2+int(binary.BigEndian.Uint16(b)):] // statement id
	r.consistency = cqltype.Consistency(binary.BigEndian.Uint16(b))
	flags := b[2]
	b = b[3:]
	if flags&0x01 != 0 {
		b = b[2:] // value count
		n := int(int32(binary.BigEndian.Uint32(b)))
		if n == 4 {
			r.key = int32(binary.BigEndian.Uint32(b[4:]))
		}
		b = b[4+max(n, 0):]
	}
	if flags&0x04 != 0 {
		r.pageSize = int32(binary.BigEndian.Uint32(b))
	}
	return r
}

func oneShotStatement() binding.Statement[int32, []byte] {
	return binding.Statement[int32, []byte]{Kind: cqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl WHERE k = ?", Params: codec.OneParam(codec.Int32), Rows: codec.RowInto(func(*[]byte) {}, codec.Column("value", codec.OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
}

func oneShotPrepare(t testing.TB, s *cluster.Core, peer net.Conn, requests chan oneShotRequest) *Prepared[int32, []byte] {
	t.Helper()
	oneShotServer(t, peer, requests)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, err := Prepare(ctx, s, oneShotStatement(), PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func waitIdle(t *testing.T, p *Prepared[int32, []byte], want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for preparedPoolIdle(p) != want {
		if time.Now().After(deadline) {
			t.Fatalf("idle queries %d, want %d", preparedPoolIdle(p), want)
		}
		time.Sleep(time.Millisecond)
	}
}
