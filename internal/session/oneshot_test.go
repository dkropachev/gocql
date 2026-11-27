// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/cluster"
	frm "github.com/scylladb/gocql/v2/internal/frame"
)

// oneShotRequest is an EXECUTE the fake server received.
type oneShotRequest struct {
	stream      int
	consistency cql.Consistency
	pageSize    int32 // -1 without a page size
	key         int32
}

// oneShotServer answers PREPARE on peer with a statement of one int
// parameter and one blob column, or none for a statement that is not a
// SELECT, and passes each EXECUTE to requests. It returns when peer closes.
func oneShotServer(t testing.TB, peer net.Conn, requests chan<- oneShotRequest) {
	key := cql.NewColumns(cql.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "k", Type: cql.IntType})
	value := cql.NewColumns(cql.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: cql.BlobType})
	prepared := preparedBody([]byte("oneshot"), key, value, 0)
	write := preparedBody([]byte("oneshot-write"), key, cql.NewColumns(), 0)
	go func() {
		defer close(requests)
		for {
			op, stream, body, err := readRequest(peer)
			if err != nil {
				return
			}
			if op == byte(frm.OpPrepare) {
				answer := prepared
				if len(body) > 4 && !bytes.HasPrefix(body[4:], []byte("SELECT")) {
					answer = write
				}
				if _, err := peer.Write(typedTestResponse(t, 4, stream, answer)); err != nil {
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
	r.consistency = cql.Consistency(binary.BigEndian.Uint16(b))
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
	return binding.Statement[int32, []byte]{Kind: cql.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl WHERE k = ?", Params: cql.OneParam(cql.Int32Codec), Rows: cql.RowInto(func(*[]byte) {}, cql.Column("value", cql.ByteSliceCodec, func(dst *[]byte) *[]byte { return dst }))}
}

// oneShotWriteStatement is a write of one int parameter and no result
// columns, which oneShotServer prepares without result metadata, for Exec.
func oneShotWriteStatement() binding.Statement[int32, cql.EmptyRow] {
	return binding.Statement[int32, cql.EmptyRow]{Kind: cql.OrdinaryStatement, CQL: "UPDATE ks.tbl SET value = 0x00 WHERE k = ?", Params: cql.OneParam(cql.Int32Codec), Rows: cql.Row[cql.EmptyRow]()}
}

// oneShotPrepareWrite prepares oneShotWriteStatement on a oneShotServer.
func oneShotPrepareWrite(t testing.TB, s *cluster.Core, peer net.Conn, requests chan oneShotRequest) *Prepared[int32, cql.EmptyRow] {
	t.Helper()
	oneShotServer(t, peer, requests)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, err := Prepare(ctx, s, oneShotWriteStatement(), PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return p
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
