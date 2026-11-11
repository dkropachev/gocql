// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func invPair(t testing.TB, proto byte) (*Conn, net.Conn) {
	t.Helper()
	server, client := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	c := &Conn{InternalVersion: proto, InternalCtx: ctx, InternalCancel: cancel, InternalR: &InternalConnReader{InternalConn: client}, InternalCalls: InternalNewCallTable(), InternalStreams: streams.New(), InternalLogger: logging.Nop(), InternalErrorHandler: InternalConnErrorHandlerFn(func(*Conn, error, bool) {})}
	c.InternalW = &InternalDeadlineContextWriter{InternalW: client, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
	c.InternalCfg = new(ConnConfig)
	c.InternalCfg.InternalReceiveCache = new(wire.ReceiveCache)
	c.InternalInitFramerCache()
	c.InternalCloseOnCancel()
	t.Cleanup(func() { c.Close(); server.Close(); c.InternalCfg.InternalReceiveCache.Close() })
	return c, server
}

func invFrame(proto byte, stream int, body []byte) []byte {
	out := []byte{proto | 0x80, 0, byte(stream >> 8), byte(stream), byte(frm.OpResult)}
	out = binary.BigEndian.AppendUint32(out, uint32(len(body)))
	return append(out, body...)
}

// v4 only: read one request frame, return its stream.
func invReadRequest(server net.Conn) (int, error) {
	var header [9]byte
	if _, err := io.ReadFull(server, header[:]); err != nil {
		return 0, err
	}
	if _, err := io.CopyN(io.Discard, server, int64(binary.BigEndian.Uint32(header[5:]))); err != nil {
		return 0, err
	}
	return int(binary.BigEndian.Uint16(header[2:4])), nil
}

// A parse failure after the caller's destination was claimed must leave the
// destination empty, not pointing at the released (pooled) lease.
func TestReceiveTypedCallParseFailureClearsDest(t *testing.T) {
	c, server := invPair(t, 4)
	go c.InternalServe(c.InternalCtx)
	dest := new(InternalResponsePage)
	done := make(chan error, 1)
	go func() {
		page, err := c.InternalExecuteTypedInto(context.Background(), &model.AttemptRequest{Statement: "SELECT value", Consistency: cqltype.One}, dest)
		if page != nil {
			t.Error("page returned for failed parse")
		}
		done <- err
	}()
	stream, err := invReadRequest(server)
	if err != nil {
		t.Fatal(err)
	}
	// Result kind 99: parsing fails after the destination is populated.
	body := binary.BigEndian.AppendUint32(nil, 99)
	if _, err := server.Write(invFrame(4, stream, body)); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatal("expected a parse error")
	}
	if dest.Owner != nil {
		t.Fatalf("dest.Owner = %p after parse failure, want nil", dest.Owner)
	}
	if dest.Rows.Len() != 0 || dest.Kind != 0 {
		t.Fatalf("dest not cleared: kind=%d", dest.Kind)
	}
}
