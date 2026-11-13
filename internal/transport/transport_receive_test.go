// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

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

// A response that arrives after its caller was cancelled must not delay the
// response of another stream on the same connection.
func TestLateResponseAfterCancelDoesNotDelayOthers(t *testing.T) {
	c, server := invPair(t, 4)
	go c.InternalServe(c.InternalCtx)
	for i := 0; i < 20; i++ {
		ctxA, cancelA := context.WithCancel(context.Background())
		aErr := make(chan error, 1)
		go func() {
			page, err := c.InternalExecuteTyped(ctxA, &model.AttemptRequest{Statement: "SELECT a", Consistency: cqltype.One})
			if page != nil {
				page.Owner.Release()
			}
			aErr <- err
		}()
		aStream, err := invReadRequest(server)
		if err != nil {
			t.Fatal(err)
		}
		cancelA()
		if err := <-aErr; err == nil {
			t.Fatal("A was not cancelled")
		}
		bDone := make(chan error, 1)
		go func() {
			page, err := c.InternalExecuteTyped(context.Background(), &model.AttemptRequest{Statement: "SELECT b", Consistency: cqltype.One})
			if err == nil {
				page.Owner.Release()
			}
			bDone <- err
		}()
		bStream, err := invReadRequest(server)
		if err != nil {
			t.Fatal(err)
		}
		// net.Pipe writes complete only when the reader consumed them, so
		// they run apart from the checks below.
		go func() {
			server.Write(invFrame(4, aStream, typedTestRowsBody([]byte("a"))))
			server.Write(invFrame(4, bStream, typedTestRowsBody([]byte("b"))))
		}()
		select {
		case err := <-bDone:
			if err != nil {
				t.Fatalf("B: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("B was delayed by A's late response")
		}
	}
	// A's late response frees its stream; give the reader no more than a second.
	deadline := time.Now().Add(time.Second)
	for c.InternalStreams.InUse() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("streams in use after run: %d", c.InternalStreams.InUse())
		}
		time.Sleep(time.Millisecond)
	}
}
