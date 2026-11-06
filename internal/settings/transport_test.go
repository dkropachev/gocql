// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/segment"
	transporttestkit "github.com/scylladb/gocql/v2/internal/testkit/transport"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func typedTestChain(t *testing.T, parts ...[]byte) wire.Chain {
	t.Helper()
	spans := make([]wire.Span, len(parts))
	for i, p := range parts {
		spans[i] = wire.Borrow(p)
	}
	c, err := wire.NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func typedTestBytes(t *testing.T, c wire.Chain) []byte {
	t.Helper()
	p := make([]byte, c.Len())
	if _, err := c.CopyTo(p); err != nil {
		t.Fatal(err)
	}
	return p
}
func typedTestConn(t testing.TB, proto byte, compressor protocol.Compressor) (*transport.Conn, net.Conn) {
	t.Helper()
	return transporttestkit.NewPair(t, proto, compressor)
}

func typedTestResponse(t testing.TB, proto byte, stream int, body []byte) []byte {
	t.Helper()
	out := []byte{proto | 0x80, 0, byte(stream >> 8), byte(stream), byte(frm.OpResult)}
	out = binary.BigEndian.AppendUint32(out, uint32(len(body)))
	return append(out, body...)
}
func typedTestRowsBody(value []byte) []byte {
	out := binary.BigEndian.AppendUint32(nil, 2)
	out = binary.BigEndian.AppendUint32(out, 1) // global table
	out = binary.BigEndian.AppendUint32(out, 1)
	for _, s := range []string{"ks", "tbl", "value"} {
		out = binary.BigEndian.AppendUint16(out, uint16(len(s)))
		out = append(out, s...)
	}
	out = binary.BigEndian.AppendUint16(out, uint16(implCqltype.TypeBlob))
	out = binary.BigEndian.AppendUint32(out, 1)
	out = binary.BigEndian.AppendUint32(out, uint32(len(value)))
	return append(out, value...)
}
func typedTestReadRequest(t testing.TB, c *transport.Conn, server net.Conn) int {
	t.Helper()
	if c.InternalVersion == 5 {
		h, err := segment.ReadHeader(server, c.InternalSegCompressor != nil)
		if err != nil {
			t.Error(err)
			return 0
		}
		owner, err := segment.ReadOwnedPayload(server, h, c.InternalSegCompressor)
		if err != nil {
			t.Error(err)
			return 0
		}
		defer owner.Release()
		cursor := wire.NewCursor(owner.Chain())
		cursor.Skip(2)
		stream, _ := cursor.Uint16()
		return int(stream)
	}
	var header [9]byte
	if _, err := io.ReadFull(server, header[:]); err != nil {
		t.Error(err)
		return 0
	}
	if _, err := io.CopyN(io.Discard, server, int64(binary.BigEndian.Uint32(header[5:]))); err != nil {
		t.Error(err)
	}
	return int(binary.BigEndian.Uint16(header[2:4]))
}

type typedShortWriter struct {
	limit int
	calls int
	mu    sync.Mutex
}

func (w *typedShortWriter) SetWriteDeadline(time.Time) error { return nil }
func (w *typedShortWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	n := min(w.limit, len(p))
	w.limit -= n
	return n, nil
}

type typedBlockedWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	spans   [][]byte
}

func (w *typedBlockedWriter) SetWriteDeadline(time.Time) error { return nil }
func (w *typedBlockedWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered); <-w.release })
	w.spans = append(w.spans, p)
	return len(p), nil
}

type typedTLSRecorder struct {
	conn     *tls.Conn
	expected [][]byte
	next     int
}

func (w *typedTLSRecorder) SetWriteDeadline(t time.Time) error { return w.conn.SetWriteDeadline(t) }
func (w *typedTLSRecorder) Write(p []byte) (int, error) {
	if w.next >= len(w.expected) || len(p) != len(w.expected[w.next]) || &p[0] != &w.expected[w.next][0] {
		return 0, fmt.Errorf("TLS plaintext was gathered before tls.Conn.Write")
	}
	w.next++
	return w.conn.Write(p)
}
