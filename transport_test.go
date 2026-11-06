// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	transporttestkit "github.com/scylladb/gocql/v2/internal/testkit/transport"

	"encoding/binary"
	"io"
	"net"
	"testing"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/segment"

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

func typedTestConn(t testing.TB, proto byte, compressor Compressor) (*driverConn, net.Conn) {
	t.Helper()
	return transporttestkit.NewPair(t, proto, compressor)
}

func typedTestResponse(t testing.TB, proto byte, stream int, body []byte) []byte {
	t.Helper()
	out := []byte{proto | 0x80, 0, byte(stream >> 8), byte(stream), byte(frm.OpResult)}
	out = binary.BigEndian.AppendUint32(out, uint32(len(body)))
	return append(out, body...)
}

func typedTestReadRequest(t testing.TB, c *driverConn, server net.Conn) int {
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
