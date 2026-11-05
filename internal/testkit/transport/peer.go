// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package transport provides real connection peers and focused transport faults.
package transport

import (
	"bufio"
	"context"
	"net"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"

	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/streams"
	runtime "github.com/scylladb/gocql/v2/internal/transport"
)

// NewPair creates the actual multiplexed transport over a private socket pair.
// The caller drives the protocol peer, including short reads and failures.
func NewPair(t testing.TB, proto byte, compressor protocol.Compressor) (*runtime.Conn, net.Conn) {
	t.Helper()
	server, client := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	c := &runtime.Conn{InternalVersion: proto, InternalCtx: ctx, InternalCancel: cancel, InternalR: &runtime.InternalConnReader{InternalConn: client}, InternalCalls: runtime.InternalNewCallTable(), InternalStreams: streams.New(), InternalLogger: logging.InternalNopLogger{}, InternalCompressor: compressor, InternalErrorHandler: runtime.InternalConnErrorHandlerFn(func(*runtime.Conn, error, bool) {})}
	c.InternalW = &runtime.InternalDeadlineContextWriter{InternalW: client, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
	if proto == 5 && compressor != nil {
		c.InternalSegCompressor = compressor.(segment.Compressor)
	}
	if c.InternalCfg == nil {
		c.InternalCfg = new(runtime.ConnConfig)
	}
	c.InternalCfg.InternalReceiveCache = new(wire.ReceiveCache)
	t.Cleanup(c.InternalCfg.InternalReceiveCache.Close)
	c.InternalInitFramerCache()
	c.InternalCloseOnCancel()
	t.Cleanup(func() { c.Close(); server.Close() })
	return c, server
}

// NewDialedConn is NewPair over a TCP connection to addr with the production
// 8 KiB shared reader, so socket batching and read-ahead match a dialed
// connection. The peer at addr drives the protocol.
func NewDialedConn(t testing.TB, proto byte, addr string) *runtime.Conn {
	t.Helper()
	client, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &runtime.Conn{InternalVersion: proto, InternalCtx: ctx, InternalCancel: cancel, InternalR: &runtime.InternalConnReader{InternalConn: client, InternalR: bufio.NewReaderSize(client, 8<<10)}, InternalCalls: runtime.InternalNewCallTable(), InternalStreams: streams.New(), InternalLogger: logging.InternalNopLogger{}, InternalErrorHandler: runtime.InternalConnErrorHandlerFn(func(*runtime.Conn, error, bool) {})}
	c.InternalW = &runtime.InternalDeadlineContextWriter{InternalW: client, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
	c.InternalCfg = new(runtime.ConnConfig)
	c.InternalCfg.InternalReceiveCache = new(wire.ReceiveCache)
	t.Cleanup(c.InternalCfg.InternalReceiveCache.Close)
	c.InternalInitFramerCache()
	c.InternalCloseOnCancel()
	t.Cleanup(func() { c.Close() })
	return c
}

// Fail closes the real connection through its normal poison/retirement path.
func Fail(c *runtime.Conn, err error) { c.InternalCloseWithError(err) }
