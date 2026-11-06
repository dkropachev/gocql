/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package execution

import (
	"bytes"
	"context"
	"io"
	"net"
	"time"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/transport"
)

type step04MemoryConn struct {
	source                 *bytes.Reader
	readCalls              int
	directBytes, otherBulk int64
	start, end             uintptr
	closed                 bool
}

func (c *step04MemoryConn) Read(p []byte) (int, error) {
	if c.closed {
		return 0, io.ErrClosedPipe
	}
	c.readCalls++
	if len(p) > 64 {
		address := uintptr(unsafe.Pointer(&p[0]))
		if address >= c.start && address+uintptr(len(p)) <= c.end {
			c.directBytes += int64(len(p))
		} else {
			c.otherBulk += int64(len(p))
		}
	}
	return c.source.Read(p)
}
func (c *step04MemoryConn) Write(p []byte) (int, error) {
	if c.closed {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}
func (c *step04MemoryConn) Close() error { c.closed = true; return nil }
func (c *step04MemoryConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 49152}
}
func (c *step04MemoryConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9042}
}
func (c *step04MemoryConn) SetDeadline(time.Time) error      { return nil }
func (c *step04MemoryConn) SetReadDeadline(time.Time) error  { return nil }
func (c *step04MemoryConn) SetWriteDeadline(time.Time) error { return nil }
func step04MemoryReservation(version byte, compressor protocol.Compressor, source net.Conn) *transport.InternalExclusiveConn {
	ctx, cancel := context.WithCancel(context.Background())
	c := &transport.Conn{InternalVersion: version, InternalCtx: ctx, InternalCancel: cancel, InternalR: &transport.InternalConnReader{InternalConn: source}, InternalStreams: streams.New(), InternalLogger: logging.InternalNopLogger{}, InternalErrorHandler: transport.InternalConnErrorHandlerFn(func(*transport.Conn, error, bool) {}), InternalCompressor: compressor}
	c.InternalW = &transport.InternalDeadlineContextWriter{InternalW: source, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
	if version == 5 && compressor != nil {
		c.InternalSegCompressor = compressor.(segment.Compressor)
	}
	c.InternalInitFramerCache()
	stream, ok := c.InternalStreams.GetStream()
	if !ok || stream != 0 {
		panic("synthetic stream allocation failed")
	}
	c.InternalExclusiveFlag.Store(true)
	r := &transport.InternalExclusiveConn{InternalConn: c, InternalCtx: ctx, InternalComplete: true, InternalStream: stream, InternalDone: make(chan struct{})}
	r.InternalInitStream(transport.StreamLimits{})
	return r
}
