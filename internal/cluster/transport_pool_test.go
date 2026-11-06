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

package cluster

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func reserve(t *testing.T, c *transport.Conn) *transport.InternalExclusiveConn {
	t.Helper()
	go c.InternalServe(c.InternalCtx)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	r, err := c.InternalReserveExclusive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.InternalClose(false) })
	return r
}
func writeResponse(t *testing.T, c *transport.Conn, server net.Conn, stream int, body []byte) {
	t.Helper()
	frame := typedTestResponse(t, c.InternalVersion, stream, body)
	if c.InternalVersion < 5 {
		if _, err := server.Write(frame); err != nil {
			t.Error(err)
		}
		return
	}
	for offset := 0; offset < len(frame); {
		end := min(offset+segment.MaxPayloadSize, len(frame))
		part := typedTestChain(t, frame[offset:end])
		packet, err := segment.EncodeSpans(part, len(frame) <= segment.MaxPayloadSize, c.InternalSegCompressor != nil)
		if err != nil {
			t.Error(err)
			return
		}
		if _, err := server.Write(typedTestBytes(t, packet)); err != nil {
			t.Error(err)
			return
		}
		offset = end
	}
}

var _ io.Reader = (*transport.InternalVerifiedStreamUnits)(nil)

func eventPacket(t *testing.T, proto byte, kind string) []byte {
	t.Helper()
	e := protocol.NewTypedEnvelope()
	e.Str(kind, false)
	if kind == "CLIENT_ROUTES_CHANGE" {
		e.Str("UPDATED", false)
		e.U16(0)
		e.U16(0)
	} else {
		e.Str("DOWN", false)
		e.U8(4)
		e.Chain(typedTestChain(t, []byte{127, 0, 0, 2}))
		e.U32(9042)
	}
	body := typedTestBytes(t, codecTestMust(e.Freeze()))
	out := []byte{proto | 0x80, 0, 0xff, 0xff, byte(frm.OpEvent)}
	out = binary.BigEndian.AppendUint32(out, uint32(len(body)))
	return append(out, body...)
}

type headerPause struct {
	entered, release chan struct{}
	once             sync.Once
}

func (o *headerPause) ObserveFrameHeader(context.Context, transport.ObservedFrameHeader) {
	o.once.Do(func() { close(o.entered); <-o.release })
}

type acceptedWriter struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (w *acceptedWriter) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	return w.Conn.Write(b)
}

type tlsCoalescedRecorder struct {
	conn      *tls.Conn
	payload   []byte
	remaining int
	partial   bool
	borrowed  int
	entered   chan struct{}
	once      sync.Once
}

func (w *tlsCoalescedRecorder) SetWriteDeadline(deadline time.Time) error {
	return w.conn.SetWriteDeadline(deadline)
}
func (w *tlsCoalescedRecorder) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	if len(p) > 1024 {
		at := uintptr(unsafe.Pointer(&p[0]))
		start := uintptr(unsafe.Pointer(&w.payload[0]))
		if at < start || at+uintptr(len(p)) > start+uintptr(len(w.payload)) {
			return 0, fmt.Errorf("coalescer gathered borrowed TLS plaintext")
		}
		w.borrowed += len(p)
	}
	if w.partial && len(p) > w.remaining {
		n, err := w.conn.Write(p[:w.remaining])
		w.remaining -= n
		if err != nil {
			return n, err
		}
		return n, io.ErrShortWrite
	}
	n, err := w.conn.Write(p)
	if w.partial {
		w.remaining -= n
	}
	return n, err
}

type fixedHostDialer struct{ conn net.Conn }

func (d fixedHostDialer) DialHost(context.Context, *endpoint.HostInfo) (*endpoint.DialedHost, error) {
	return &endpoint.DialedHost{Conn: d.conn}, nil
}

type wireCounter struct {
	net.Conn
	reads, writes, readBytes, writeBytes, records atomic.Int64
}

func (c *wireCounter) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.reads.Add(1)
	c.readBytes.Add(int64(n))
	return n, err
}
func (c *wireCounter) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.writes.Add(1)
	c.writeBytes.Add(int64(n))
	// Count complete actual TLS application records independently of Write calls.
	for at := 0; at+5 <= n; {
		length := int(binary.BigEndian.Uint16(p[at+3 : at+5]))
		if at+5+length > n {
			break
		}
		if p[at] == 23 {
			c.records.Add(1)
		}
		at += 5 + length
	}
	return n, err
}
func (c *wireCounter) reset() {
	c.reads.Store(0)
	c.writes.Store(0)
	c.readBytes.Store(0)
	c.writeBytes.Store(0)
	c.records.Store(0)
}
func benchTLS(b testing.TB) (*tls.Conn, *tls.Conn, *wireCounter, func()) {
	b.Helper()
	rawClient, rawServer := net.Pipe()
	wire := &wireCounter{Conn: rawClient}
	cert, config := testTLS(b)
	client := tls.Client(wire, config)
	server := tls.Server(rawServer, &tls.Config{Certificates: []tls.Certificate{cert}, SessionTicketsDisabled: true})
	done := make(chan error, 1)
	go func() { done <- server.Handshake() }()
	if err := client.Handshake(); err != nil {
		b.Fatal(err)
	}
	if err := <-done; err != nil {
		b.Fatal(err)
	}
	return client, server, wire, func() { rawClient.Close(); rawServer.Close() }
}
