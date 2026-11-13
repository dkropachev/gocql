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

package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/wire"
	lz4 "github.com/scylladb/gocql/v2/internal/wirecompress"
)

type memoryConn struct {
	source                 *bytes.Reader
	readCalls              int
	directBytes, otherBulk int64
	start, end             uintptr
	closed                 bool
}

func (c *memoryConn) Read(p []byte) (int, error) {
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
func (c *memoryConn) Write(p []byte) (int, error) {
	if c.closed {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}
func (c *memoryConn) Close() error { c.closed = true; return nil }
func (c *memoryConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 49152}
}
func (c *memoryConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9042}
}
func (c *memoryConn) SetDeadline(time.Time) error      { return nil }
func (c *memoryConn) SetReadDeadline(time.Time) error  { return nil }
func (c *memoryConn) SetWriteDeadline(time.Time) error { return nil }
func memoryReservation(version byte, compressor protocol.Compressor, source net.Conn) *InternalExclusiveConn {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Conn{InternalVersion: version, InternalCtx: ctx, InternalCancel: cancel, InternalR: &InternalConnReader{InternalConn: source}, InternalStreams: streams.New(), InternalLogger: logging.Nop(), InternalErrorHandler: InternalConnErrorHandlerFn(func(*Conn, error, bool) {}), InternalCompressor: compressor}
	c.InternalW = &InternalDeadlineContextWriter{InternalW: source, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
	if version == 5 && compressor != nil {
		c.InternalSegCompressor = compressor.(segment.Compressor)
	}
	c.InternalInitFramerCache()
	stream, ok := c.InternalStreams.GetStream()
	if !ok || stream != 0 {
		panic("synthetic stream allocation failed")
	}
	c.InternalSetExclusive(true)
	r := &InternalExclusiveConn{InternalConn: c, InternalCtx: ctx, InternalComplete: true, InternalStream: stream, InternalDone: make(chan struct{})}
	r.InternalInitStream(StreamLimits{})
	return r
}
func encodedResponse(version byte, compressed bool, payload []byte) []byte {
	body := typedTestRowsBody(payload)
	frame := []byte{version | 0x80, 0, 0, 0, 8}
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(body)))
	frame = append(frame, body...)
	if version < 5 {
		return frame
	}
	var out []byte
	for at := 0; at < len(frame); {
		end := min(at+segment.MaxPayloadSize, len(frame))
		chain, _ := wire.NewChain(wire.Borrow(frame[at:end]))
		packet, err := segment.EncodeSpans(chain, len(frame) <= segment.MaxPayloadSize, compressed)
		if err != nil {
			panic(err)
		}
		start := len(out)
		out = append(out, make([]byte, packet.Len())...)
		packet.CopyTo(out[start:])
		at = end
	}
	return out
}
func FuzzV2ExclusiveIncrementalFraming(f *testing.F) {
	f.Add(byte(4), encodedResponse(4, false, []byte("value")))
	f.Add(byte(5), encodedResponse(5, false, []byte("value")))
	f.Add(byte(5), []byte{0, 0, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, version byte, input []byte) {
		if len(input) > 8192 {
			t.Skip()
		}
		version = 3 + version%3
		source := &memoryConn{source: bytes.NewReader(input)}
		r := memoryReservation(version, nil, source)
		defer r.InternalClose(false)
		r.InternalLimits.MaxMetadataBytes = 2048
		r.InternalMetadataBudget.SetLimit(2048)
		response, err := r.InternalExchange(context.Background(), &model.AttemptRequest{Statement: "SELECT", Consistency: cqltype.One})
		if err != nil {
			if response != nil {
				response.InternalReleaseMetadata()
			}
			return
		}
		defer response.InternalReleaseMetadata()
		if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
			return
		}
		plan, err := codec.StreamRowInto(func(*codec.EmptyRecord) {}).Compile(response.InternalColumns)
		if err != nil {
			return
		}
		var dst codec.EmptyRecord
		for i := 0; i < 16; i++ {
			ok, err := InternalDecodeStreamRow(response, plan, &dst)
			if err != nil || !ok {
				return
			}
		}
	})
}

type testWireCounter struct {
	net.Conn
	reads, writes, readBytes, writeBytes, records atomic.Int64
}

func (c *testWireCounter) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.reads.Add(1)
	c.readBytes.Add(int64(n))
	return n, err
}
func (c *testWireCounter) Write(p []byte) (int, error) {
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
func (c *testWireCounter) reset() {
	c.reads.Store(0)
	c.writes.Store(0)
	c.readBytes.Store(0)
	c.writeBytes.Store(0)
	c.records.Store(0)
}
func benchTLS(b testing.TB) (*tls.Conn, *tls.Conn, *testWireCounter, func()) {
	b.Helper()
	rawClient, rawServer := net.Pipe()
	wire := &testWireCounter{Conn: rawClient}
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

type plaintextCounter struct {
	net.Conn
	reads, outputBytes int64
	start, end         uintptr
}

func (c *plaintextCounter) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.reads++
	if n > 0 {
		address := uintptr(unsafe.Pointer(&p[0]))
		if address >= c.start && address+uintptr(n) <= c.end {
			c.outputBytes += int64(n)
		} else if n > 256 {
			// Headers and metadata may land in scratch: nativeStreamReadAhead
			// speculates small reads into a window that starts at 256 bytes.
			// A larger read outside the destination is a gathered value.
			return n, fmt.Errorf("driver gathered TLS plaintext instead of final output")
		}
	}
	return n, err
}

func BenchmarkStreamTransport(b *testing.B) {
	b.Run("Exclusive", func(b *testing.B) {
		for _, mode := range []struct {
			name       string
			version    byte
			compressor protocol.Compressor
		}{{"raw4", 4, nil}, {"raw5", 5, nil}, {"lz4v5", 5, lz4.LZ4Compressor{}}} {
			for _, size := range []int{1024, 400 << 10, 8 << 20} {
				b.Run(fmt.Sprintf("%s/%d", mode.name, size), func(b *testing.B) {
					payload := bytes.Repeat([]byte("stream-payload"), size/14+1)[:size]
					encoded := encodedResponse(mode.version, mode.compressor != nil, payload)
					source := &memoryConn{source: bytes.NewReader(encoded)}
					r := memoryReservation(mode.version, mode.compressor, source)
					defer r.InternalClose(true)
					dst := make([]byte, 0, size)
					request := &model.AttemptRequest{Statement: "SELECT value", Consistency: cqltype.One}
					var plan codec.StreamRowPlan[[]byte]
					run := func() {
						source.source.Reset(encoded)
						source.closed = false
						source.readCalls = 0
						source.directBytes = 0
						source.otherBulk = 0
						if r.InternalUnits != nil {
							r.InternalUnits.InternalData = nil
							r.InternalUnits.offset = 0
							r.InternalUnits.frameActive = false
							r.InternalUnits.started = false
							r.InternalUnits.InternalCopies = 0
						}
						response, err := r.InternalExchange(context.Background(), request)
						if err != nil {
							b.Fatal(err)
						}
						if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
							b.Fatal(err)
						}
						if plan == nil {
							plan, err = codec.StreamOneColumn(codec.StreamBlob).Compile(response.InternalColumns)
							if err != nil {
								b.Fatal(err)
							}
						}
						if ok, err := InternalDecodeStreamRow(response, plan, &dst); !ok || err != nil {
							b.Fatal(ok, err)
						}
						response.InternalReleaseMetadata()
					}
					run()
					b.SetBytes(int64(size))
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						run()
					}
					b.StopTimer()
					b.ReportMetric(float64(source.readCalls), "reads/op")
					if r.InternalUnits != nil {
						b.ReportMetric(float64(r.InternalUnits.InternalUnits.StorageBytes()), "transform-B")
						b.ReportMetric(float64(r.InternalUnits.InternalCopies), "unit-copy-B/op")
					}
				})
			}
		}
	})
	b.Run("RowsScaling", func(b *testing.B) {
		for _, count := range []int{1, 32, 1024} {
			b.Run(fmt.Sprint(count), func(b *testing.B) {
				body := typedTestRowsBody([]byte{42})
				binary.BigEndian.PutUint32(body[len(body)-9:], uint32(count))
				cell := []byte{0, 0, 0, 1, 42}
				for i := 1; i < count; i++ {
					body = append(body, cell...)
				}
				encoded := []byte{0x84, 0, 0, 0, 8}
				encoded = binary.BigEndian.AppendUint32(encoded, uint32(len(body)))
				encoded = append(encoded, body...)
				source := &memoryConn{source: bytes.NewReader(encoded)}
				r := memoryReservation(4, nil, source)
				defer r.InternalClose(true)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var plan codec.StreamRowPlan[[]byte]
				dst := make([]byte, 0, 1)
				run := func() {
					source.source.Reset(encoded)
					response, err := r.InternalExchange(ctx, &model.AttemptRequest{Statement: "SELECT value", Consistency: cqltype.One})
					if err != nil {
						b.Fatal(err)
					}
					if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
						b.Fatal(err)
					}
					if plan == nil {
						plan, err = codec.StreamOneColumn(codec.StreamBlob).Compile(response.InternalColumns)
						if err != nil {
							b.Fatal(err)
						}
					}
					n := 0
					for {
						ok, err := InternalDecodeStreamRow(response, plan, &dst)
						if err != nil {
							b.Fatal(err)
						}
						if !ok {
							break
						}
						n++
					}
					if n != count {
						b.Fatal(n, count)
					}
					response.InternalReleaseMetadata()
				}
				run()
				b.ReportAllocs()
				b.ReportMetric(float64(count), "rows/op")
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					run()
				}
			})
		}
	})
	b.Run("TLSReceiveAmplification", func(b *testing.B) {
		for _, size := range []int{4096, 400 << 10} {
			b.Run(fmt.Sprint(size), func(b *testing.B) {
				client, server, wireCounter, cleanup := benchTLS(b)
				defer cleanup()
				encoded := encodedResponse(4, false, bytes.Repeat([]byte("z"), size))
				plain := &plaintextCounter{Conn: client}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				c := &Conn{InternalVersion: 4, InternalCtx: ctx, InternalCancel: cancel, InternalR: &InternalConnReader{InternalConn: plain}, InternalStreams: streams.New(), InternalLogger: logging.Nop(), InternalErrorHandler: InternalConnErrorHandlerFn(func(*Conn, error, bool) {})}
				c.InternalW = &InternalDeadlineContextWriter{InternalW: client, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
				c.InternalInitFramerCache()
				stream, _ := c.InternalStreams.GetStream()
				c.InternalSetExclusive(true)
				r := &InternalExclusiveConn{InternalConn: c, InternalCtx: ctx, InternalStream: stream, InternalComplete: true, InternalDone: make(chan struct{})}
				r.InternalInitStream(StreamLimits{})
				defer r.InternalClose(true)
				warm := make(chan struct{})
				done := make(chan error, 1)
				go func() {
					var header [9]byte
					for i := 0; i <= b.N; i++ {
						if _, err := io.ReadFull(server, header[:]); err != nil {
							done <- err
							return
						}
						if _, err := io.CopyN(io.Discard, server, int64(binary.BigEndian.Uint32(header[5:]))); err != nil {
							done <- err
							return
						}
						if _, err := server.Write(encoded); err != nil {
							done <- err
							return
						}
						if i == 0 {
							close(warm)
						}
					}
					done <- nil
				}()
				dst := make([]byte, 0, size)
				plain.start = uintptr(unsafe.Pointer(&dst[:cap(dst)][0]))
				plain.end = plain.start + uintptr(size)
				var plan codec.StreamRowPlan[[]byte]
				run := func() {
					response, err := r.InternalExchange(ctx, &model.AttemptRequest{Statement: "SELECT value", Consistency: cqltype.One})
					if err != nil {
						b.Fatal(err)
					}
					if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
						b.Fatal(err)
					}
					if plan == nil {
						plan, err = codec.StreamOneColumn(codec.StreamBlob).Compile(response.InternalColumns)
						if err != nil {
							b.Fatal(err)
						}
					}
					if ok, err := InternalDecodeStreamRow(response, plan, &dst); !ok || err != nil {
						b.Fatal(ok, err)
					}
					response.InternalReleaseMetadata()
				}
				run()
				<-warm
				wireCounter.reset()
				plain.reads = 0
				plain.outputBytes = 0
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					run()
				}
				b.StopTimer()
				if err := <-done; err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(wireCounter.reads.Load())/float64(b.N), "net-reads/op")
				b.ReportMetric(float64(wireCounter.readBytes.Load())/float64(b.N), "tls-wire-read-B/op")
				b.ReportMetric(float64(plain.reads)/float64(b.N), "plaintext-reads/op")
				b.ReportMetric(float64(plain.outputBytes)/float64(b.N), "tls-output-handoff-B/op")
				b.ReportMetric(0, "driver-gather-B/op")
			})
		}
	})
}
