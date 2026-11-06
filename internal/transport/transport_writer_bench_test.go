// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"bytes"
	"context"
	"crypto/tls"

	"fmt"

	"io"

	"github.com/scylladb/gocql/v2/internal/wire"

	"net"

	"testing"
	"time"
)

// Uses real loopback sockets and the actual coalescer; no wrapper hides writev.
// OS syscall counts are collected in separate strace runs of the same cases.
func BenchmarkStep15SocketWrites(b *testing.B) {
	for _, tlsOn := range []bool{false, true} {
		for _, count := range []int{8, 1024, 4096} {
			for _, mixed := range []bool{false, true} {
				b.Run(fmt.Sprintf("tls%t/spans%d/mixed%t", tlsOn, count, mixed), func(b *testing.B) {
					listener, e := net.Listen("tcp", "127.0.0.1:0")
					if e != nil {
						b.Fatal(e)
					}
					defer listener.Close()
					accepted := make(chan net.Conn, 1)
					go func() {
						conn, e := listener.Accept()
						if e == nil {
							accepted <- conn
						}
					}()
					client, e := net.Dial("tcp", listener.Addr().String())
					if e != nil {
						b.Fatal(e)
					}
					server := <-accepted
					defer client.Close()
					defer server.Close()
					if tlsOn {
						cert, conf := step04TLS(b)
						a := tls.Client(client, conf)
						z := tls.Server(server, &tls.Config{Certificates: []tls.Certificate{cert}})
						done := make(chan error, 1)
						go func() { done <- z.Handshake() }()
						if e = a.Handshake(); e != nil {
							b.Fatal(e)
						}
						if e = <-done; e != nil {
							b.Fatal(e)
						}
						client, server = a, z
					}
					spans := make([]wire.Span, 0, count+1)
					for i := 0; i < count; i++ {
						spans = append(spans, wire.Borrow([]byte{byte(i)}))
					}
					if mixed {
						spans = append(spans, wire.Borrow(make([]byte, 500<<10)))
					}
					chain := codecTestMust(wire.NewChain(spans...))
					quit := make(chan struct{})
					writer := InternalNewWriteCoalescer(client, 0, time.Microsecond, quit)
					defer close(quit)
					done := make(chan error, 1)
					go func() { _, e := io.CopyN(io.Discard, server, int64(b.N)*chain.Len()); done <- e }()
					b.SetBytes(chain.Len())
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, e := writer.InternalWriteSpans(context.Background(), chain); e != nil {
							b.Fatal(e)
						}
					}
					b.StopTimer()
					if e := <-done; e != nil {
						b.Fatal(e)
					}
					b.ReportMetric(float64(len(spans)), "spans/op")
				})
			}
		}
	}
}

func BenchmarkV2TLSWriteAmplification(b *testing.B) {
	for _, size := range []int{64 << 10, 400 << 10} {
		for _, fragments := range []int{1, 16, 1024} {
			b.Run(fmt.Sprintf("bytes=%d/fragments=%d", size, fragments), func(b *testing.B) {
				client, server, wireCounter, cleanup := step04BenchTLS(b)
				defer cleanup()
				payload := bytes.Repeat([]byte("x"), size)
				parts := make([][]byte, 0, fragments)
				spans := make([]wire.Span, 0, fragments)
				for offset := 0; offset < size; {
					end := min(offset+max(1, size/fragments), size)
					part := payload[offset:end]
					parts = append(parts, part)
					spans = append(spans, wire.Borrow(part))
					offset = end
				}
				chain, err := wire.NewChain(spans...)
				if err != nil {
					b.Fatal(err)
				}
				recorder := &typedTLSRecorder{conn: client, expected: parts}
				writer := &InternalDeadlineContextWriter{InternalW: recorder, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
				warm := make(chan struct{})
				done := make(chan error, 1)
				go func() {
					_, err := io.CopyN(io.Discard, server, int64(size))
					if err != nil {
						done <- err
						close(warm)
						return
					}
					close(warm)
					_, err = io.CopyN(io.Discard, server, int64(size)*int64(b.N))
					done <- err
				}()
				run := func() {
					recorder.next = 0
					if _, err := writer.InternalWriteSpans(context.Background(), chain); err != nil {
						b.Fatal(err)
					}
					if recorder.next != len(parts) {
						b.Fatal("span accounting")
					}
				}
				run()
				<-warm
				wireCounter.reset()
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
				b.ReportMetric(float64(wireCounter.writes.Load())/float64(b.N), "tls-writes/op")
				b.ReportMetric(float64(wireCounter.records.Load())/float64(b.N), "tls-records/op")
				b.ReportMetric(float64(wireCounter.writeBytes.Load())/float64(b.N), "tls-wire-B/op")
				b.ReportMetric(float64(len(parts)), "plaintext-spans/op")
				b.ReportMetric(0, "driver-gather-B/op")
			})
		}
	}
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
