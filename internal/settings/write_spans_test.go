// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestWriteSpans(t *testing.T) {
	t.Run("Coalescer", func(t *testing.T) {
		t.Run("PartialCompletion", func(t *testing.T) {
			for _, limit := range []int{0, 3, 6, 8, 10} {
				sink := &typedShortWriter{limit: limit}
				coalescer := &transport.InternalWriteCoalescer{InternalC: sink}
				a, b := make(chan transport.InternalWriteResult, 1), make(chan transport.InternalWriteResult, 1)
				coalescer.InternalFlushSizes([]chan<- transport.InternalWriteResult{a, b}, net.Buffers{[]byte("abc"), []byte("def"), []byte("ghi"), []byte("j")}, []int64{6, 4})
				ar, br := <-a, <-b
				if ar.InternalN != min(limit, 6) || br.InternalN != max(0, limit-6) {
					t.Fatalf("limit %d wrong completion: %v %v", limit, ar, br)
				}
				if limit < 6 && !errors.Is(ar.InternalErr, io.ErrShortWrite) {
					t.Fatal("missing first short write")
				}
				if limit < 10 && !errors.Is(br.InternalErr, io.ErrShortWrite) {
					t.Fatal("missing second short write")
				}
			}
		})

		t.Run("ShutdownCompletesAccepted", func(t *testing.T) {
			quit := make(chan struct{})
			sink := &typedShortWriter{limit: 100}
			w := transport.InternalNewWriteCoalescer(sink, 0, time.Hour, quit)
			admitted := make(chan struct{})
			w.InternalTestEnqueuedHook = func() { close(admitted) }
			out := make(chan error, 1)
			go func() {
				_, err := w.InternalWriteSpans(context.Background(), typedTestChain(t, []byte("one"), []byte("two")))
				out <- err
			}()
			<-admitted
			close(quit)
			select {
			case err := <-out:
				if err == nil {
					t.Fatal("shutdown unexpectedly succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("accepted request never completed")
			}
		})
	})

	t.Run("DeadlineWriter", func(t *testing.T) {
		t.Run("CancellationJoinsAcceptedSpans", func(t *testing.T) {
			sink := &typedBlockedWriter{entered: make(chan struct{}), release: make(chan struct{})}
			w := &transport.InternalDeadlineContextWriter{InternalW: sink, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
			a, b := []byte("first"), []byte("second")
			input := typedTestChain(t, a, b)
			ctx, cancel := context.WithCancel(context.Background())
			out := make(chan error, 1)
			go func() { _, err := w.InternalWriteSpans(ctx, input); out <- err }()
			<-sink.entered
			cancel()
			select {
			case <-out:
				t.Fatal("cancellation returned while accepted writer accesses input")
			default:
			}
			close(sink.release)
			if err := <-out; err != nil {
				t.Fatal(err)
			}
			if len(sink.spans) != 2 || &sink.spans[0][0] != &a[0] || &sink.spans[1][0] != &b[0] {
				t.Fatal("writer gathered spans")
			}
		})
	})

	t.Run("TLS", func(t *testing.T) {
		t.Run("OriginalPlaintextSpans", func(t *testing.T) {
			key, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			template := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{"span.test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
			der, err := x509.CreateCertificate(cryptorand.Reader, template, template, &key.PublicKey, key)
			if err != nil {
				t.Fatal(err)
			}
			certificate, err := x509.ParseCertificate(der)
			if err != nil {
				t.Fatal(err)
			}
			roots := x509.NewCertPool()
			roots.AddCert(certificate)
			rawClient, rawServer := net.Pipe()
			defer rawClient.Close()
			defer rawServer.Close()
			server := tls.Server(rawServer, &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS13})
			client := tls.Client(rawClient, &tls.Config{RootCAs: roots, ServerName: "span.test", MinVersion: tls.VersionTLS13})
			parts := [][]byte{[]byte("header"), bytes.Repeat([]byte("borrowed plaintext "), 30000), []byte("trailer")}
			var expected []byte
			for _, part := range parts {
				expected = append(expected, part...)
			}
			readResult := make(chan error, 1)
			go func() {
				received := make([]byte, len(expected))
				_, err := io.ReadFull(server, received)
				if err == nil && !bytes.Equal(received, expected) {
					err = fmt.Errorf("TLS reordered payload")
				}
				readResult <- err
			}()
			recorder := &typedTLSRecorder{conn: client, expected: parts}
			writer := &transport.InternalDeadlineContextWriter{InternalW: recorder, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
			writer.InternalSetWriteTimeout(5 * time.Second)
			if _, err := writer.InternalWriteSpans(context.Background(), typedTestChain(t, parts...)); err != nil {
				t.Fatal(err)
			}
			if err := <-readResult; err != nil {
				t.Fatal(err)
			}
			if recorder.next != len(parts) {
				t.Fatal("TLS missed an input span")
			}
		})
	})

	t.Run("Writev", func(t *testing.T) {
		t.Run("BeyondPlatformVectorLimit", func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			peer := make(chan net.Conn, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				peer <- conn
			}()
			client, err := net.Dial("tcp", listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			server := <-peer
			defer server.Close()
			var spans []wire.Span
			var expected []byte
			for i := 0; i < 2300; i++ {
				part := []byte{byte(i), byte(i >> 8), byte(i >> 16)}
				spans = append(spans, wire.Borrow(part))
				expected = append(expected, part...)
			}
			chain, _ := wire.NewChain(spans...)
			writer := &transport.InternalDeadlineContextWriter{InternalW: client, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
			done := make(chan error, 1)
			go func() {
				n, err := writer.InternalWriteSpans(context.Background(), chain)
				if err == nil && n != chain.Len() {
					err = io.ErrShortWrite
				}
				done <- err
			}()
			received := make([]byte, len(expected))
			if _, err := io.ReadFull(server, received); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(expected, received) {
				t.Fatal("writev truncated or reordered spans beyond vector limit")
			}
		})
	})
}
