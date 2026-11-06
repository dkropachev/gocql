// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/retry"

	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/testkit"

	lz4 "github.com/scylladb/gocql/v2/internal/wirecompress"
)

type fixtureMode struct {
	name        string
	version     byte
	compression protocol.Compressor
	tls         bool
}

var fixtureModes = []fixtureMode{{"v3", 3, nil, false}, {"v4", 4, nil, false}, {"v4tls", 4, nil, true}, {"v5raw", 5, nil, false}, {"v5lz4", 5, lz4.LZ4Compressor{}, false}, {"v3tls", 3, nil, true}, {"v5rawtls", 5, nil, true}, {"v5lz4tls", 5, lz4.LZ4Compressor{}, true}}

func fixtureStreamSession(t testing.TB, mode fixtureMode, spy ...bool) (*cluster.Session, *transport.Conn, net.Conn) {
	t.Helper()
	s, _, _ := runtimeTestSession(t, 0)
	c, peer := typedTestConn(t, mode.version, mode.compression)
	if mode.tls {
		cert, config := testTLS(t)
		rawClient := c.InternalR.(*transport.InternalConnReader).InternalConn
		client := tls.Client(rawClient, config)
		server := tls.Server(peer, &tls.Config{Certificates: []tls.Certificate{cert}})
		done := make(chan error, 1)
		go func() { done <- server.Handshake() }()
		if err := client.Handshake(); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		clientConn := &tlsConn{Conn: client, raw: rawClient}
		c.InternalR.(*transport.InternalConnReader).InternalConn = clientConn
		c.InternalW = &transport.InternalDeadlineContextWriter{InternalW: clientConn, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
		peer = &tlsConn{Conn: server, raw: peer}
	}
	if len(spy) > 0 && spy[0] {
		c.InternalR.(*transport.InternalConnReader).InternalConn = &fixtureReadTargets{Conn: c.InternalR.(*transport.InternalConnReader).InternalConn}
	}
	c.InternalHost = func() *endpoint.HostInfo {
		opaqueStorageValue := new(endpoint.HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValue).InternalHostId = cqltype.UUID{15: 1}
		implEndpoint.HostInfoData(opaqueStorageValue).InternalState.Store(endpoint.NodeUp)
		return opaqueStorageValue
	}()
	c.InternalHooks = cluster.InternalNewConnectionHooks(s)
	implCluster.SessionData(s).InternalHostSource.InternalHosts[c.InternalHost.HostID()] = c.InternalHost
	picker := pool.InternalNewDefaultConnPicker(1)
	picker.Put(c)
	implCluster.SessionData(s).InternalPool.InternalHostConnPools[implEndpoint.InternalHostInfoHostUUID(c.InternalHost)] = &pool.InternalHostConnPool{InternalHost: c.InternalHost, InternalConnPicker: picker, InternalRepairCtx: c.InternalCtx}
	t.Cleanup(func() { peer.Close(); s.Close() })
	go c.InternalServe(c.InternalCtx)
	return s, c, peer
}

func fixturePacket(mode fixtureMode, frame []byte) []byte {
	if mode.version < 5 {
		return frame
	}
	var packet []byte
	for offset := 0; offset < len(frame); {
		end := min(offset+130000, len(frame))
		packet = append(packet, testkit.Unit(frame[offset:end], mode.compression != nil, len(frame) <= 130000)...)
		offset = end
	}
	return packet
}

func TestStreamIndependentEverySplitAndTruncation(t *testing.T) {
	for _, mode := range fixtureModes {
		t.Run(mode.name, func(t *testing.T) {
			sample := fixturePacket(mode, testkit.BlobRows(mode.version, 0, []byte("first"), []byte("last")))
			for _, truncate := range []bool{false, true} {
				t.Run(fmt.Sprintf("truncate=%v", truncate), func(t *testing.T) {
					for split := 0; split < len(sample); split++ {
						s, c, peer := fixtureStreamSession(t, mode)
						done := make(chan struct{})
						finish := make(chan struct{})
						var finishOnce sync.Once
						finishRead := func() { finishOnce.Do(func() { close(finish) }) }
						t.Cleanup(finishRead)
						go func() {
							defer close(done)
							stream := typedTestReadRequest(t, c, peer)
							packet := fixturePacket(mode, testkit.BlobRows(mode.version, uint16(stream), []byte("first"), []byte("last")))
							_, _ = peer.Write(packet[:split])
							if !truncate {
								_, _ = peer.Write(packet[split:])
								// net.Pipe rejects deadline setters after peer close,
								// unlike a TCP socket with a received FIN. Keep complete
								// TLS records readable until the consumer acknowledges.
								<-finish
							}
							peer.Close()
						}()
						b := runtimeQuery(t)
						ctx, cancel := context.WithTimeout(context.Background(), time.Second)
						cursor, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
						if err == nil {
							dst := make([]byte, 0, 10)
							var rows [][]byte
							for cursor.NextInto(&dst) {
								rows = append(rows, append([]byte(nil), dst...))
							}
							err = cursor.Err()
							cursor.Close()
							if !truncate && (len(rows) != 2 || string(rows[0]) != "first" || string(rows[1]) != "last") {
								t.Fatalf("split %d rows %q error %v", split, rows, err)
							}
						}
						if truncate && err == nil {
							t.Fatalf("accepted truncated prefix %d/%d", split, len(sample))
						}
						if !truncate && err != nil {
							t.Fatalf("split %d: %v", split, err)
						}
						finishRead()
						cancel()
						b.Close()
						peer.Close()
						s.Close()
						<-done
					}
				})
			}
		})
	}
}

func TestStreamIndependentFirstRowGate(t *testing.T) {
	for _, mode := range fixtureModes {
		t.Run(mode.name, func(t *testing.T) {
			s, c, peer := fixtureStreamSession(t, mode)
			release := make(chan struct{})
			done := make(chan struct{})
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			go func() {
				defer close(done)
				stream := typedTestReadRequest(t, c, peer)
				frame := testkit.BlobRows(mode.version, uint16(stream), []byte("first"), []byte("second"))
				cut := len(frame) - 10
				first, last := frame[:cut], frame[cut:]
				if mode.version == 5 {
					first = testkit.Unit(first, mode.compression != nil, false)
					last = testkit.Unit(last, mode.compression != nil, false)
				}
				_, _ = peer.Write(first)
				<-release
				_, _ = peer.Write(last)
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			b := runtimeQuery(t)
			defer b.Close()
			cursor, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
			if err != nil {
				t.Fatal(err)
			}
			dst := make([]byte, 0, 12)
			if !cursor.NextInto(&dst) || string(dst) != "first" {
				t.Fatal("first row waited beyond gate", cursor.Err())
			}
			close(release)
			if !cursor.NextInto(&dst) || string(dst) != "second" {
				t.Fatal(cursor.Err())
			}
			if cursor.NextInto(&dst) || cursor.Err() != nil {
				t.Fatal(cursor.Err())
			}
			cursor.Close()
			<-done
		})
	}
}

func TestStreamIndependentCorruptChecksums(t *testing.T) {
	for _, mode := range fixtureModes {
		if mode.version != 5 {
			continue
		}
		t.Run(mode.name, func(t *testing.T) {
			// Corrupt every byte of each header and payload checksum, independently.
			headerSize := 6
			if mode.compression != nil {
				headerSize = 8
			}
			for _, checksum := range []string{"header", "payload"} {
				for i := 0; i < map[string]int{"header": 3, "payload": 4}[checksum]; i++ {
					s, c, peer := fixtureStreamSession(t, mode)
					done := make(chan struct{})
					finish := make(chan struct{})
					var finishOnce sync.Once
					finishRead := func() { finishOnce.Do(func() { close(finish) }) }
					t.Cleanup(finishRead)
					go func() {
						defer close(done)
						stream := typedTestReadRequest(t, c, peer)
						packet := fixturePacket(mode, testkit.BlobRows(mode.version, uint16(stream), []byte("forbidden")))
						offset := headerSize - 3 + i
						if checksum == "payload" {
							offset = len(packet) - 4 + i
						}
						packet[offset] ^= 0x80
						_, _ = peer.Write(packet)
						peer.Close()
					}()
					b := runtimeQuery(t)
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					cursor, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
					if err == nil {
						dst := make([]byte, 0, 20)
						if cursor.NextInto(&dst) {
							t.Fatal("corrupt unit delivered caller output")
						}
						err = cursor.Err()
						cursor.Close()
					}
					if err == nil {
						t.Fatal("corrupt checksum accepted")
					}
					finishRead()
					cancel()
					b.Close()
					peer.Close()
					s.Close()
					<-done
				}
			}
		})
	}
}

type finalAccess struct {
	entered, leave chan struct{}
	writes         atomic.Int32
}

func (d *finalAccess) Compile(cqltype.Columns) (codec.StreamRowPlan[[]byte], error) {
	return d, nil
}
func (d *finalAccess) Reset(dst *[]byte) { *dst = (*dst)[:0] }
func (d *finalAccess) DecodeInto(r *codec.StreamRowReader, dst *[]byte) error {
	plan, err := codec.StreamBlob.Compile(codecTestMust(cqltype.ScalarType(cqltype.TypeBlob)))
	if err != nil {
		return err
	}
	if err = codec.ReadStreamColumn(r, plan, dst); err != nil {
		return err
	}
	close(d.entered)
	<-d.leave
	(*dst)[0] = 'F'
	d.writes.Add(1)
	return nil
}
func TestStreamCloseDuringFinalCallerAccess(t *testing.T) {
	for _, mode := range fixtureModes {
		for _, action := range []string{"close", "cancel", "session"} {
			t.Run(mode.name+"-"+action, func(t *testing.T) {
				s, c, peer := fixtureStreamSession(t, mode)
				go func() {
					stream := typedTestReadRequest(t, c, peer)
					_, _ = peer.Write(fixturePacket(mode, testkit.BlobRows(mode.version, uint16(stream), []byte("final"))))
				}()
				b := runtimeQuery(t)
				defer b.Close()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				decoder := &finalAccess{entered: make(chan struct{}), leave: make(chan struct{})}
				cursor, err := RunStream(ctx, s, b, decoder, StreamOptions{Incremental: true})
				if err != nil {
					t.Fatal(err)
				}
				dst := make([]byte, 0, 10)
				next := make(chan struct{})
				go func() { cursor.NextInto(&dst); close(next) }()
				<-decoder.entered
				closing := make(chan struct{})
				closed := make(chan struct{})
				go func() {
					close(closing)
					switch action {
					case "cancel":
						cancel()
						cursor.Close()
					case "session":
						s.Close()
					default:
						cursor.Close()
					}
					close(closed)
				}()
				<-closing
				for i := 0; i < 100; i++ {
					runtime.Gosched()
				}
				select {
				case <-closed:
					t.Fatal("close returned during final storage access")
				default:
				}
				close(decoder.leave)
				<-closed
				<-next
				if decoder.writes.Load() != 1 || !bytes.Equal(dst, []byte("Final")) {
					t.Fatal("missing joined final write", dst)
				}
				snapshot := append([]byte(nil), dst...)
				cursor.Close()
				s.Close()
				if !bytes.Equal(snapshot, dst) {
					t.Fatal("late write")
				}
			})
		}
	}
}

func TestLoggerConcurrentCapture(t *testing.T) {
	logger := &logging.InternalTestLogger{}
	done := make(chan struct{}, 8)
	for i := 0; i < 8; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				logger.Print("a")
				logger.Printf("%s", "b")
				logger.Println("c")
				_ = logger.String()
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	if len(logger.String()) != 8*100*4 {
		t.Fatal("lost concurrent log data")
	}
}

// A failed stream write must poison its connection and release canonical input,
// whether no byte or a request prefix reached the peer.
type interruptedWriter struct {
	net.Conn
	accepted int
	calls    atomic.Int32
}

func (w *interruptedWriter) Write(p []byte) (int, error) {
	w.calls.Add(1)
	return min(w.accepted, len(p)), net.ErrClosed
}
func TestStreamInterruptedRequestWrite(t *testing.T) {
	for _, mode := range fixtureModes {
		for _, accepted := range []int{0, 1, 8} {
			t.Run(mode.name+fmt.Sprint(accepted), func(t *testing.T) {
				s, c, peer := fixtureStreamSession(t, mode)
				defer peer.Close()
				writer := &interruptedWriter{Conn: c.InternalR.(*transport.InternalConnReader).InternalConn, accepted: accepted}
				c.InternalW.(*transport.InternalDeadlineContextWriter).InternalW = writer
				b := runtimeQuery(t)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				cursor, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true, Options: OptionValues{Retry: retry.Safe{MaxAttempts: 1}}})
				if cursor != nil {
					cursor.Close()
				}
				if err == nil {
					t.Fatal("interrupted write succeeded")
				}
				b.Close()
				if err := b.Wait(ctx); err != nil {
					t.Fatal("write failure retained input", err)
				}
				if !c.Closed() || writer.calls.Load() != 1 {
					t.Fatal("failed request connection not poisoned", writer.calls.Load())
				}
			})
		}
	}
}

func TestIndependentRawDirectRead(t *testing.T) {
	for _, mode := range fixtureModes {
		if mode.version == 5 {
			continue
		}
		t.Run(mode.name, func(t *testing.T) {
			s, c, peer := fixtureStreamSession(t, mode, true)
			payload := bytes.Repeat([]byte{0x71}, 400<<10)
			done := make(chan struct{})
			go func() {
				defer close(done)
				stream := typedTestReadRequest(t, c, peer)
				_, _ = peer.Write(testkit.BlobRows(mode.version, uint16(stream), payload))
			}()
			b := runtimeQuery(t)
			defer b.Close()
			cursor, err := RunStream(context.Background(), s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
			if err != nil {
				t.Fatal(err)
			}
			dst := make([]byte, 0, len(payload))
			backing := &dst[:cap(dst)][0]
			if !cursor.NextInto(&dst) || !bytes.Equal(dst, payload) || &dst[0] != backing {
				t.Fatal("lost final storage", cursor.Err())
			}
			spy := c.InternalR.(*transport.InternalConnReader).InternalConn.(*fixtureReadTargets)
			spy.mu.Lock()
			direct := 0
			otherBulk := 0
			start := uintptr(unsafe.Pointer(backing))
			end := start + uintptr(len(dst))
			for _, target := range spy.targets {
				if target.n == 0 {
					continue
				}
				address := uintptr(unsafe.Pointer(&target.p[0]))
				if address >= start && address+uintptr(target.n) <= end {
					direct += target.n
				} else if target.n >= 4096 {
					otherBulk += target.n
				}
			}
			spy.mu.Unlock()
			// Native read-ahead may stage one bounded prefix; the bulk must
			// still land in the caller backing without payload-sized scratch.
			if direct < len(payload)-(4<<10) || direct > len(payload) || otherBulk > 4<<10 {
				t.Fatal("unexpected raw payload staging", direct, otherBulk)
			}
			cursor.NextInto(&dst)
			cursor.Close()
			<-done
		})
	}
}

// Fault fixtures interrupt TLS through the socket, including blocked close alerts.
type tlsConn struct {
	*tls.Conn
	raw net.Conn
}

func (c *tlsConn) Close() error { return c.raw.Close() }

type fixtureReadTargets struct {
	net.Conn
	mu      sync.Mutex
	targets []struct {
		p []byte
		n int
	}
}

func (c *fixtureReadTargets) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.mu.Lock()
	c.targets = append(c.targets, struct {
		p []byte
		n int
	}{p, n})
	c.mu.Unlock()
	return n, err
}
