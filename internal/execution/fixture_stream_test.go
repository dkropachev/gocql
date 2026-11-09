// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/transport"

	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"
	"testing"

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

type finalAccess struct {
	entered, leave chan struct{}
	writes         atomic.Int32
}

func (d *finalAccess) Compile(cqltype.Columns) (codec.StreamRowPlan[[]byte], error) {
	return d, nil
}
func (d *finalAccess) Reset(dst *[]byte) { *dst = (*dst)[:0] }
func (d *finalAccess) DecodeInto(r *codec.StreamRowReader, dst *[]byte) error {
	plan, err := codec.StreamBlob.Compile(cqltype.BlobType)
	if err != nil {
		return err
	}
	if err = r.ReadColumn(plan, dst); err != nil {
		return err
	}
	close(d.entered)
	<-d.leave
	(*dst)[0] = 'F'
	d.writes.Add(1)
	return nil
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
