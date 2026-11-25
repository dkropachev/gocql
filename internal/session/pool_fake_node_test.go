// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session_test

import (
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

// listenerKind names the port of a fakeNode a connection arrived on.
type listenerKind int

const (
	regularPort listenerKind = iota
	shardAwarePort
	shardAwareTLSPort
	// routePort is a second regular port, published by a client route only.
	routePort
	listenerKinds
)

func (k listenerKind) String() string {
	return [...]string{"regular", "shard-aware", "shard-aware TLS", "route"}[k]
}

// nodeConn is one connection a fakeNode accepted.
type nodeConn struct {
	conn       net.Conn
	kind       listenerKind
	shard      int
	sourcePort int
	// control is set once the connection registers for events.
	control bool
}

// fakeNode is a one-node ScyllaDB cluster speaking just enough of protocol 4
// for a session to connect, fill its pool and keep it: SUPPORTED with the
// sharding keys, system.local, an empty system.peers, and void for
// everything else.
//
// A connection to the regular port lands on the next shard round-robin; one
// to a shard-aware port lands on its source port modulo the shard count, as
// in ScyllaDB, after natShift is added to the port the way a NAT rewriting
// source ports would.
type fakeNode struct {
	t      testing.TB
	shards int
	// rpc is the address system.local reports for clients, broadcast the
	// one for other nodes; nil broadcast reports rpc.
	rpc, broadcast net.IP
	// tls, when set, wraps the regular and shard-aware TLS ports.
	tls *tls.Config
	// route, when set, is the client connection ID system.client_routes
	// publishes the route port under.
	route cql.UUID

	mu        sync.Mutex
	listeners [listenerKinds]net.Listener
	addrs     [listenerKinds]string
	// advertise reports the shard-aware ports in SUPPORTED, even when no
	// listener serves them.
	advertise [listenerKinds]bool
	natShift  int
	next      int
	open      map[net.Conn]nodeConn
	accepted  [listenerKinds]int
	stopped   bool
	wg        sync.WaitGroup
}

// newFakeNode starts the node's regular port and, as listed in serve, its
// shard-aware ports, all on loopback; SUPPORTED advertises those it serves.
func newFakeNode(t testing.TB, shards int, tlsConfig *tls.Config, serve ...listenerKind) *fakeNode {
	t.Helper()
	n := &fakeNode{t: t, shards: shards, rpc: net.IPv4(127, 0, 0, 1), tls: tlsConfig, open: make(map[net.Conn]nodeConn)}
	n.listen(regularPort, "127.0.0.1:0")
	for _, kind := range serve {
		n.listen(kind, "127.0.0.1:0")
		n.advertise[kind] = true
	}
	t.Cleanup(n.stop)
	return n
}

func (n *fakeNode) listen(kind listenerKind, addr string) {
	n.t.Helper()
	l, err := net.Listen("tcp", addr)
	if err != nil {
		n.t.Fatal(err)
	}
	n.mu.Lock()
	n.listeners[kind] = l
	n.addrs[kind] = l.Addr().String()
	n.mu.Unlock()
	n.wg.Add(1)
	go n.accept(kind, l)
}

// port returns the TCP port of kind.
func (n *fakeNode) port(kind listenerKind) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, p, _ := net.SplitHostPort(n.addrs[kind])
	v, _ := strconv.Atoi(p)
	return v
}

func (n *fakeNode) contact() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.addrs[regularPort]
}

// closeListener stops serving kind; the port keeps being advertised.
func (n *fakeNode) closeListener(kind listenerKind) {
	n.mu.Lock()
	l := n.listeners[kind]
	n.listeners[kind] = nil
	n.mu.Unlock()
	if l != nil {
		_ = l.Close()
	}
}

// down closes every listener and connection, so dials are refused.
func (n *fakeNode) down() {
	for kind := range listenerKinds {
		n.closeListener(kind)
	}
	n.dropAll()
}

// up listens again on the ports down closed.
func (n *fakeNode) up() {
	n.t.Helper()
	for kind := range listenerKinds {
		n.mu.Lock()
		addr, closed := n.addrs[kind], n.listeners[kind] == nil
		n.mu.Unlock()
		if addr != "" && closed {
			n.listen(kind, addr)
		}
	}
}

// dropAll closes every open connection.
func (n *fakeNode) dropAll() {
	n.mu.Lock()
	conns := make([]net.Conn, 0, len(n.open))
	for c := range n.open {
		conns = append(conns, c)
	}
	n.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (n *fakeNode) stop() {
	n.mu.Lock()
	n.stopped = true
	n.mu.Unlock()
	n.down()
	n.wg.Wait()
}

// poolConns returns the open connections other than the control
// connection.
func (n *fakeNode) poolConns() []nodeConn {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]nodeConn, 0, len(n.open))
	for _, c := range n.open {
		if !c.control {
			out = append(out, c)
		}
	}
	return out
}

// openConns returns how many connections are open, the control connection
// included.
func (n *fakeNode) openConns() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.open)
}

// acceptedOn returns how many connections kind accepted.
func (n *fakeNode) acceptedOn(kind listenerKind) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.accepted[kind]
}

func (n *fakeNode) accept(kind listenerKind, l net.Listener) {
	defer n.wg.Done()
	for {
		raw, err := l.Accept()
		if err != nil {
			return
		}
		c := nodeConn{conn: raw, kind: kind, sourcePort: raw.RemoteAddr().(*net.TCPAddr).Port}
		if n.tls != nil && kind != shardAwarePort {
			c.conn = tls.Server(raw, n.tls)
		}
		n.mu.Lock()
		if n.stopped || n.listeners[kind] != l {
			// down closed l after this connection arrived.
			n.mu.Unlock()
			_ = raw.Close()
			return
		}
		if kind == regularPort || kind == routePort {
			c.shard = n.next % n.shards
			n.next++
		} else {
			c.shard = (c.sourcePort + n.natShift) % n.shards
		}
		n.accepted[kind]++
		n.open[c.conn] = c
		n.wg.Add(1)
		n.mu.Unlock()
		go n.serve(c)
	}
}

func (n *fakeNode) supported(c nodeConn) map[string][]string {
	n.mu.Lock()
	defer n.mu.Unlock()
	m := map[string][]string{
		"CQL_VERSION":                {"3.0.0"},
		"SCYLLA_SHARD":               {strconv.Itoa(c.shard)},
		"SCYLLA_NR_SHARDS":           {strconv.Itoa(n.shards)},
		"SCYLLA_PARTITIONER":         {"org.apache.cassandra.dht.Murmur3Partitioner"},
		"SCYLLA_SHARDING_ALGORITHM":  {"biased-token-round-robin"},
		"SCYLLA_SHARDING_IGNORE_MSB": {"12"},
	}
	for kind, key := range map[listenerKind]string{shardAwarePort: "SCYLLA_SHARD_AWARE_PORT", shardAwareTLSPort: "SCYLLA_SHARD_AWARE_PORT_SSL"} {
		if n.advertise[kind] {
			_, p, _ := net.SplitHostPort(n.addrs[kind])
			m[key] = []string{p}
		}
	}
	return m
}

func (n *fakeNode) serve(c nodeConn) {
	defer n.wg.Done()
	defer func() {
		_ = c.conn.Close()
		n.mu.Lock()
		delete(n.open, c.conn)
		n.mu.Unlock()
	}()
	for {
		header := make([]byte, 9)
		head, err := protocol.ReadHeader(c.conn, header)
		if err != nil {
			n.readFailed(err)
			return
		}
		req := protocol.NewFramer(nil, protocol.ProtoVersion4)
		if err := req.ReadFrame(c.conn, &head); err != nil {
			n.readFailed(err)
			return
		}
		resp := protocol.NewFramer(nil, protocol.ProtoVersion4)
		stream := head.Stream
		switch head.Op {
		case frm.OpOptions:
			resp.WriteHeader(0, frm.OpSupported, stream)
			resp.WriteStringMultiMap(n.supported(c))
		case frm.OpRegister:
			n.mu.Lock()
			if cur, ok := n.open[c.conn]; ok {
				cur.control = true
				n.open[c.conn] = cur
			}
			n.mu.Unlock()
			resp.WriteHeader(0, frm.OpReady, stream)
		case frm.OpStartup:
			resp.WriteHeader(0, frm.OpReady, stream)
		case frm.OpQuery:
			n.query(req.ReadLongString(), stream, resp)
		default:
			resp.WriteHeader(0, frm.OpError, stream)
			resp.WriteInt(0)
			resp.WriteString("not supported")
		}
		resp.Buf[0] = protocol.ProtoVersion4 | 0x80
		if err := resp.Finish(); err != nil {
			n.t.Error(err)
			return
		}
		if err := resp.WriteFrameTo(c.conn); err != nil {
			return
		}
	}
}

func (n *fakeNode) readFailed(err error) {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, os.ErrDeadlineExceeded) {
		return
	}
	var alert tls.AlertError
	if errors.As(err, &alert) || strings.Contains(err.Error(), "tls:") || strings.Contains(err.Error(), "use of closed") {
		return
	}
	n.t.Logf("fake node read: %v", err)
}

var nodeHostID = cql.MustParseUUID("8f3e2b1a-4c5d-4e6f-9a0b-1c2d3e4f5a6b")

type column struct {
	name string
	typ  []cql.Kind
}

func (n *fakeNode) query(q string, stream int, resp *protocol.Framer) {
	switch {
	case strings.Contains(q, "system.local"):
		n.mu.Lock()
		rpc, broadcast := n.rpc.To4(), n.broadcast.To4()
		n.mu.Unlock()
		if broadcast == nil {
			broadcast = rpc
		}
		writeRows(resp, stream, []column{
			{"host_id", []cql.Kind{cql.KindUUID}},
			{"data_center", []cql.Kind{cql.KindText}},
			{"rack", []cql.Kind{cql.KindText}},
			{"rpc_address", []cql.Kind{cql.KindInet}},
			{"broadcast_address", []cql.Kind{cql.KindInet}},
			{"release_version", []cql.Kind{cql.KindText}},
			{"partitioner", []cql.Kind{cql.KindText}},
			{"tokens", []cql.Kind{cql.KindSet, cql.KindText}},
		}, [][]byte{nodeHostID[:], []byte("dc1"), []byte("rack1"), rpc, broadcast, []byte("3.0.8"), []byte("org.apache.cassandra.dht.Murmur3Partitioner"), textSet("-100", "100")})
	case strings.Contains(q, "system.client_routes"):
		cols := []column{
			{"connection_id", []cql.Kind{cql.KindUUID}},
			{"host_id", []cql.Kind{cql.KindUUID}},
			{"address", []cql.Kind{cql.KindText}},
			{"port", []cql.Kind{cql.KindInt}},
			{"tls_port", []cql.Kind{cql.KindInt}},
		}
		if n.route.IsZero() {
			writeRows(resp, stream, cols)
			return
		}
		writeRows(resp, stream, cols, [][]byte{n.route[:], nodeHostID[:], []byte("127.0.0.1"), binary.BigEndian.AppendUint32(nil, uint32(n.port(routePort))), nil})
	case strings.Contains(q, "system.peers"):
		writeRows(resp, stream, []column{
			{"peer", []cql.Kind{cql.KindInet}},
			{"data_center", []cql.Kind{cql.KindText}},
			{"host_id", []cql.Kind{cql.KindUUID}},
			{"rack", []cql.Kind{cql.KindText}},
			{"rpc_address", []cql.Kind{cql.KindInet}},
			{"tokens", []cql.Kind{cql.KindSet, cql.KindText}},
		})
	default:
		resp.WriteHeader(0, frm.OpResult, stream)
		resp.WriteInt(frm.ResultKindVoid)
	}
}

// writeRows writes a RESULT/Rows reply with the given rows.
func writeRows(resp *protocol.Framer, stream int, cols []column, rows ...[][]byte) {
	resp.WriteHeader(0, frm.OpResult, stream)
	resp.WriteInt(frm.ResultKindRows)
	resp.WriteInt(int32(frm.FlagGlobalTableSpec))
	resp.WriteInt(int32(len(cols)))
	resp.WriteString("system")
	resp.WriteString("local")
	for _, c := range cols {
		resp.WriteString(c.name)
		for _, typ := range c.typ {
			resp.WriteShort(uint16(typ))
		}
	}
	resp.WriteInt(int32(len(rows)))
	for _, row := range rows {
		for _, v := range row {
			resp.WriteBytes(v)
		}
	}
}

func textSet(items ...string) []byte {
	out := binary.BigEndian.AppendUint32(nil, uint32(len(items)))
	for _, it := range items {
		out = binary.BigEndian.AppendUint32(out, uint32(len(it)))
		out = append(out, it...)
	}
	return out
}
