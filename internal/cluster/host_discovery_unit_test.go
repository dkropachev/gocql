package cluster

import (
	"net"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

type testPeer struct {
	hostID cql.UUID
	ip     net.IP
	dc     string
	rack   string
	tokens []string
}

var systemPeersCols = []rowsCol{
	{"peer", optNative(cql.TypeInet)},
	{"data_center", optNative(cql.TypeVarchar)},
	{"host_id", optNative(cql.TypeUUID)},
	{"rack", optNative(cql.TypeVarchar)},
	{"rpc_address", optNative(cql.TypeInet)},
	{"tokens", optCollection(cql.TypeSet, optNative(cql.TypeVarchar))},
}

// optBytes encodes s as a value, or as null when it is empty.
func optBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return []byte(s)
}

// ringTestServer is a controlTestServer whose system.peers table a test can
// change between ring refreshes.
type ringTestServer struct {
	*controlTestServer
	mu    sync.Mutex
	peers []testPeer
	// peersErr, when set, makes the system.peers query fail.
	peersErr string
}

func newRingTestServer(t *testing.T, peers ...testPeer) *ringTestServer {
	t.Helper()
	rs := &ringTestServer{peers: peers}
	rs.controlTestServer = newControlTestServer(t, func(srv *controlTestServer) {
		srv.queries = func(query string, stream int, resp *protocol.Framer) bool {
			if !strings.Contains(query, "system.peers") {
				return false
			}
			rs.mu.Lock()
			peers := append([]testPeer(nil), rs.peers...)
			peersErr := rs.peersErr
			rs.mu.Unlock()
			if peersErr != "" {
				writeErrorResult(resp, stream, cqlerr.CodeServer, peersErr)
				return true
			}
			var rows [][][]byte
			for _, p := range peers {
				id := p.hostID
				rows = append(rows, [][]byte{
					p.ip.To4(), optBytes(p.dc), id[:], optBytes(p.rack), p.ip.To4(), encodeTextSet(p.tokens...),
				})
			}
			writeRows(resp, stream, systemPeersCols, rows)
			return true
		}
	})
	return rs
}

func (rs *ringTestServer) setPeers(peers ...testPeer) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.peers = peers
}

func (rs *ringTestServer) failPeers(msg string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.peersErr = msg
}

func (rs *ringTestServer) session(t *testing.T, configure func(cfg *InternalClusterSettings)) *Core {
	t.Helper()
	cfg := rs.cluster()
	if configure != nil {
		configure(cfg)
	}
	s, err := cfg.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// ringHosts lists the session's known hosts as "hostID@ip" sorted, so tests
// can compare the whole ring at once.
func ringHosts(s *Core) []string {
	var out []string
	for _, h := range SessionData(s).InternalHostSource.InternalGetHostsList() {
		out = append(out, h.HostID()+"@"+h.ConnectAddress().String())
	}
	sort.Strings(out)
	return out
}

func peerKey(p testPeer) string { return p.hostID.String() + "@" + p.ip.String() }

func localKey() string { return testLocalHostID.String() + "@127.0.0.1" }

func sortedKeys(keys ...string) []string {
	sort.Strings(keys)
	return keys
}

func newTestPeer(ip string) testPeer {
	return testPeer{hostID: cql.RandomUUID(), ip: net.ParseIP(ip), dc: "dc1", rack: "rack1", tokens: []string{"5"}}
}
