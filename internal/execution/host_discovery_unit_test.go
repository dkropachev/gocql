package execution

import (
	"net"
	"sort"
	"strings"
	"sync"
	"testing"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

type testPeer struct {
	hostID cqltype.UUID
	ip     net.IP
	dc     string
	rack   string
	tokens []string
}

var systemPeersCols = []rowsCol{
	{"peer", optNative(cqltype.TypeInet)},
	{"data_center", optNative(cqltype.TypeVarchar)},
	{"host_id", optNative(cqltype.TypeUUID)},
	{"rack", optNative(cqltype.TypeVarchar)},
	{"rpc_address", optNative(cqltype.TypeInet)},
	{"tokens", optCollection(cqltype.TypeSet, optNative(cqltype.TypeVarchar))},
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
				writeErrorResult(resp, stream, protocol.ErrCodeServer, peersErr)
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

func (rs *ringTestServer) session(t *testing.T, configure func(cfg *implCluster.InternalClusterSettings)) *implCluster.Session {
	t.Helper()
	cfg := rs.cluster()
	if configure != nil {
		configure(cfg)
	}
	s, err := implCluster.InternalClusterSettingsOpen(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// ringHosts lists the session's known hosts as "hostID@ip" sorted, so tests
// can compare the whole ring at once.
func ringHosts(s *implCluster.Session) []string {
	var out []string
	for _, h := range implCluster.SessionData(s).InternalHostSource.InternalGetHostsList() {
		out = append(out, h.HostID()+"@"+h.ConnectAddress().String())
	}
	sort.Strings(out)
	return out
}

func newTestPeer(ip string) testPeer {
	return testPeer{hostID: cqltype.MustRandomUUID(), ip: net.ParseIP(ip), dc: "dc1", rack: "rack1", tokens: []string{"5"}}
}
