package cluster

import (
	"net"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/internal/protocol"
)

// A RandomPartitioner cluster has 128-bit tokens, which metadata.HostSnapshot
// cannot hold, so opening a session to it fails and names the token. The
// driver supports clusters whose partitioner has int64 tokens (Murmur3), as
// docs/source/server-differences.md says.
func TestRandomPartitionerClusterIsRejected(t *testing.T) {
	t.Parallel()
	rs := newRingTestServer(t)
	rs.localRows = func(stream int, resp *protocol.Framer) {
		row := systemLocalRow(net.IPv4(127, 0, 0, 1))
		row[5] = []byte("org.apache.cassandra.dht.RandomPartitioner")
		row[6] = encodeTextSet("42535295865117307932921825928971026432") // 2^125
		writeRows(resp, stream, systemLocalCols, [][][]byte{row})
	}
	s, err := rs.cluster().Open()
	if err == nil {
		_ = s.Close()
		t.Fatal("Open succeeded against a RandomPartitioner cluster")
	}
	if !strings.Contains(err.Error(), "invalid token") {
		t.Fatalf("Open = %v, want an invalid token error", err)
	}
}
