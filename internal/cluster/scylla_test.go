package cluster

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	implPool "github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func hammerConnPicker(t *testing.T, wg *sync.WaitGroup, s *implPool.InternalScyllaConnPicker, loops int) {
	t.Helper()
	for i := 0; i < loops; i++ {
		if c := s.Pick(nil); c == nil {
			t.Error("unexpected nil")
		}
	}
	wg.Done()
}

// framerForExts returns a pooled write framer from a mock driverConn that negotiated
// exts, i.e. through the same connFramers.initCache path production uses. There is
// no standalone framer-with-extensions constructor to test against: the per-driverConn
// pool is the only place the negotiated extension list is turned into framer state.
func framerForExts(exts []endpoint.InternalCqlProtocolExtension) *protocol.Framer {
	conn := mockConn(0)
	conn.InternalVersion = protocol.ProtoVersion4
	conn.InternalLogger = logging.Nop()
	conn.InternalCqlProtoExts = exts
	conn.InternalInitFramerCache()
	return conn.InternalGetWriteFramer()
}

func mockConn(shard int) *transport.Conn {
	return &transport.Conn{
		InternalStreams: streams.New(),
		InternalScyllaSupported: *func() *endpoint.ScyllaConnectionFeatures {
			opaqueStorageValuexx := new(endpoint.ScyllaConnectionFeatures)
			implEndpoint.ScyllaConnectionFeaturesData(opaqueStorageValuexx).InternalShard = shard
			opaqueStorageValuexx.ScyllaHostFeatures = *func() *endpoint.ScyllaHostFeatures {
				opaqueStorageValuex := new(endpoint.ScyllaHostFeatures)
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuex).InternalNrShards = 4
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuex).InternalMsbIgnore = 12
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuex).InternalPartitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuex).InternalShardingAlgorithm = "biased-token-round-robin"
				return opaqueStorageValuex
			}()
			return opaqueStorageValuexx
		}(),
	}
}

func mockConnForPicker(shard, nrShards int) *transport.Conn {
	ctx, cancel := context.WithCancel(context.Background())

	conn1, conn2 := net.Pipe()
	_ = conn2.Close()

	return &transport.Conn{
		InternalScyllaSupported: *func() *endpoint.ScyllaConnectionFeatures {
			opaqueStorageValuexxxx := new(endpoint.ScyllaConnectionFeatures)
			implEndpoint.ScyllaConnectionFeaturesData(opaqueStorageValuexxxx).InternalShard = shard
			opaqueStorageValuexxxx.ScyllaHostFeatures = *func() *endpoint.ScyllaHostFeatures {
				opaqueStorageValuexxx := new(endpoint.ScyllaHostFeatures)
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexxx).InternalNrShards = nrShards
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexxx).InternalMsbIgnore = 12
				return opaqueStorageValuexxx
			}()
			return opaqueStorageValuexxxx
		}(),

		InternalR:       &transport.InternalConnReader{InternalConn: conn1, InternalR: bufio.NewReader(conn1)},
		InternalAddr:    fmt.Sprintf("192.168.1.%d:9042", shard+1),
		InternalClosed:  false,
		InternalMu:      sync.Mutex{},
		InternalLogger:  logging.Nop(),
		InternalCtx:     ctx,
		InternalCancel:  cancel,
		InternalCalls:   newCallTable(),
		InternalStreams: streams.New(),
	}
}
