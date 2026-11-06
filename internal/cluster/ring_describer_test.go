package cluster

import (
	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/transport"

	"context"
	"fmt"
	"net"
	"time"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/tests/mock"
)

type mockConnection struct{}

func (*mockConnection) Close() {}
func (*mockConnection) InternalExec(ctx context.Context, req protocol.FrameBuilder, tracer transport.Tracer, requestTimeout time.Duration) (*protocol.Framer, error) {
	return nil, nil
}
func (*mockConnection) InternalAwaitSchemaAgreement(ctx context.Context) error { return nil }

var systemLocalResultMetadata = protocol.ResultMetadata{
	Flags:          0,
	PagingState:    []byte{},
	ActualColCount: 11,
	Columns: []implCqltype.ColumnInfo{{
		Keyspace: "system",
		Table:    "local",
		Name:     "broadcast_address",
		TypeInfo: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeInet, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "cluster_name",
		TypeInfo: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeVarchar, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "data_center",
		TypeInfo: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeVarchar, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "host_id",
		TypeInfo: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeUUID, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "listen_address",
		TypeInfo: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeInet, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "partitioner",
		TypeInfo: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeVarchar, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "rack",
		TypeInfo: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeVarchar, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "release_version",
		TypeInfo: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeVarchar, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "rpc_address",
		TypeInfo: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeInet, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "schema_version",
		TypeInfo: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeUUID, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "tokens",
		TypeInfo: implCqltype.CollectionType{
			NativeType: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeSet, ""),
			Elem:       cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeVarchar, ""),
		},
	}},
}

var systemPeersResultMetadata = protocol.ResultMetadata{
	Flags:          0,
	PagingState:    []byte{},
	ActualColCount: 9,
	Columns: []implCqltype.ColumnInfo{{
		Keyspace: "system",
		Table:    "peers",
		Name:     "peer",
		TypeInfo: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeInet, ""),
	}, {
		Keyspace: "system",
		Table:    "peers",
		Name:     "data_center",
		TypeInfo: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeVarchar, ""),
	}, {
		Keyspace: "system",
		Table:    "peers",
		Name:     "host_id",
		TypeInfo: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeUUID, ""),
	}, {
		Keyspace: "system",
		Table:    "peers",
		Name:     "preferred_ip",
		TypeInfo: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeInet, ""),
	}, {
		Keyspace: "system",
		Table:    "peers",
		Name:     "rack",
		TypeInfo: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeVarchar, ""),
	}, {
		Keyspace: "system",
		Table:    "peers",
		Name:     "release_version",
		TypeInfo: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeVarchar, ""),
	}, {
		Keyspace: "system",
		Table:    "peers",
		Name:     "rpc_address",
		TypeInfo: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeInet, ""),
	}, {
		Keyspace: "system",
		Table:    "peers",
		Name:     "schema_version",
		TypeInfo: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeUUID, ""),
	}, {
		Keyspace: "system",
		Table:    "peers",
		Name:     "tokens",
		TypeInfo: implCqltype.CollectionType{
			NativeType: cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeSet, ""),
			Elem:       cqltype.NewCustomType(protocol.ProtoVersion4, implCqltype.TypeVarchar, ""),
		},
	}},
}

func (*mockConnection) querySystem(ctx context.Context, query string, values ...any) *nativeResultFixture {
	// Column order matches the explicit SELECT in qrySystemLocal:
	// broadcast_address, cluster_name, data_center, host_id, listen_address, partitioner, rack, release_version, rpc_address, schema_version, tokens
	localData := []any{net.IPv4(192, 168, 100, 12), "", "datacenter1", implCqltype.ParseUUIDMust("045859a7-6b9f-4efd-a5e7-acd64a295e13"), net.IPv4(192, 168, 100, 12), "org.apache.cassandra.dht.Murmur3Partitioner", "rack1", "3.0.8", net.IPv4(192, 168, 100, 12), implCqltype.ParseUUIDMust("daf4df2c-b708-11ef-5c25-3004361afd71"), []string{}}
	// Column order matches the explicit SELECT in qrySystemPeersCassandra:
	// peer, data_center, host_id, preferred_ip, rack, release_version, rpc_address, schema_version, tokens
	peerData1 := []any{net.IPv4(192, 168, 100, 13), "datacenter1", implCqltype.ParseUUIDMust("b953309f-6e68-41f2-baf5-0e60da317a9c"), nil, "rack1", "3.0.8", net.IPv4(192, 168, 100, 13), implCqltype.ParseUUIDMust("b6ed5bde-b318-11ef-8f58-aeba19e31273"), []string{"-1032311531684407545", "-1112089412567859825"}}
	peerData2 := []any{net.IPv4(192, 168, 100, 14), "datacenter1", implCqltype.ParseUUIDMust("8269e111-ea38-44bd-a73f-9d3d12cfaf78"), nil, "rack1", "3.0.8", net.IPv4(192, 168, 100, 14), implCqltype.ParseUUIDMust("b6ed5bde-b318-11ef-8f58-aeba19e31273"), []string{}}

	if query == transport.InternalQrySystemLocal {
		return &nativeResultFixture{
			meta:    systemLocalResultMetadata,
			framer:  &mock.MockFramer{Data: marshalMetadataMust(systemLocalResultMetadata, localData)},
			numRows: 1,
		}
	} else if query == transport.InternalQrySystemPeersCassandra {
		return &nativeResultFixture{
			meta:    systemPeersResultMetadata,
			framer:  &mock.MockFramer{Data: append(marshalMetadataMust(systemPeersResultMetadata, peerData1), marshalMetadataMust(systemPeersResultMetadata, peerData2)...)},
			numRows: 2,
		}
	}
	return nil
}

func (*mockConnection) InternalGetIsSchemaV2() bool { return false }
func (*mockConnection) InternalSetSchemaV2(s bool)  {}
func (*mockConnection) InternalIsScyllaConn() bool  { return false }
func (*mockConnection) InternalGetScyllaSupported() endpoint.ScyllaConnectionFeatures {
	return endpoint.ScyllaConnectionFeatures{}
}

type mockControlConn struct{}

func (m *mockControlConn) querySystem(statement string, values ...any) (iter *nativeResultFixture) {
	return nil
}

func (m *mockControlConn) InternalReconnect() error {
	return nil
}

func (m *mockControlConn) InternalGetConn() *InternalConnHost {
	return &InternalConnHost{
		InternalConn: &mockConnection{},
		InternalHost: func() *endpoint.HostInfo {
			opaqueStorageValue := new(endpoint.HostInfo)
			return opaqueStorageValue
		}(),
	}
}

func (m *mockControlConn) InternalAwaitSchemaAgreement() error { return nil }
func (m *mockControlConn) query(statement string, values ...any) (iter *nativeResultFixture) {
	return nil
}
func (m *mockControlConn) InternalDiscoverProtocol(hosts []*endpoint.HostInfo) (int, error) {
	return 0, nil
}
func (m *mockControlConn) InternalConnect(hosts []*endpoint.HostInfo) error { return nil }
func (m *mockControlConn) InternalClose()                                   {}
func (m *mockControlConn) InternalGetSession() *Session                     { return nil }

func marshalMetadataMust(metadata protocol.ResultMetadata, data []any) [][]byte {
	if len(metadata.Columns) != len(data) {
		panic("metadata length mismatch")
	}
	res := make([][]byte, len(metadata.Columns))
	for id, col := range metadata.Columns {
		var err error
		value := data[id]
		res[id], err = encodeFixtureCell(col.TypeInfo, value)
		if err != nil {
			panic(fmt.Sprintf("unable to marshal column %d: %v", id, err))
		}
	}
	return res
}

type trackingRingConnection struct {
	iter      *nativeResultFixture
	schemaV2  bool
	scylla    bool
	lastQuery string
}

func (*trackingRingConnection) Close() {}
func (*trackingRingConnection) InternalExec(context.Context, protocol.FrameBuilder, transport.Tracer, time.Duration) (*protocol.Framer, error) {
	return nil, nil
}
func (*trackingRingConnection) InternalAwaitSchemaAgreement(context.Context) error { return nil }

func (c *trackingRingConnection) querySystem(_ context.Context, q string, _ ...any) *nativeResultFixture {
	c.lastQuery = q
	return c.iter
}
func (c *trackingRingConnection) InternalGetIsSchemaV2() bool { return c.schemaV2 }
func (*trackingRingConnection) InternalSetSchemaV2(bool)      {}
func (c *trackingRingConnection) InternalIsScyllaConn() bool  { return c.scylla }
func (*trackingRingConnection) InternalGetScyllaSupported() endpoint.ScyllaConnectionFeatures {
	return endpoint.ScyllaConnectionFeatures{}
}
