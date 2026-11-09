package gocql

import (
	"github.com/scylladb/gocql/v2/cql"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"

	"encoding/binary"
	"net"
	"testing"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
)

func runtimeTestSession(t testing.TB, n int) (*Session, []*driverConn, []net.Conn) {
	t.Helper()
	s := func() *Session {
		opaqueStorageValue := implExecution.InternalWrapSession(new(implCluster.Core))
		implExecution.InternalSessionData(opaqueStorageValue).InternalLogger = nopLogger{}
		return opaqueStorageValue
	}()
	implExecution.InternalSessionData(s).InternalHostSource = &ringDescriber{InternalHosts: map[string]*HostInfo{}}
	implExecution.InternalSessionData(s).InternalPool = &policyConnPool{InternalHostConnPools: map[cql.UUID]*hostConnPool{}}
	var conns []*driverConn
	var peers []net.Conn
	for i := 0; i < n; i++ {
		conn, peer := typedTestConn(t, 4, nil)
		id := cql.UUID{15: byte(i + 1)}
		host := func() *HostInfo {
			opaqueStorageValuex := new(HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValuex).InternalHostId = id
			implEndpoint.HostInfoData(opaqueStorageValuex).InternalState.Store(NodeUp)
			return opaqueStorageValuex
		}()
		conn.InternalHost = host
		conn.InternalHooks = newConnectionHooks(s)
		implExecution.InternalSessionData(s).InternalHostSource.InternalHosts[id.String()] = host
		picker := newDefaultConnPicker(1)
		picker.Put(conn)
		implExecution.InternalSessionData(s).InternalPool.InternalHostConnPools[id] = &hostConnPool{InternalHost: host, InternalConnPicker: picker}
		conns = append(conns, conn)
		peers = append(peers, peer)
		go conn.InternalServe(conn.InternalCtx)
	}
	return s, conns, peers
}
func runtimeRowsBody(token []byte, values ...string) []byte {
	b := binary.BigEndian.AppendUint32(nil, 2)
	flags := uint32(1)
	if token != nil {
		flags |= 2
	}
	b = binary.BigEndian.AppendUint32(b, flags)
	b = binary.BigEndian.AppendUint32(b, 1)
	if token != nil {
		b = binary.BigEndian.AppendUint32(b, uint32(len(token)))
		b = append(b, token...)
	}
	for _, name := range []string{"ks", "tbl", "value"} {
		b = binary.BigEndian.AppendUint16(b, uint16(len(name)))
		b = append(b, name...)
	}
	b = binary.BigEndian.AppendUint16(b, uint16(cql.TypeBlob))
	b = binary.BigEndian.AppendUint32(b, uint32(len(values)))
	for _, value := range values {
		b = binary.BigEndian.AppendUint32(b, uint32(len(value)))
		b = append(b, value...)
	}
	return b
}
func runtimeQuery(t testing.TB) *Bound[cql.Bytes] {
	t.Helper()
	b, err := BindUnpreparedBorrowed(cql.Statement[cql.EmptyParams, cql.Bytes]{Kind: cql.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: cql.EmptyParamsEncoder, Rows: cql.OneColumn(cql.BlobCodec)}, cql.NewColumns(), cql.EmptyParams{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
