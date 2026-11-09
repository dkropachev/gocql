package execution

import (
	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	implPool "github.com/scylladb/gocql/v2/internal/pool"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

	"encoding/binary"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

type blockingPolicy struct {
	RoutingPolicy
	block            atomic.Bool
	entered, release chan struct{}
}

func (p *blockingPolicy) Validate(*implRouting.MetadataSnapshot) error {
	if p.block.CompareAndSwap(true, false) {
		close(p.entered)
		<-p.release
	}
	return nil
}
func uuidRows(id, version cqltype.UUID) []byte {
	body := binary.BigEndian.AppendUint32(nil, frm.ResultKindRows)
	body = binary.BigEndian.AppendUint32(body, 1)
	body = binary.BigEndian.AppendUint32(body, 2)
	str := func(v string) { body = binary.BigEndian.AppendUint16(body, uint16(len(v))); body = append(body, v...) }
	str("system")
	str("local")
	for _, name := range []string{"host_id", "schema_version"} {
		str(name)
		body = binary.BigEndian.AppendUint16(body, uint16(cqltype.TypeUUID))
	}
	body = binary.BigEndian.AppendUint32(body, 1)
	for _, id := range []cqltype.UUID{id, version} {
		body = binary.BigEndian.AppendUint32(body, 16)
		body = append(body, id[:]...)
	}
	return body
}
func agreementConn(t *testing.T, s *cluster.Core, id, version cqltype.UUID, code int) (*transport.Conn, *atomic.Int32) {
	t.Helper()
	c, server := typedTestConn(t, 4, nil)
	c.InternalHost = func() *endpoint.HostInfo {
		opaqueStorageValuexx := new(endpoint.HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexx).InternalHostId = id
		implEndpoint.HostInfoData(opaqueStorageValuexx).InternalConnectAddress = net.IPv4(127, 0, 0, 1)
		implEndpoint.HostInfoData(opaqueStorageValuexx).InternalPort = 9042
		return opaqueStorageValuexx
	}()
	c.InternalHooks = cluster.InternalNewConnectionHooks(s)
	go c.InternalServe(c.InternalCtx)
	count := &atomic.Int32{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			header := make([]byte, 9)
			if _, err := io.ReadFull(server, header); err != nil {
				return
			}
			n := binary.BigEndian.Uint32(header[5:])
			if _, err := io.CopyN(io.Discard, server, int64(n)); err != nil {
				return
			}
			stream := int(binary.BigEndian.Uint16(header[2:4]))
			count.Add(1)
			response := typedTestResponse(t, 4, stream, uuidRows(id, version))
			if code != 0 {
				response = errorResponse(t, stream, code, nil)
			}
			if _, err := server.Write(response); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { c.Close(); server.Close(); <-done; c.InternalWaitEventDispatch() })
	return c, count
}
func agreementSession(t *testing.T) (*cluster.Core, cqltype.UUID, cqltype.UUID) {
	t.Helper()
	s := func() *cluster.Core {
		opaqueStorageValuexxx := new(cluster.Core)
		implCluster.SessionData(opaqueStorageValuexxx).InternalLogger = logging.InternalNopLogger{}
		return opaqueStorageValuexxx
	}()
	config := settings.DefaultConfig("localhost")
	config.Metadata.AgreementTimeout = 35 * time.Millisecond
	config.Metadata.AgreementPoll = time.Millisecond
	implCluster.InternalSessionConfigureV2Peripheral(s, config)
	implCluster.SessionData(s).InternalPool = &implPool.InternalPolicyConnPool{InternalHostConnPools: make(map[cqltype.UUID]*implPool.InternalHostConnPool)}
	t.Cleanup(func() { s.Close() })
	return s, cqltype.MustRandomUUID(), cqltype.MustRandomUUID()
}
func agreementPool(s *cluster.Core, c *transport.Conn) {
	implCluster.SessionData(s).InternalPool.InternalHostConnPools[implEndpoint.InternalHostInfoHostUUID(c.InternalHost)] = &implPool.InternalHostConnPool{InternalHost: c.InternalHost, InternalConnPicker: &implPool.InternalDefaultConnPicker{InternalConns: []*transport.Conn{c}}}
}

type lifecycleFunc func(settings.LifecycleEvent)

func (f lifecycleFunc) ObserveLifecycle(e settings.LifecycleEvent) { f(e) }

type unstablePolicy struct {
	RoutingPolicy
	session *cluster.Core
}

func (p unstablePolicy) Validate(*implRouting.MetadataSnapshot) error {
	state := implCluster.InternalSessionPeripheral(p.session)
	state.InternalMu.Lock()
	state.InternalPublicationRevision++
	state.InternalMu.Unlock()
	return nil
}
