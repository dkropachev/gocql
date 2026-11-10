package execution

import (
	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/retry"

	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
)

func runtimeTestSession(t testing.TB, n int) (*cluster.Core, []*transport.Conn, []net.Conn) {
	t.Helper()
	s := func() *cluster.Core {
		opaqueStorageValue := new(cluster.Core)
		implCluster.SessionData(opaqueStorageValue).InternalLogger = logging.InternalNopLogger{}
		return opaqueStorageValue
	}()
	implCluster.SessionData(s).InternalHostSource = &cluster.InternalRingDescriber{InternalHosts: map[string]*endpoint.HostInfo{}}
	implCluster.SessionData(s).InternalPool = &pool.InternalPolicyConnPool{InternalHostConnPools: map[cqltype.UUID]*pool.InternalHostConnPool{}}
	var conns []*transport.Conn
	var peers []net.Conn
	for i := 0; i < n; i++ {
		conn, peer := typedTestConn(t, 4, nil)
		id := cqltype.UUID{15: byte(i + 1)}
		host := func() *endpoint.HostInfo {
			opaqueStorageValuex := new(endpoint.HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValuex).InternalHostId = id
			implEndpoint.HostInfoData(opaqueStorageValuex).InternalState.Store(endpoint.NodeUp)
			return opaqueStorageValuex
		}()
		conn.InternalHost = host
		conn.InternalHooks = s.NewConnectionHooks()
		implCluster.SessionData(s).InternalHostSource.InternalHosts[id.String()] = host
		picker := pool.InternalNewDefaultConnPicker(1)
		picker.Put(conn)
		implCluster.SessionData(s).InternalPool.InternalHostConnPools[id] = &pool.InternalHostConnPool{InternalHost: host, InternalConnPicker: picker}
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
	b = binary.BigEndian.AppendUint16(b, uint16(cqltype.TypeBlob))
	b = binary.BigEndian.AppendUint32(b, uint32(len(values)))
	for _, value := range values {
		b = binary.BigEndian.AppendUint32(b, uint32(len(value)))
		b = append(b, value...)
	}
	return b
}
func runtimeQuery(t testing.TB) *Bound[cqltype.Bytes] {
	t.Helper()
	b, err := BindUnpreparedBorrowed(binding.Statement[codec.EmptyRecord, cqltype.Bytes]{Kind: cqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: codec.EmptyParamsEncoder, Rows: codec.OneColumn(codec.BlobCodec)}, cqltype.NewColumns(), codec.EmptyRecord{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type runtimePolicy struct {
	targets []model.Target
	events  atomic.Int32
}

func (p *runtimePolicy) Validate(*routing.MetadataSnapshot) error { return nil }
func (p *runtimePolicy) Pick(model.RoutingInfo, *routing.MetadataSnapshot) (routing.Route, bool) {
	return routing.Route{}, false
}
func (p *runtimePolicy) Fallback(model.RoutingInfo, *routing.MetadataSnapshot) routing.QueryPlan {
	return &routing.InternalTargetPlan{InternalTargets: append([]model.Target(nil), p.targets...)}
}
func (p *runtimePolicy) Observe(routing.AttemptEvent) { p.events.Add(1) }

type runtimeSpeculation struct{}

func (runtimeSpeculation) Delay(attempt int, _ routing.MetricsSnapshot) (time.Duration, bool) {
	return time.Millisecond, attempt == 1
}
func runtimeSpeculativeLoserWriterKeepsInput(t *testing.T, pageTimeout time.Duration) {
	s, conns, peers := runtimeTestSession(t, 2)
	policy := &runtimePolicy{targets: []model.Target{{HostID: implEndpoint.InternalHostInfoHostUUID(conns[0].InternalHost)}, {HostID: implEndpoint.InternalHostInfoHostUUID(conns[1].InternalHost)}}}
	profile := settings.InternalDefaultProfile()
	profile.Routing = policy
	profile.Speculation = runtimeSpeculation{}
	profile.PageTimeout = pageTimeout
	handle := new(profile)
	go func() {
		stream := typedTestReadRequest(t, conns[1], peers[1])
		_, _ = peers[1].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "winner")))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	bound := runtimeQuery(t)
	page, err := RunPage(ctx, s, bound, model.StartPage(), testOptions(Options{Profile: handle, Idempotent: true, CustomPayload: []model.PayloadEntry{{Key: "borrowed", Value: cqltype.Nullable[cqltype.Bytes]{Valid: true, Value: cqltype.BorrowBytes([]byte("per-call-payload"))}}}}))
	if err != nil {
		t.Fatal(err)
	}
	var row cqltype.Bytes
	if !page.NextInto(&row) || string(row.OwnedBytes()) != "winner" {
		t.Fatal(page.Err())
	}
	bound.Close()
	page.Close()
	short, stop := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer stop()
	if !errors.Is(bound.Wait(short), context.DeadlineExceeded) {
		t.Fatal("Wait released loser while its writer was blocked")
	}
	stream := typedTestReadRequest(t, conns[0], peers[0])
	go func() { _, _ = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "loser"))) }()
	if err = bound.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if policy.events.Load() != 2 {
		t.Fatalf("attempt events %d", policy.events.Load())
	}
}

type runtimeDelayedRetry struct {
	delay     time.Duration
	scheduled chan<- struct{}
}

func (p runtimeDelayedRetry) Decide(e retry.Attempt) retry.Decision {
	if e.Number >= 3 {
		return retry.Decision{}
	}
	if p.scheduled != nil {
		p.scheduled <- struct{}{}
	}
	action := retry.Same
	if e.Number == 2 {
		action = retry.Next
	}
	return retry.Decision{Action: action, Delay: p.delay}
}
