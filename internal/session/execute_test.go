package session

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/retry"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

func runtimeTestSession(t testing.TB, n int) (*cluster.Core, []*transport.Conn, []net.Conn) {
	t.Helper()
	s := func() *cluster.Core {
		opaqueStorageValue := new(cluster.Core)
		implCluster.SessionData(opaqueStorageValue).InternalLogger = logging.Nop()
		return opaqueStorageValue
	}()
	implCluster.SessionData(s).InternalHostSource = &cluster.InternalRingDescriber{InternalHosts: map[string]*endpoint.HostInfo{}}
	implCluster.SessionData(s).InternalPool = &pool.InternalPolicyConnPool{InternalHostConnPools: map[cql.UUID]*pool.InternalHostConnPool{}}
	var conns []*transport.Conn
	var peers []net.Conn
	for i := 0; i < n; i++ {
		conn, peer := typedTestConn(t, 4, nil)
		id := cql.UUID{15: byte(i + 1)}
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
	b, err := BindUnpreparedBorrowed(binding.Statement[cql.EmptyRow, cql.Bytes]{Kind: cql.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: cql.EmptyParamsEncoder, Rows: cql.OneColumn(cql.BlobCodec)}, cql.NewColumns(), cql.EmptyRow{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// runtimePolicy routes to its targets in order, counting the attempt events
// it observes and keeping them.
type runtimePolicy struct {
	targets []metadata.Target
	events  atomic.Int32
	mu      sync.Mutex
	seen    []routing2.AttemptEvent
}

func (p *runtimePolicy) Validate(*routing.MetadataSnapshot) error { return nil }
func (p *runtimePolicy) Pick(routing2.Info, *routing.MetadataSnapshot) (routing.Route, bool) {
	return routing.Route{}, false
}
func (p *runtimePolicy) Fallback(routing2.Info, *routing.MetadataSnapshot) routing.QueryPlan {
	return &routing.InternalTargetPlan{InternalTargets: append([]metadata.Target(nil), p.targets...)}
}
func (p *runtimePolicy) Observe(e routing2.AttemptEvent) {
	p.events.Add(1)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen = append(p.seen, e)
}

// observed returns the attempt events seen so far.
func (p *runtimePolicy) observed() []routing2.AttemptEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]routing2.AttemptEvent(nil), p.seen...)
}

type runtimeSpeculation struct{}

func (runtimeSpeculation) Delay(attempt int) (time.Duration, bool) {
	return time.Millisecond, attempt == 1
}
func runtimeSpeculativeLoserWriterKeepsInput(t *testing.T, pageTimeout time.Duration) {
	s, conns, peers := runtimeTestSession(t, 2)
	policy := &runtimePolicy{targets: []metadata.Target{{HostID: implEndpoint.InternalHostInfoHostUUID(conns[0].InternalHost)}, {HostID: implEndpoint.InternalHostInfoHostUUID(conns[1].InternalHost)}}}
	profile := settings.InternalDefaultProfile()
	profile.Routing = policy
	profile.Speculation = runtimeSpeculation{}
	profile.PageTimeout = pageTimeout
	handle := new(profile)
	// The winner answers only once the loser's frame header has been read,
	// so the loser's writer is known to be blocked mid-frame. Without this,
	// the winner could answer before the loser's write was accepted, and the
	// loser would be cancelled with nothing to drain.
	var loserHeader [9]byte
	loserStarted := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(peers[0], loserHeader[:])
		loserStarted <- err
		if err != nil {
			t.Error(err)
			return
		}
		stream := typedTestReadRequest(t, conns[1], peers[1])
		_, _ = peers[1].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "winner")))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	bound := runtimeQuery(t)
	page, err := RunPage(ctx, s, bound, cql.StartPage, 0, testOptions(Options{Profile: handle, Idempotent: true, CustomPayload: []cql.PayloadEntry{{Key: "borrowed", Value: cql.Nullable[cql.Bytes]{Valid: true, Value: cql.BorrowBytes([]byte("per-call-payload"))}}}}))
	if err != nil {
		t.Fatal(err)
	}
	var row cql.Bytes
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
	if err := <-loserStarted; err != nil {
		t.Fatal(err)
	}
	if _, err := io.CopyN(io.Discard, peers[0], int64(binary.BigEndian.Uint32(loserHeader[5:]))); err != nil {
		t.Fatal(err)
	}
	stream := int(binary.BigEndian.Uint16(loserHeader[2:4]))
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
