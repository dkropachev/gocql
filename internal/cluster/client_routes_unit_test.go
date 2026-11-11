package cluster

import (
	"context"
	"net"
	"net/netip"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/eventbus"
	"github.com/scylladb/gocql/v2/internal/events"
)

// dnsResolverFunc adapts a function to the Resolver interface.
type dnsResolverFunc func(host string) ([]net.IP, error)

func (f dnsResolverFunc) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	ips, err := f(host)
	return netIPAddrs(ips), err
}

// netIPAddrs converts test fixtures written as net.IP to resolver results.
func netIPAddrs(ips []net.IP) []netip.Addr {
	var out []netip.Addr
	for _, ip := range ips {
		if a, ok := netip.AddrFromSlice(ip); ok {
			out = append(out, a.Unmap())
		}
	}
	return out
}

type fakeControlConn struct {
	statement string
	values    []any
	iter      *nativeResultFixture
}

func (f *fakeControlConn) InternalGetConn() *InternalConnHost  { return nil }
func (f *fakeControlConn) InternalAwaitSchemaAgreement() error { return nil }
func (f *fakeControlConn) query(statement string, values ...any) *nativeResultFixture {
	f.statement = statement
	f.values = values
	if f.iter != nil {
		return f.iter
	}
	return &nativeResultFixture{}
}
func (f *fakeControlConn) querySystem(statement string, values ...any) *nativeResultFixture {
	return &nativeResultFixture{}
}
func (f *fakeControlConn) InternalConnect(hosts []*endpoint.HostInfo) error { return nil }
func (f *fakeControlConn) InternalClose()                                   {}
func (f *fakeControlConn) InternalGetSession() *Core                        { return nil }
func (f *fakeControlConn) InternalReconnect() error                         { return nil }

func newClientRoutesEventHarness(t *testing.T, allowedConnectionIDs []string) (*ClientRoutesHandler, *eventbus.EventBus[events.Event]) {
	t.Helper()

	bus := eventbus.New[events.Event](eventbus.EventBusConfig{InputEventsQueueSize: 1}, nil)
	if err := bus.Start(); err != nil {
		t.Fatalf("starting event bus: %v", err)
	}

	h := func() *ClientRoutesHandler {
		opaqueStorageValue := new(ClientRoutesHandler)
		ClientRoutesHandlerData(opaqueStorageValue).sub = bus.Subscribe("port-mux", 1, nil)
		ClientRoutesHandlerData(opaqueStorageValue).InternalUpdateTasks = make(chan InternalUpdateTask, 1)
		ClientRoutesHandlerData(opaqueStorageValue).InternalCloseChan = make(chan struct{})
		return opaqueStorageValue
	}()

	clientRoutesHandlerStartReadingEvents(h, allowedConnectionIDs)

	t.Cleanup(func() {
		if err := bus.Stop(); err != nil {
			t.Fatalf("stopping event bus: %v", err)
		}
	})

	return h, bus
}

// Host IDs used by client route translation tests.
const (
	hostH1      = "a0000000-0000-0000-0000-000000000001"
	hostH2      = "a0000000-0000-0000-0000-000000000002"
	hostH3      = "a0000000-0000-0000-0000-000000000003"
	hostMissing = "a0000000-0000-0000-0000-0000000000ff"
)

// routePeer returns a peer with hostID, or with no host ID when it is empty.
func routePeer(hostID string) endpoint.Peer {
	if hostID == "" {
		return endpoint.Peer{}
	}
	return endpoint.Peer{HostID: cqltype.MustParseUUID(hostID)}
}

// sameIP reports whether addr carries ip.
func sameIP(addr netip.AddrPort, ip net.IP) bool {
	return addr.Addr() == endpoint.InternalNetIPAddr(ip)
}

func newCacheEntry(routes ...InternalClientRoute) clientRouteCacheEntry {
	return clientRouteCacheEntry{allRoutes: routes}
}

func newCache(routes map[string]clientRouteCacheEntry) clientRouteCache {
	return clientRouteCache{routes: routes}
}

func newCacheEntryWithCurrent(current InternalClientRoute, routes ...InternalClientRoute) clientRouteCacheEntry {
	entry := clientRouteCacheEntry{allRoutes: routes}
	entry.BindCurrent(current.InternalConnectionID)
	return entry
}
