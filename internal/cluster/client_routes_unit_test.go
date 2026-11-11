package cluster

import (
	"net"
	"testing"

	"github.com/scylladb/gocql/v2/events"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/eventbus"
)

// dnsResolverFunc adapts a function to the DNSResolver interface.
type dnsResolverFunc func(host string) ([]net.IP, error)

func (f dnsResolverFunc) LookupIP(host string) ([]net.IP, error) {
	return f(host)
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

type testHostInfo struct {
	hostID string
}

func (t testHostInfo) HostID() string                     { return t.hostID }
func (t testHostInfo) Rack() string                       { return "" }
func (t testHostInfo) DataCenter() string                 { return "" }
func (t testHostInfo) BroadcastAddress() net.IP           { return nil }
func (t testHostInfo) ListenAddress() net.IP              { return nil }
func (t testHostInfo) RPCAddress() net.IP                 { return nil }
func (t testHostInfo) PreferredIP() net.IP                { return nil }
func (t testHostInfo) Peer() net.IP                       { return nil }
func (t testHostInfo) UntranslatedConnectAddress() net.IP { return nil }
func (t testHostInfo) Port() int                          { return 0 }
func (t testHostInfo) Partitioner() string                { return "" }
func (t testHostInfo) ClusterName() string                { return "" }
func (t testHostInfo) ScyllaShardAwarePort() uint16       { return 0 }
func (t testHostInfo) ScyllaShardAwarePortTLS() uint16    { return 0 }
func (t testHostInfo) ScyllaShardCount() int              { return 0 }

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
