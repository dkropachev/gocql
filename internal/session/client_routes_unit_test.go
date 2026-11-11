package session

import (
	"net"

	"github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/endpoint"
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

func (f *fakeControlConn) InternalGetConn() *cluster.InternalConnHost { return nil }
func (f *fakeControlConn) InternalAwaitSchemaAgreement() error        { return nil }
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
func (f *fakeControlConn) InternalDiscoverProtocol(hosts []*endpoint.HostInfo) (int, error) {
	return 0, nil
}
func (f *fakeControlConn) InternalConnect(hosts []*endpoint.HostInfo) error { return nil }
func (f *fakeControlConn) InternalClose()                                   {}
func (f *fakeControlConn) InternalGetSession() *cluster.Core                { return nil }
func (f *fakeControlConn) InternalReconnect() error                         { return nil }
