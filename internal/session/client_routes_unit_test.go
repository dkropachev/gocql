package session

import (
	"context"
	"net"
	"net/netip"

	"github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/endpoint"
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
func (f *fakeControlConn) InternalConnect(hosts []*endpoint.HostInfo) error { return nil }
func (f *fakeControlConn) InternalClose()                                   {}
func (f *fakeControlConn) InternalGetSession() *cluster.Core                { return nil }
func (f *fakeControlConn) InternalReconnect() error                         { return nil }
