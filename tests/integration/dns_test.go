//go:build integration

package integration

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
)

type mockDNSResolver struct {
	lock sync.RWMutex
	data map[string][]net.IP
}

func newMockDNSResolver() *mockDNSResolver {
	return &mockDNSResolver{
		data: make(map[string][]net.IP),
		lock: sync.RWMutex{},
	}
}

func (r *mockDNSResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	ips := r.data[host]
	if len(ips) == 0 {
		return nil, &net.DNSError{Err: errors.New("no IP addresses").Error(), Name: host}
	}
	addrs := make([]netip.Addr, 0, len(ips))
	for _, ip := range ips {
		addr, _ := netip.AddrFromSlice(ip)
		addrs = append(addrs, addr.Unmap())
	}
	return addrs, nil
}

func (r *mockDNSResolver) Update(host string, ips ...net.IP) {
	r.lock.Lock()
	r.data[host] = ips
	defer r.lock.Unlock()
}

func (r *mockDNSResolver) Delete(hosts ...string) {
	r.lock.Lock()
	for _, host := range hosts {
		delete(r.data, host)
	}
	defer r.lock.Unlock()
}

func MustIP(ip string) net.IP {
	out := net.ParseIP(ip)
	if out == nil {
		panic("failed to parse IP: " + ip)
	}
	return out
}
