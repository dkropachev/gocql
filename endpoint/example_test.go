// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint_test

import (
	"context"
	"log"
	"net"
	"net/netip"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/endpoint"
)

// preferIPv4 returns only the IPv4 addresses of a name when it has any, and
// all of its addresses otherwise. Pass "ip4" as the network to the wrapped
// resolver instead when IPv6 addresses must never be used.
type preferIPv4 struct{ *net.Resolver }

func (r preferIPv4) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	addrs, err := r.Resolver.LookupNetIP(ctx, network, host)
	if err != nil {
		return nil, err
	}
	var v4 []netip.Addr
	for _, addr := range addrs {
		if addr = addr.Unmap(); addr.Is4() {
			v4 = append(v4, addr)
		}
	}
	if len(v4) == 0 {
		return addrs, nil
	}
	return v4, nil
}

// ExampleResolver wraps net.DefaultResolver to prefer IPv4 addresses for
// contact points and client route host names.
func ExampleResolver() {
	config, err := gocql.NewConfig("db.example.com:9042").
		WithResolver(preferIPv4{net.DefaultResolver}).
		Build()
	if err != nil {
		log.Fatal(err)
	}
	_ = config
}

// ExampleTranslator dials nodes through their private addresses, kept in a
// table keyed by the address each node advertises. Ports are preserved, so
// CQL and shard-aware endpoints stay distinct.
func ExampleTranslator() {
	private := map[netip.Addr]netip.Addr{
		netip.MustParseAddr("203.0.113.10"): netip.MustParseAddr("10.0.0.10"),
		netip.MustParseAddr("203.0.113.11"): netip.MustParseAddr("10.0.0.11"),
	}
	translator := endpoint.TranslatorFunc(func(_ context.Context, _ endpoint.Peer, addr netip.AddrPort) (netip.AddrPort, error) {
		if ip, ok := private[addr.Addr()]; ok {
			return netip.AddrPortFrom(ip, addr.Port()), nil
		}
		return addr, nil
	})
	config, err := gocql.NewConfig("10.0.0.10:9042").
		WithAddressTranslator(translator).
		Build()
	if err != nil {
		log.Fatal(err)
	}
	_ = config
}
