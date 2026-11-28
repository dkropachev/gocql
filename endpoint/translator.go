// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

import (
	"context"
	"net"
	"net/netip"
	"strconv"

	"github.com/scylladb/gocql/v2/cql"
)

// Translator translates node addresses (and ports) that are discovered
// or received as a node event, for example public IPs to private IPs in EC2,
// or every node to the DNS name of a load balancer in front of it.
type Translator interface {
	// Translate returns the address to dial for addr, which belongs to
	// peer. Return AddressOf(addr) when no translation applies. ctx bounds
	// the connection setup. An error fails that connection attempt like a
	// dial error.
	Translate(ctx context.Context, peer Peer, addr netip.AddrPort) (Address, error)
}

// TranslatorFunc adapts a function to Translator.
type TranslatorFunc func(ctx context.Context, peer Peer, addr netip.AddrPort) (Address, error)

// Translate calls fn.
func (fn TranslatorFunc) Translate(ctx context.Context, peer Peer, addr netip.AddrPort) (Address, error) {
	return fn(ctx, peer, addr)
}

// Address is where a Translator sends a connection: a host, which is an IP
// address or a DNS name, and a port.
//
// The driver resolves a DNS name on every dial, through the session's
// resolver (config.ConfigBuilder.WithResolver) and within its DNS timeout
// (WithDNSTimeout), as it resolves client route host names, and dials the
// first address it gets; while the name still resolves to the address a
// node's connections were dialed on, new connections keep that address. A
// DNS name is also the default name a TLS connection sends and verifies, in
// place of the node's IP address, as a contact point's name is.
//
// The zero Address has no host, and the driver fails the connection attempt
// that a Translator returns it for.
type Address struct {
	// Host is an IP address, such as "10.0.0.1" or "fd00::1", or a DNS name.
	// An IPv4-mapped IPv6 address is dialed as IPv4.
	Host string
	// Port is the port to dial, 1 to 65535.
	Port uint16
}

// AddressOf returns addr as an Address, for a Translator that leaves an
// address unchanged.
func AddressOf(addr netip.AddrPort) Address {
	return Address{Host: addr.Addr().String(), Port: addr.Port()}
}

// String returns host:port, with an IPv6 host in brackets.
func (a Address) String() string {
	return net.JoinHostPort(a.Host, strconv.Itoa(int(a.Port)))
}

// Peer is what a node advertises about itself in system.local or
// system.peers, as known when its address is translated. Zero fields are
// unknown; a contact point that has not been discovered yet has no HostID.
type Peer struct {
	// RPCAddress, BroadcastAddress, ListenAddress and PreferredIP are the
	// addresses the node advertises.
	RPCAddress, BroadcastAddress, ListenAddress, PreferredIP netip.Addr
	// Datacenter and Rack are the node's location.
	Datacenter, Rack string
	// HostID is the node's host ID, or the zero UUID when not yet known.
	HostID cql.UUID
	// Port is the node's CQL port. ShardAwarePort and ShardAwarePortTLS are
	// its ScyllaDB shard-aware ports, zero when not advertised. Compare the
	// port being translated with them to tell the endpoints apart.
	Port, ShardAwarePort, ShardAwarePortTLS uint16
}
