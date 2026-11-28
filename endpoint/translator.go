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
	// Translate returns the address to dial for addr, the kind endpoint of
	// peer. Return AddressOf(addr) when no translation applies. ctx bounds
	// the connection setup.
	//
	// The driver calls it on every dial, for the one endpoint that dial
	// opens: the CQL port, the TLS port (WithTLSPort), or a shard-aware port
	// when the connection picks its shard by port. An error fails the dial of
	// a CQL or TLS port like a dial error; an error for a shard-aware port
	// makes the dial fall back to the node's CQL or TLS port, as a refused
	// shard-aware port does.
	Translate(ctx context.Context, peer Peer, addr netip.AddrPort, kind PortKind) (Address, error)
}

// TranslatorFunc adapts a function to Translator.
type TranslatorFunc func(ctx context.Context, peer Peer, addr netip.AddrPort, kind PortKind) (Address, error)

// Translate calls fn.
func (fn TranslatorFunc) Translate(ctx context.Context, peer Peer, addr netip.AddrPort, kind PortKind) (Address, error) {
	return fn(ctx, peer, addr, kind)
}

// PortKind is which of a node's endpoints a Translator is asked for.
type PortKind uint8

const (
	// PortCQL is the node's CQL port, Peer.Port, dialed in plaintext or,
	// without WithTLSPort, over TLS.
	PortCQL PortKind = iota + 1
	// PortTLS is the session's TLS port (WithTLSPort), Peer.TLSPort.
	PortTLS
	// PortShardAware is the node's plaintext ScyllaDB shard-aware port,
	// Peer.ShardAwarePort.
	PortShardAware
	// PortShardAwareTLS is the node's TLS ScyllaDB shard-aware port,
	// Peer.ShardAwarePortTLS.
	PortShardAwareTLS
)

// String returns the kind's name, such as "cql" or "shard-aware-tls".
func (k PortKind) String() string {
	switch k {
	case PortCQL:
		return "cql"
	case PortTLS:
		return "tls"
	case PortShardAware:
		return "shard-aware"
	case PortShardAwareTLS:
		return "shard-aware-tls"
	}
	return "PortKind(" + strconv.Itoa(int(k)) + ")"
}

// Address is where a Translator sends a connection: a host, which is an IP
// address or a DNS name, and a port.
//
// The driver resolves a DNS name once on every dial, through the session's
// resolver (config.ConfigBuilder.WithResolver) and within its DNS timeout
// (WithDNSTimeout), as it resolves client route host names, and dials the
// first address it gets; while the name still resolves to the address a
// node's connections were dialed on, new connections keep that address. A
// DNS name returned for the endpoint a TLS connection dials is also the
// default name it sends and verifies, in place of the node's IP address, as
// a contact point's name is.
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
	// Port is the node's CQL port. TLSPort is the port TLS connections dial
	// (WithTLSPort), zero when the session sets none. ShardAwarePort and
	// ShardAwarePortTLS are the node's ScyllaDB shard-aware ports, zero when
	// not advertised. Translate's PortKind says which one it is asked for.
	Port, TLSPort, ShardAwarePort, ShardAwarePortTLS uint16
}
