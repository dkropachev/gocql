// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

import (
	"context"
	"net/netip"

	"github.com/scylladb/gocql/v2/cql"
)

// Translator translates node addresses (and ports) that are discovered
// or received as a node event, for example public IPs to private IPs in EC2.
type Translator interface {
	// Translate returns the address and port to dial for addr, which belongs
	// to peer. Return addr unchanged when no translation applies. ctx bounds
	// the connection setup. An error fails that connection attempt like a
	// dial error.
	Translate(ctx context.Context, peer Peer, addr netip.AddrPort) (netip.AddrPort, error)
}

// TranslatorFunc adapts a function to Translator.
type TranslatorFunc func(ctx context.Context, peer Peer, addr netip.AddrPort) (netip.AddrPort, error)

// Translate calls fn.
func (fn TranslatorFunc) Translate(ctx context.Context, peer Peer, addr netip.AddrPort) (netip.AddrPort, error) {
	return fn(ctx, peer, addr)
}

// IdentityTranslator returns a translator that leaves addresses unchanged.
func IdentityTranslator() Translator {
	return TranslatorFunc(func(_ context.Context, _ Peer, addr netip.AddrPort) (netip.AddrPort, error) {
		return addr, nil
	})
}

// Peer is what a node advertises about itself in system.local or
// system.peers, as known when its address is translated. Zero fields are
// unknown; a contact point that has not been discovered yet has no HostID.
type Peer struct {
	// RPCAddress, BroadcastAddress, ListenAddress and PreferredIP are the
	// addresses the node advertises.
	RPCAddress, BroadcastAddress, ListenAddress, PreferredIP netip.Addr
	Datacenter, Rack                                         string
	HostID                                                   cql.UUID
	// Port is the node's CQL port. ShardAwarePort and ShardAwarePortTLS are
	// its ScyllaDB shard-aware ports, zero when not advertised. Compare the
	// port being translated with them to tell the endpoints apart.
	Port, ShardAwarePort, ShardAwarePortTLS uint16
}
