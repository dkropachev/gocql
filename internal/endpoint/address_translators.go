/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package endpoint

import (
	"context"
	"net"
	"net/netip"

	"github.com/scylladb/gocql/v2/internal/cqltype"
)

// AddressTranslator translates node addresses (and ports) that are discovered
// or received as a node event, for example public IPs to private IPs in EC2.
type AddressTranslator interface {
	// Translate returns the address and port to dial for addr, which belongs
	// to peer. Return addr unchanged when no translation applies. ctx bounds
	// the connection setup. An error fails that connection attempt like a
	// dial error.
	Translate(ctx context.Context, peer Peer, addr netip.AddrPort) (netip.AddrPort, error)
}

// AddressTranslatorFunc adapts a function to AddressTranslator.
type AddressTranslatorFunc func(ctx context.Context, peer Peer, addr netip.AddrPort) (netip.AddrPort, error)

func (fn AddressTranslatorFunc) Translate(ctx context.Context, peer Peer, addr netip.AddrPort) (netip.AddrPort, error) {
	return fn(ctx, peer, addr)
}

// IdentityTranslator returns a translator that leaves addresses unchanged.
func IdentityTranslator() AddressTranslator {
	return AddressTranslatorFunc(func(_ context.Context, _ Peer, addr netip.AddrPort) (netip.AddrPort, error) {
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
	HostID                                                   cqltype.UUID
	// Port is the node's CQL port. ShardAwarePort and ShardAwarePortTLS are
	// its ScyllaDB shard-aware ports, zero when not advertised. Compare the
	// port being translated with them to tell the endpoints apart.
	Port, ShardAwarePort, ShardAwarePortTLS uint16
}

// InternalPeerOf snapshots the translator-facing view of h.
func InternalPeerOf(h *HostInfo) Peer {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	s := &h.privateStorage
	return Peer{
		HostID:            s.InternalHostId,
		Datacenter:        s.InternalDataCenter,
		Rack:              s.InternalRack,
		RPCAddress:        InternalNetIPAddr(s.InternalRpcAddress),
		BroadcastAddress:  InternalNetIPAddr(s.InternalBroadcastAddress),
		ListenAddress:     InternalNetIPAddr(s.InternalListenAddress),
		PreferredIP:       InternalNetIPAddr(s.InternalPreferredIP),
		Port:              uint16(s.InternalPort),
		ShardAwarePort:    s.InternalScyllaFeatures.ShardAwarePort(),
		ShardAwarePortTLS: s.InternalScyllaFeatures.ShardAwarePortTLS(),
	}
}

// InternalNetIPAddr converts ip, unmapping IPv4-in-IPv6. Nil is the zero Addr.
func InternalNetIPAddr(ip net.IP) netip.Addr {
	addr, _ := netip.AddrFromSlice(ip)
	return addr.Unmap()
}

// InternalUsableAddrPort reports whether addr names a dialable endpoint.
func InternalUsableAddrPort(addr netip.AddrPort) bool {
	return addr.Addr().IsValid() && !addr.Addr().IsUnspecified() && addr.Port() != 0
}
