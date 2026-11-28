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
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"strings"

	"github.com/scylladb/gocql/v2/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
)

// InternalPeerOf snapshots the translator-facing view of h.
func InternalPeerOf(h *HostInfo) endpoint.Peer {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	s := &h.privateStorage
	return endpoint.Peer{
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

// InternalTranslateAddressPort applies addressTranslator to addr, the kind
// endpoint of peer, and returns the address to dial and, when the translator
// named a DNS host, that name. A nil translator or an unusable addr returns
// addr unchanged. A DNS name is resolved through resolver,
// net.DefaultResolver when nil; prefer, an address the node was dialed on
// before, is kept while the name still resolves to it, and the first usable
// address is taken otherwise.
func InternalTranslateAddressPort(ctx context.Context, addressTranslator endpoint.Translator, resolver endpoint.Resolver, peer endpoint.Peer, addr, prefer netip.AddrPort, kind endpoint.PortKind, logger *slog.Logger) (netip.AddrPort, string, error) {
	logger = logging.Or(logger)
	if addressTranslator == nil || !InternalUsableAddrPort(addr) {
		return addr, "", nil
	}
	translated, err := addressTranslator.Translate(ctx, peer, addr, kind)
	if err != nil {
		logger.Debug("address translation failed", "addr", addr.String(), "port_kind", kind.String(), logging.Err(err))
		return addr, "", err
	}
	if translated.Port == 0 {
		return addr, "", fmt.Errorf("invalid translated port 0 for %q: port must be between 1 and 65535", addr)
	}
	host := strings.TrimSuffix(strings.TrimPrefix(translated.Host, "["), "]")
	if host == "" {
		return addr, "", fmt.Errorf("invalid translated address for %q: no host", addr)
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if ip.IsUnspecified() {
			return addr, "", fmt.Errorf("invalid translated address %q for %q", ip, addr)
		}
		newAddr := netip.AddrPortFrom(ip.Unmap(), translated.Port)
		logger.Debug("translated address", "addr", addr.String(), "translated", newAddr.String())
		return newAddr, "", nil
	}
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	ips, err := resolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return addr, "", fmt.Errorf("resolve translated host %q for %q: %w", host, addr, err)
	}
	var chosen netip.AddrPort
	for _, ip := range ips {
		if ip = ip.Unmap(); !ip.IsValid() || ip.IsUnspecified() {
			continue
		}
		candidate := netip.AddrPortFrom(ip, translated.Port)
		if !chosen.IsValid() || candidate == prefer {
			chosen = candidate
		}
		if candidate == prefer {
			break
		}
	}
	if !chosen.IsValid() {
		return addr, "", fmt.Errorf("translated host %q for %q resolved to no usable address", host, addr)
	}
	logger.Debug("translated address", "addr", addr.String(), "translated", translated.String(), "resolved", chosen.String())
	return chosen, host, nil
}

// InternalDialTranslation translates the endpoints that one dial of one host
// opens. It is made for each dial, so a DNS name is resolved at most once
// per dial whichever endpoints the translator sends to it.
type InternalDialTranslation struct {
	Translator endpoint.Translator
	Resolver   endpoint.Resolver
	Logger     *slog.Logger
	resolved   map[string]resolvedName
}

type resolvedName struct {
	err   error
	addrs []netip.Addr
}

// LookupNetIP resolves host through t's resolver once per dial.
func (t *InternalDialTranslation) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	key := network + "/" + host
	if r, ok := t.resolved[key]; ok {
		return r.addrs, r.err
	}
	resolver := t.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	addrs, err := resolver.LookupNetIP(ctx, network, host)
	if t.resolved == nil {
		t.resolved = make(map[string]resolvedName, 1)
	}
	t.resolved[key] = resolvedName{addrs: addrs, err: err}
	return addrs, err
}

type dialTranslationKey struct{}

// InternalWithDialTranslation returns ctx carrying t, which the host dialers
// use to translate the endpoints they dial.
func InternalWithDialTranslation(ctx context.Context, t *InternalDialTranslation) context.Context {
	return context.WithValue(ctx, dialTranslationKey{}, t)
}

func dialTranslationFrom(ctx context.Context) *InternalDialTranslation {
	t, _ := ctx.Value(dialTranslationKey{}).(*InternalDialTranslation)
	return t
}

// InternalDialEndpoint returns the address to dial for the kind endpoint of
// host and, when the address translator named a DNS host for it, that name.
// tlsPort is the session's TLS port, for PortTLS. A shard-aware kind the node
// does not advertise returns the zero AddrPort.
//
// With a translation in ctx (InternalWithDialTranslation), only that endpoint
// is translated, and its address is stored on host; when the stored address
// changes, the host's transport generation moves on, and *epoch with it when
// it was current, so the connection being dialed is not taken as superseded.
// Without one, the address stored by an earlier translation is used, or the
// untranslated one.
func InternalDialEndpoint(ctx context.Context, host *HostInfo, kind endpoint.PortKind, tlsPort uint16, epoch *uint64) (netip.AddrPort, string, error) {
	h := &host.privateStorage
	h.InternalMu.RLock()
	var port uint16
	switch kind {
	case endpoint.PortCQL:
		if h.InternalPort < 1 || h.InternalPort > 65535 {
			h.InternalMu.RUnlock()
			return netip.AddrPort{}, "", fmt.Errorf("invalid CQL port %d for host %s: port must be between 1 and 65535", h.InternalPort, h.InternalConnectAddress)
		}
		port = uint16(h.InternalPort)
	case endpoint.PortTLS:
		port = tlsPort
	case endpoint.PortShardAware:
		port = h.InternalScyllaFeatures.ShardAwarePort()
	case endpoint.PortShardAwareTLS:
		port = h.InternalScyllaFeatures.ShardAwarePortTLS()
	}
	var stored netip.AddrPort
	var storedName string
	if current := h.InternalTranslatedAddresses; current != nil {
		stored = current.field(kind)
		storedName = current.TLSName
	}
	t := dialTranslationFrom(ctx)
	shardAware := kind == endpoint.PortShardAware || kind == endpoint.PortShardAwareTLS
	if t == nil {
		// No translation for this dial: the stored translation, else the
		// node's address; a shard-aware port is dialed only once translated.
		connectIP, _ := hostInfoConnectAddressLocked(host)
		h.InternalMu.RUnlock()
		switch {
		case InternalUsableAddrPort(stored):
			return stored, storedName, nil
		case port == 0 || shardAware:
			return netip.AddrPort{}, "", nil
		}
		return netip.AddrPortFrom(InternalNetIPAddr(connectIP), port), storedName, nil
	}
	connectIP, _ := untranslatedConnectAddressLocked(host)
	h.InternalMu.RUnlock()
	if port == 0 {
		return netip.AddrPort{}, "", nil
	}
	addr := netip.AddrPortFrom(InternalNetIPAddr(connectIP), port)
	peer := InternalPeerOf(host)
	peer.TLSPort = tlsPort
	translated, name, err := InternalTranslateAddressPort(ctx, t.Translator, t, peer, addr, stored, kind, t.Logger)
	if err != nil {
		return netip.AddrPort{}, "", err
	}
	internalStoreTranslated(host, kind, translated, name, tlsPort, epoch)
	return translated, name, nil
}

func (h InternalTranslatedAddresses) field(kind endpoint.PortKind) netip.AddrPort {
	switch kind {
	case endpoint.PortCQL:
		return h.CQL
	case endpoint.PortTLS:
		return h.CQLTLS
	case endpoint.PortShardAware:
		return h.ShardAware
	case endpoint.PortShardAwareTLS:
		return h.ShardAwareTLS
	}
	return netip.AddrPort{}
}

// internalStoreTranslated records the translation of host's kind endpoint.
// Filling an endpoint not translated before keeps the transport generation,
// since no connection was dialed on another address; changing one moves it.
func internalStoreTranslated(host *HostInfo, kind endpoint.PortKind, addr netip.AddrPort, name string, tlsPort uint16, epoch *uint64) {
	h := &host.privateStorage
	h.InternalMu.Lock()
	defer h.InternalMu.Unlock()
	var next InternalTranslatedAddresses
	if h.InternalTranslatedAddresses != nil {
		next = *h.InternalTranslatedAddresses
	}
	previous := next.field(kind)
	switch kind {
	case endpoint.PortCQL:
		next.CQL = addr
	case endpoint.PortTLS:
		next.CQLTLS = addr
	case endpoint.PortShardAware:
		next.ShardAware = addr
	case endpoint.PortShardAwareTLS:
		next.ShardAwareTLS = addr
	}
	if kind == endpoint.PortTLS || kind == endpoint.PortCQL && tlsPort == 0 {
		next.TLSName = name
	}
	if h.InternalTranslatedAddresses != nil && next.Equal(h.InternalTranslatedAddresses) {
		return
	}
	if InternalUsableAddrPort(previous) && previous != addr {
		if epoch != nil && *epoch == h.InternalTransportGeneration {
			*epoch++
		}
		h.InternalTransportGeneration++
	}
	h.InternalTranslatedAddresses = &next
}
