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

// InternalTranslateAddressPort applies addressTranslator to addr of peer. A nil
// translator or an unusable addr returns addr unchanged.
func InternalTranslateAddressPort(ctx context.Context, addressTranslator endpoint.Translator, peer endpoint.Peer, addr netip.AddrPort, logger *slog.Logger) (netip.AddrPort, error) {
	logger = logging.Or(logger)
	if addressTranslator == nil || !InternalUsableAddrPort(addr) {
		return addr, nil
	}
	newAddr, err := addressTranslator.Translate(ctx, peer, addr)
	if err != nil {
		logger.Debug("address translation failed", "addr", addr.String(), logging.Err(err))
		return addr, err
	}
	if newAddr.Port() == 0 {
		return addr, fmt.Errorf("invalid translated port 0 for %q: port must be between 1 and 65535", addr)
	}
	if !newAddr.Addr().IsValid() || newAddr.Addr().IsUnspecified() {
		return addr, fmt.Errorf("invalid translated address %q for %q", newAddr.Addr(), addr)
	}
	newAddr = netip.AddrPortFrom(newAddr.Addr().Unmap(), newAddr.Port())
	logger.Debug("translated address", "addr", addr.String(), "translated", newAddr.String())
	return newAddr, nil
}
