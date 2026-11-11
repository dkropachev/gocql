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
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package endpoint

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"

	"log/slog"

	"github.com/scylladb/gocql/v2/internal/logging"
)

// InternalTranslateAddressPort applies addressTranslator to addr of peer. A nil
// translator or an unusable addr returns addr unchanged.
func InternalTranslateAddressPort(ctx context.Context, addressTranslator AddressTranslator, peer Peer, addr netip.AddrPort, logger *slog.Logger) (netip.AddrPort, error) {
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

func InternalResolveInitialEndpoints(resolver Resolver, addrs []string, defaultPort int, logger *slog.Logger) ([]*HostInfo, error) {
	logger = logging.Or(logger)
	var hosts []*HostInfo
	var errs []error
	for _, hostaddr := range addrs {
		resolvedHosts, err := InternalResolveInitialEndpoint(resolver, hostaddr, defaultPort)
		if err != nil {
			err = fmt.Errorf("failed to resolve endpoint %q: %w", hostaddr, err)
			errs = append(errs, err)
			logger.Warn("contact point did not resolve", "contact", hostaddr, logging.Err(err))
			continue
		}
		hosts = append(hosts, resolvedHosts...)
	}
	if len(hosts) == 0 {
		return nil, fmt.Errorf("failed to resolve any of the provided hostnames: %w", errors.Join(errs...))
	}
	return hosts, nil
}

type HostInformation interface {
	Peer() net.IP
	ConnectAddress() net.IP
	UntranslatedConnectAddress() net.IP
	BroadcastAddress() net.IP
	ListenAddress() net.IP
	RPCAddress() net.IP
	PreferredIP() net.IP
	DataCenter() string
	Rack() string
	HostID() string
	WorkLoad() string
	Partitioner() string
	ClusterName() string
	Tokens() []string
	Port() int
	IsUp() bool
	ScyllaShardAwarePort() uint16
	ScyllaShardAwarePortTLS() uint16
	ScyllaShardCount() int
}
