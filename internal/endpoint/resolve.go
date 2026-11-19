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
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/scylladb/gocql/v2/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
)

func InternalResolveInitialEndpoint(resolver endpoint.Resolver, addr string, defaultPort int) ([]*HostInfo, error) {
	var port int
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
		port = defaultPort
	} else {
		port, err = strconv.Atoi(portStr)
		if err != nil {
			return nil, err
		}
	}

	// Check if host is a literal IP address
	if ip := net.ParseIP(host); ip != nil {
		if InternalValidIpAddr(ip) {
			hb := HostInfoBuilder{
				// Fake hosts for initial endpoints do not need HostID
				Hostname:       host,
				ConnectAddress: ip,
				Port:           port,
			}
			hh := hb.Build()
			return []*HostInfo{&hh}, nil
		}
	}

	// Look up host in DNS
	var addrs []netip.Addr
	var setupDeadline time.Time
	if typed, ok := resolver.(*InternalV2DNSResolver); ok {
		addrs, setupDeadline, err = typed.lookupSetup(host)
	} else {
		addrs, err = resolver.LookupNetIP(context.Background(), "ip", host)
	}
	if err != nil {
		return nil, err
	} else if len(addrs) == 0 {
		return nil, fmt.Errorf("no IP's returned from DNS lookup for %q", addr)
	}

	var hosts []*HostInfo
	for _, a := range addrs {
		if ip := net.IP(a.Unmap().AsSlice()); InternalValidIpAddr(ip) {
			hb := HostInfoBuilder{
				// Fake hosts for initial endpoints do not need HostID
				Hostname:       host,
				ConnectAddress: ip,
				Port:           port,
			}
			hh := hb.Build()
			hh.privateStorage.InternalInitialSetupDeadline = setupDeadline
			hosts = append(hosts, &hh)
		}
	}

	if len(hosts) == 0 {
		return nil, fmt.Errorf("no IP's founded for %q", addr)
	}
	return hosts, nil
}

// InternalResolveInitialEndpoints resolves the contact points addrs, skipping
// the ones that do not resolve; it fails only when none does.
func InternalResolveInitialEndpoints(resolver endpoint.Resolver, addrs []string, defaultPort int, logger *slog.Logger) ([]*HostInfo, error) {
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

type InternalContextBox struct{ InternalCtx context.Context }

// InternalV2DNSResolver applies the configured DNS and setup timeouts to the
// user resolver, or to net.DefaultResolver when none is configured. Lookups
// run on the caller's goroutine; the resolver must honour ctx.
type InternalV2DNSResolver struct {
	InternalBase                          endpoint.Resolver
	InternalStartup                       atomic.Pointer[InternalContextBox]
	InternalTimeout, InternalSetupTimeout time.Duration
}

func (r *InternalV2DNSResolver) baseContext() context.Context {
	if box := r.InternalStartup.Load(); box != nil {
		return box.InternalCtx
	}
	return context.Background()
}

// LookupNetIP resolves host within ctx, bounded by the DNS timeout.
func (r *InternalV2DNSResolver) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	if r.InternalTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.InternalTimeout)
		defer cancel()
	}
	var base endpoint.Resolver = net.DefaultResolver
	if r.InternalBase != nil {
		base = r.InternalBase
	}
	return base.LookupNetIP(ctx, network, host)
}

// lookupSetup resolves a contact point under the session startup context and
// returns the setup deadline the resulting connection attempt must inherit.
func (r *InternalV2DNSResolver) lookupSetup(host string) ([]netip.Addr, time.Time, error) {
	ctx := r.baseContext()
	cancel := func() {}
	if r.InternalSetupTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, r.InternalSetupTimeout)
	}
	defer cancel()
	deadline, _ := ctx.Deadline()
	addrs, err := r.LookupNetIP(ctx, "ip", host)
	return addrs, deadline, err
}

func (r *InternalV2DNSResolver) InternalClearStartup() { r.InternalStartup.Store(nil) }
