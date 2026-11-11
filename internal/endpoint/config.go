// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

import (
	"context"
	"net"
	"net/netip"
	"sync/atomic"
	"time"
)

type InternalContextBox struct{ InternalCtx context.Context }

// InternalV2DNSResolver applies the configured DNS and setup timeouts to the
// user resolver, or to net.DefaultResolver when none is configured. Lookups
// run on the caller's goroutine; the resolver must honour ctx.
type InternalV2DNSResolver struct {
	InternalBase                          Resolver
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
	var base Resolver = net.DefaultResolver
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
