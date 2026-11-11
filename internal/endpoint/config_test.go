// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

import (
	"context"
	"errors"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"
)

type resolverFunc func(ctx context.Context, network, host string) ([]netip.Addr, error)

func (f resolverFunc) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return f(ctx, network, host)
}

func TestV2DNSResolver(t *testing.T) {
	// The DNS timeout reaches the resolver through ctx and the lookup runs on
	// the caller's goroutine, so nothing outlives a timed-out lookup.
	t.Run("TimeoutWithoutLeak", func(t *testing.T) {
		var returned atomic.Bool
		r := &InternalV2DNSResolver{InternalTimeout: time.Millisecond, InternalBase: resolverFunc(func(ctx context.Context, _, _ string) ([]netip.Addr, error) {
			defer returned.Store(true)
			<-ctx.Done()
			return nil, ctx.Err()
		})}
		_, err := r.LookupNetIP(context.Background(), "ip", "slow.test")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		if !returned.Load() {
			t.Fatal("lookup returned before the resolver did")
		}
	})

	t.Run("SetupLookup", func(t *testing.T) {
		want := netip.MustParseAddr("127.0.0.1")
		var network string
		r := &InternalV2DNSResolver{InternalSetupTimeout: time.Minute, InternalBase: resolverFunc(func(ctx context.Context, n, _ string) ([]netip.Addr, error) {
			network = n
			if _, ok := ctx.Deadline(); !ok {
				t.Error("setup lookup has no deadline")
			}
			return []netip.Addr{want}, nil
		})}
		hosts, err := InternalResolveInitialEndpoint(r, "node.test:9142", 9042)
		if err != nil {
			t.Fatal(err)
		}
		if network != "ip" || len(hosts) != 1 || !hosts[0].ConnectAddress().Equal(want.AsSlice()) || hosts[0].Port() != 9142 {
			t.Fatal(network, hosts)
		}
		if hosts[0].privateStorage.InternalInitialSetupDeadline.IsZero() {
			t.Fatal("setup deadline not recorded")
		}
	})
}
