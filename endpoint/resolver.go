// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

import (
	"context"
	"net/netip"
)

// Resolver resolves host names to IP addresses. *net.Resolver satisfies it.
// network is "ip", "ip4" or "ip6", as for net.Resolver.LookupNetIP.
// Implementations must return promptly once ctx is done.
type Resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}
