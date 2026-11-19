// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package endpoint owns what the driver knows about a single node.
//
// It contains HostInfo and its state, the node's addresses and how they are
// resolved and translated (contact points, DNS, address translators), the
// node's ScyllaDB features as values (ScyllaHostFeatures,
// ScyllaConnectionFeatures), and the Token interface with its int64 form
// (InternalInt64Token). Those two stay here, not with the partitioners in
// internal/cluster, because the connection pickers in internal/pool, which
// internal/cluster imports, pick a shard by them.
//
// It must not contain anything that dials or talks to a node: dialers
// (HostDialer, DialedHost, the shard-aware dialer and source port), TLS,
// SUPPORTED parsing, the CQL protocol extensions, STARTUP options
// (ApplicationInfo) and the rest of connection startup belong in
// internal/transport. It must not contain pools (internal/pool) or the
// cluster-wide view of the nodes and their placement, including the
// partitioners and the token ring (internal/cluster).
package endpoint
