// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package transport owns a single connection to a node.
//
// It contains dialing: HostDialer, ShardDialer and DialedHost, TLS, and the
// shard-aware source port (ScyllaShardAwareDialer, ScyllaGetSourcePort).
// It contains connection startup (startup.go): OPTIONS, parsing SUPPORTED
// into the node's ScyllaDB features and the CQL protocol extensions the
// connection opts into (supported.go), the STARTUP options including
// ApplicationInfo, and authentication. And it contains the request/response
// transport over an open connection: stream IDs, framing and segments,
// writers, response reading, timeouts and orphaned streams.
//
// It must not contain anything that spans connections: pools
// (internal/pool), the cluster view (internal/cluster), or retries and
// execution (internal/session).
package transport
