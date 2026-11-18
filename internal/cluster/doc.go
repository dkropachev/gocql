// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package cluster owns the session's view of the cluster and the session
// core that execution runs on.
//
// It contains the control connection, topology and schema discovery from
// the system tables (system.local, system.peers and system.peers_v2 rows
// become endpoint.HostInfo values here), the topology, status and schema
// event handling, keyspace metadata, the partitioners (the ScyllaDB CDC
// partitioner among them) and the token ring, replica placement for each
// replication strategy, client routes, the loop that reconnects downed
// hosts, and the Core that ties the control connection, the connection
// pools and the metadata together. It also wires internal/pool to the
// session: the pool services, the per-shard dial with its observers, and
// the connection configuration built from the cluster settings.
//
// It must not contain connection pool internals (internal/pool), the
// request/response transport or socket dialing (internal/transport), node
// state (internal/endpoint), or statement execution and retries
// (internal/session).
package cluster
