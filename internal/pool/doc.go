// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package pool owns the connections the session keeps open to each node.
//
// It contains the per-host connection pools, filled with one connection per
// shard on ScyllaDB and a configured number per host otherwise; the
// connection pickers that choose a shard's connection for a request; the
// retries of a pool's own dials under its reconnection policy; and the
// policy-level pool that holds one host pool per node.
//
// It must not contain dialing or the connection itself (internal/transport),
// the decision which hosts exist or when a downed one is retried
// (internal/cluster), or request routing (internal/routing). It reaches the
// session only through the services the session passes it.
package pool
