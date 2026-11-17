// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package metadata describes what a session knows about the cluster. A
// Snapshot holds the topology (hosts, token ring and tablets) and the schema:
// keyspaces, tables, columns, indexes, views, user types, functions and
// aggregates. Mode controls how much schema a session loads; topology always
// loads.
package metadata
