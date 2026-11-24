// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package routingtest builds metadata snapshots for the tests of a
// routing.Policy, without a session or a cluster.
//
// A program that imports it links the engine behind the built-in policies,
// so a routing.NewPolicy or routing.SingleTarget policy routes over its
// snapshots as it would in a session, and so do routing.HostRoutes and
// routing.ReplicaRoutes.
package routingtest

import (
	"cmp"
	"maps"
	"slices"

	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
)

// Cluster describes the cluster a snapshot shows.
//
// The zero Cluster has no hosts and no schema.
type Cluster struct {
	// Keyspaces holds the schema by keyspace name. Replicas and routing find
	// only the tables and views listed here.
	Keyspaces map[string]*metadata.Keyspace
	// TokenRanges holds the token ranges of each keyspace with their
	// replicas, as a token ring places them. The last range wraps around to
	// the first.
	TokenRanges map[string][]metadata.TokenRange
	// Tablets holds the tablets of each tablet table with their replicas. A
	// table listed here is placed by its tablets alone: a token outside them
	// has no replicas.
	Tablets map[Table][]metadata.TokenRange
	// LocalDatacenter and LocalRack are the session's locality, as
	// config.ConfigBuilder.WithLocalDatacenter sets it. A built-in policy
	// without a datacenter of its own prefers them.
	LocalDatacenter, LocalRack string
	// Hosts lists the cluster's hosts, in the order of metadata.Snapshot.Hosts.
	Hosts []metadata.HostSnapshot
	// Generation is the snapshot's generation.
	Generation uint64
}

// Table names a table.
//
// The zero Table names no table.
type Table struct {
	// Keyspace and Name are the table's keyspace and name.
	Keyspace, Name string
}

// NewSnapshot returns a snapshot of c. Its keyspaces' tables use the
// Murmur3 partitioner unless their options name the CDC one. The snapshot
// shares c's keyspaces and its hosts' tokens, which the caller must not
// modify afterwards.
func NewSnapshot(c Cluster) *metadata.Snapshot {
	hosts := slices.Clone(c.Hosts)
	index := implRouting.InternalBuildRoutingHostIndex(hosts)
	index.InternalSetLocal(c.LocalDatacenter, c.LocalRack)
	state := implRouting.SnapshotState{
		InternalGeneration:   c.Generation,
		InternalHosts:        hosts,
		InternalHostIndex:    index,
		InternalKeyspaces:    maps.Clone(c.Keyspaces),
		InternalRanges:       make(map[string][]metadata.TokenRange, len(c.TokenRanges)),
		InternalTablets:      make(map[implRouting.InternalMetadataTable][]metadata.TokenRange, len(c.Tablets)),
		InternalPartitioner:  murmur3,
		InternalPartitioners: partitioners,
	}
	if state.InternalKeyspaces == nil {
		state.InternalKeyspaces = make(map[string]*metadata.Keyspace)
	}
	for ks, r := range c.TokenRanges {
		state.InternalRanges[ks] = sorted(r)
	}
	for t, r := range c.Tablets {
		state.InternalTablets[implRouting.InternalMetadataTable{InternalKeyspace: t.Keyspace, InternalTable: t.Name}] = sorted(r)
	}
	return implRouting.NewMetadataSnapshot(state).Public()
}

const murmur3 = "org.apache.cassandra.dht.Murmur3Partitioner"

var partitioners = map[string]routing.TokenPartitioner{
	murmur3:                           routing.Murmur3TokenPartitioner,
	"Murmur3Partitioner":              routing.Murmur3TokenPartitioner,
	"com.scylladb.dht.CDCPartitioner": routing.CDCTokenPartitioner,
	"CDCPartitioner":                  routing.CDCTokenPartitioner,
}

// sorted returns a copy of ranges in token order, with copied replicas.
func sorted(ranges []metadata.TokenRange) []metadata.TokenRange {
	out := slices.Clone(ranges)
	for i := range out {
		out[i].Replicas = slices.Clone(out[i].Replicas)
	}
	slices.SortFunc(out, func(a, b metadata.TokenRange) int { return cmp.Compare(a.End, b.End) })
	return out
}
