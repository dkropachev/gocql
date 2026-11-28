// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package routingtest builds metadata snapshots for the tests of a
// routing.Policy, without a session or a cluster.
//
// A routing.Info describes the request a policy routes. Its zero Kind is
// cql.UnknownStatement, which the built-in policies route as possibly
// conditional: in replica order, without shuffling or Pick, and to a
// replica's shard only. Set Kind to cql.OrdinaryStatement, as a session does
// for a statement known not to be conditional, to test the usual plan;
// Request returns such an Info.
//
// A program that imports it links the engine behind the built-in policies,
// so a routing.NewPolicy or routing.SingleTarget policy routes over its
// snapshots as it would in a session, and so do routing.HostRoutes and
// routing.ReplicaRoutes.
package routingtest

import (
	"cmp"
	"fmt"
	"maps"
	"slices"

	"github.com/scylladb/gocql/v2/cql"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
)

// Cluster describes the cluster a snapshot shows.
//
// The zero Cluster has no hosts and no schema.
type Cluster struct {
	// Keyspaces holds the schema by keyspace name. Replicas and routing find
	// only the tables and views listed here and in Tables.
	Keyspaces map[string]*metadata.Keyspace
	// Tables lists tables to add to Keyspaces without columns, enough for
	// routing by a token; a missing keyspace is added too. A table Keyspaces
	// already holds is kept as it is, and Keyspaces itself is not modified.
	Tables []Table
	// TokenRanges holds the token ranges of each keyspace with their
	// replicas, as a token ring places them. The last range wraps around to
	// the first.
	TokenRanges map[string][]metadata.TokenRange
	// Tablets holds the tablets of each tablet table with their replicas. A
	// table listed here is placed by its tablets alone: a token outside them
	// has no replicas.
	Tablets map[Table][]metadata.TokenRange
	// LocalDatacenter and LocalRack are the session's locality, as
	// config.ConfigBuilder.WithLocalDatacenter and WithLocalRack set them. A built-in policy
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
//
// It fails with a cql.ValidationError when c is not a cluster a session
// could see: two hosts with one HostID, a replica whose HostID no host has,
// two token ranges of a keyspace with one End, a tablet that is empty or
// overlaps another tablet of its table, or a tablet of a table that neither
// Keyspaces nor Tables holds.
func NewSnapshot(c Cluster) (*metadata.Snapshot, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
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
	for _, t := range c.Tables {
		ks := state.InternalKeyspaces[t.Keyspace]
		if ks == nil {
			ks = &metadata.Keyspace{Name: t.Keyspace}
		} else if ks.Tables[t.Name] != nil {
			continue
		} else {
			// The caller's keyspace is shared; add the table to a copy.
			clone := *ks
			ks = &clone
		}
		ks.Tables = maps.Clone(ks.Tables)
		if ks.Tables == nil {
			ks.Tables = make(map[string]*metadata.Table)
		}
		ks.Tables[t.Name] = &metadata.Table{Keyspace: t.Keyspace, Name: t.Name}
		state.InternalKeyspaces[t.Keyspace] = ks
	}
	for ks, r := range c.TokenRanges {
		state.InternalRanges[ks] = sorted(r)
	}
	for t, r := range c.Tablets {
		state.InternalTablets[implRouting.InternalMetadataTable{InternalKeyspace: t.Keyspace, InternalTable: t.Name}] = sorted(r)
	}
	return implRouting.NewMetadataSnapshot(state).Public(), nil
}

// validate checks that c is a cluster a session could see.
func (c *Cluster) validate() error {
	invalid := func(format string, args ...any) error {
		return &cql.ValidationError{Field: "routingtest cluster", Err: fmt.Errorf(format, args...)}
	}
	hosts := make(map[cql.UUID]bool, len(c.Hosts))
	for _, h := range c.Hosts {
		if hosts[h.HostID] {
			return invalid("host %s is listed twice", h.HostID)
		}
		hosts[h.HostID] = true
	}
	replicas := func(where string, r metadata.TokenRange) error {
		for _, t := range r.Replicas {
			if !hosts[t.HostID] {
				return invalid("%s range (%d, %d]: replica %s is not a host of the cluster", where, r.Start, r.End, t.HostID)
			}
		}
		return nil
	}
	for ks, ranges := range c.TokenRanges {
		ends := make(map[int64]bool, len(ranges))
		for _, r := range ranges {
			if ends[r.End] {
				return invalid("keyspace %s: two token ranges end at %d", ks, r.End)
			}
			ends[r.End] = true
			if err := replicas("keyspace "+ks, r); err != nil {
				return err
			}
		}
	}
	for t, ranges := range c.Tablets {
		if !c.holds(t) {
			return invalid("tablets of %s.%s: no such table in Keyspaces or Tables", t.Keyspace, t.Name)
		}
		for i, r := range sorted(ranges) {
			if r.Start >= r.End {
				return invalid("%s.%s: tablet (%d, %d] is empty", t.Keyspace, t.Name, r.Start, r.End)
			}
			if i > 0 && r.Start < sorted(ranges)[i-1].End {
				return invalid("%s.%s: tablet (%d, %d] overlaps another", t.Keyspace, t.Name, r.Start, r.End)
			}
			if err := replicas(t.Keyspace+"."+t.Name, r); err != nil {
				return err
			}
		}
	}
	return nil
}

// holds reports whether c's Keyspaces or Tables hold table t, or a view of
// that name.
func (c *Cluster) holds(t Table) bool {
	if slices.Contains(c.Tables, t) {
		return true
	}
	ks := c.Keyspaces[t.Keyspace]
	return ks != nil && (ks.Tables[t.Name] != nil || ks.Views[t.Name] != nil)
}

// Request returns the routing.Info of an ordinary, not conditional, request
// for the partition of token in keyspace's table: Kind is
// cql.OrdinaryStatement, so a built-in policy routes it with its usual plan.
// Its Consistency is zero, cql.Any; set it to test a LOCAL_* consistency.
func Request(keyspace, table string, token int64) routing.Info {
	return routing.Info{Keyspace: keyspace, Table: table, Token: cql.Some(token), Kind: cql.OrdinaryStatement}
}

// Host returns a host of the cluster with id, in datacenter and rack, that
// is enabled, up and connected, as a host a session routes to is.
func Host(id cql.UUID, datacenter, rack string) metadata.HostSnapshot {
	return metadata.HostSnapshot{HostID: id, Datacenter: datacenter, Rack: rack, Enabled: true, Up: true, Connected: true}
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
