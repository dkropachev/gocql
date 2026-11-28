// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"math"
	"strconv"
	"strings"
)

// Replication is how a keyspace places its replicas: its replication
// strategy and options. Snapshot.Replication returns it without copying the
// keyspace's schema.
//
// The zero Replication names no strategy and has no replication factor.
type Replication struct {
	// Options holds the replication options other than the class, as
	// Keyspace.StrategyOptions does.
	Options map[string]string
	// Class is the replication strategy class, as Keyspace.StrategyClass
	// is.
	Class string
	// TabletBased reports a ScyllaDB keyspace that distributes its data in
	// tablets rather than vnodes, as Keyspace.TabletBased does.
	TabletBased bool
}

// Factor returns the number of replicas the keyspace keeps in datacenter dc,
// and reports whether the strategy and its options state it:
//
//   - NetworkTopologyStrategy: the factor of dc, which is zero, and reported,
//     for a datacenter the options do not name.
//   - SimpleStrategy: the replication_factor option, for any dc: the
//     strategy places that many replicas in the whole cluster, regardless of
//     datacenters.
//   - LocalStrategy: one, the local node's copy, for any dc.
//
// It reports false for EverywhereStrategy, whose factor is the number of
// hosts in dc, which Snapshot.Hosts lists, for an option that is not a
// non-negative integer, and for another strategy.
func (r Replication) Factor(dc string) (int, bool) {
	switch {
	case strings.HasSuffix(r.Class, "NetworkTopologyStrategy"):
		v, ok := r.Options[dc]
		if !ok {
			return 0, dc != "" && dc != "class"
		}
		return parseFactor(v)
	case strings.HasSuffix(r.Class, "SimpleStrategy"):
		return parseFactor(r.Options["replication_factor"])
	case strings.HasSuffix(r.Class, "LocalStrategy"):
		return 1, true
	default:
		return 0, false
	}
}

// parseFactor parses a replication factor option.
func parseFactor(v string) (int, bool) {
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, false
	}
	return int(min(n, math.MaxInt32)), true
}

// ReplicationFactor returns the number of replicas the keyspace keeps in
// datacenter dc, as Replication.Factor does for its strategy and options.
func (ks *Keyspace) ReplicationFactor(dc string) (int, bool) {
	if ks == nil {
		return 0, false
	}
	return Replication{Class: ks.StrategyClass, Options: ks.StrategyOptions}.Factor(dc)
}
