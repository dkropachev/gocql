// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	routing "github.com/scylladb/gocql/v2/internal/routing"
	metadata2 "github.com/scylladb/gocql/v2/metadata"
)

// Cold-path replica precomputation uses the same distinct-node/rack-budget
// traversal for every factor, then retains its global ring order for LWT.
func InternalBuildV2VnodeRanges(ring *InternalTokenRing, ks *metadata2.Keyspace) ([]routing.Span, error) {
	hosts := make(map[cql.UUID]*implEndpoint.HostInfo)
	dcHosts := make(map[string]int)
	dcRacks := make(map[string]map[string]bool)
	for _, entry := range ring.InternalTokens {
		h := entry.InternalHost
		id := implEndpoint.InternalHostInfoHostUUID(h)
		if hosts[id] != nil {
			continue
		}
		hosts[id] = h
		dc := h.DataCenter()
		dcHosts[dc]++
		if dcRacks[dc] == nil {
			dcRacks[dc] = make(map[string]bool)
		}
		dcRacks[dc][h.Rack()] = true
	}
	// factor is the keyspace's replication factor in dc, capped at the
	// hosts there are to place replicas on.
	nts := strings.HasSuffix(ks.StrategyClass, "NetworkTopologyStrategy")
	factor := func(dc string, cap int) (int, error) {
		n, ok := ks.ReplicationFactor(dc)
		if !ok {
			value := ks.StrategyOptions["replication_factor"]
			if nts {
				value = ks.StrategyOptions[dc]
			}
			return 0, cqltype.TypeValidation("replication factor", "invalid value %q", value)
		}
		return min(n, cap), nil
	}
	rf := 1
	dcs := make(map[string]int)
	var err error
	switch {
	case nts:
		for dc := range ks.StrategyOptions {
			if dc == "class" {
				continue
			}
			dcs[dc], err = factor(dc, dcHosts[dc])
			if err != nil {
				return nil, err
			}
		}
	case strings.HasSuffix(ks.StrategyClass, "SimpleStrategy"):
		rf, err = factor("", len(hosts))
		if err != nil {
			return nil, err
		}
	case strings.HasSuffix(ks.StrategyClass, "EverywhereStrategy"):
		rf = len(hosts)
	}
	// want is the number of replicas each token gets: the walk stops once it
	// has them, instead of visiting every token of the ring for every token.
	want := min(rf, len(hosts))
	if nts {
		want = 0
		for _, n := range dcs {
			want += n
		}
	}
	out := make([]routing.Span, len(ring.InternalTokens))
	seen := make(map[cql.UUID]bool, len(hosts))
	counts := make(map[string]int, len(dcs))
	racks := make(map[string]map[string]bool, len(dcs))
	repeats := make(map[string]int, len(dcs))
	for i, entry := range ring.InternalTokens {
		token, ok := entry.InternalToken.(implEndpoint.InternalInt64Token)
		if !ok {
			return nil, cqltype.TypeValidation("replica placement", "non-int64 token")
		}
		out[i].End = int64(token)
		if want == 0 {
			continue
		}
		clear(seen)
		clear(counts)
		for _, r := range racks {
			clear(r)
		}
		if nts {
			for dc, rf := range dcs {
				repeats[dc] = max(rf-len(dcRacks[dc]), 0)
			}
		}
		out[i].Replicas = make([]metadata2.Target, 0, want)
		for j := range ring.InternalTokens {
			h := ring.InternalTokens[(i+j)%len(ring.InternalTokens)].InternalHost
			id := implEndpoint.InternalHostInfoHostUUID(h)
			if seen[id] {
				continue
			}
			seen[id] = true
			if nts {
				dc, rack := h.DataCenter(), h.Rack()
				if counts[dc] >= dcs[dc] {
					continue
				}
				if racks[dc] == nil {
					racks[dc] = make(map[string]bool)
				}
				if racks[dc][rack] {
					if repeats[dc] == 0 {
						continue
					}
					repeats[dc]--
				}
				racks[dc][rack] = true
				counts[dc]++
			}
			out[i].Replicas = append(out[i].Replicas, metadata2.Target{HostID: id})
			if len(out[i].Replicas) == want || len(seen) == len(hosts) {
				break
			}
		}
	}
	for i := range out {
		out[i].Start = out[(i+len(out)-1)%len(out)].End
	}
	return out, nil
}

// vnodeRingEntry is one token of the ring a vnodeRangeCache was built for,
// with what placement reads of its host.
type vnodeRingEntry struct {
	dc, rack string
	token    int64
	id       cql.UUID
}

// vnodeRangeCache keeps the replica ranges built for the last token ring, by
// replication. Keyspaces with the same replication share their ranges, and
// a publication whose ring did not change reuses the previous one's: building
// them walks the whole ring, and every node or schema event publishes.
// Ranges are read-only once built, so snapshots share them.
type vnodeRangeCache struct {
	ranges map[string][]routing.Span
	ring   []vnodeRingEntry
	mu     sync.Mutex
}

// build returns the ranges of every keyspace on ring, and the errors of the
// keyspaces whose ranges it could not build.
func (c *vnodeRangeCache) build(ring *InternalTokenRing, keyspaces map[string]*metadata2.Keyspace) (map[string][]routing.Span, []error) {
	entries := make([]vnodeRingEntry, 0, len(ring.InternalTokens))
	for _, t := range ring.InternalTokens {
		token, _ := t.InternalToken.(implEndpoint.InternalInt64Token)
		entries = append(entries, vnodeRingEntry{token: int64(token), id: implEndpoint.InternalHostInfoHostUUID(t.InternalHost), dc: t.InternalHost.DataCenter(), rack: t.InternalHost.Rack()})
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	previous := c.ranges
	if !slices.Equal(entries, c.ring) {
		previous = nil
	}
	used := make(map[string][]routing.Span)
	out := make(map[string][]routing.Span, len(keyspaces))
	var errs []error
	for name, ks := range keyspaces {
		if ks == nil {
			continue
		}
		key := replicationKey(ks)
		ranges, ok := used[key]
		if !ok {
			ranges, ok = previous[key]
		}
		if !ok {
			var err error
			if ranges, err = InternalBuildV2VnodeRanges(ring, ks); err != nil {
				errs = append(errs, err)
				continue
			}
		}
		used[key] = ranges
		out[name] = ranges
	}
	c.ring, c.ranges = entries, used
	return out, errs
}

// replicationKey identifies the placement a keyspace's replication gives.
func replicationKey(ks *metadata2.Keyspace) string {
	var b strings.Builder
	b.WriteString(ks.StrategyClass)
	for _, k := range slices.Sorted(maps.Keys(ks.StrategyOptions)) {
		b.WriteByte(0)
		b.WriteString(k)
		b.WriteByte(0)
		b.WriteString(ks.StrategyOptions[k])
	}
	return b.String()
}
