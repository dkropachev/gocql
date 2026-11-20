// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	metadata2 "github.com/scylladb/gocql/v2/metadata"
)

// vnodeTestRing returns a ring of nodes hosts in dcs datacenters and racks
// racks per datacenter, each with vnodes random tokens.
func vnodeTestRing(tb testing.TB, r *rand.Rand, nodes, dcs, racks, vnodes int) *InternalTokenRing {
	tb.Helper()
	hosts := make([]*implEndpoint.HostInfo, nodes)
	for i := range hosts {
		h := new(implEndpoint.HostInfo)
		d := implEndpoint.HostInfoData(h)
		d.InternalHostId = cql.UUID{byte(i + 1), byte(r.Intn(256))}
		d.InternalDataCenter = "dc" + strconv.Itoa(i%dcs)
		d.InternalRack = "r" + strconv.Itoa(r.Intn(racks))
		for range vnodes {
			d.InternalTokens = append(d.InternalTokens, strconv.FormatInt(r.Int63()-r.Int63(), 10))
		}
		hosts[i] = h
	}
	ring, err := InternalNewTokenRing("Murmur3Partitioner", hosts)
	if err != nil {
		tb.Fatal(err)
	}
	return ring
}

func BenchmarkBuildV2VnodeRanges(b *testing.B) {
	ring := vnodeTestRing(b, rand.New(rand.NewSource(1)), 12, 2, 3, 256)
	ks := &metadata2.Keyspace{Name: "ks", StrategyClass: "org.apache.cassandra.locator.NetworkTopologyStrategy", StrategyOptions: map[string]string{"dc0": "3", "dc1": "3"}}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := InternalBuildV2VnodeRanges(ring, ks); err != nil {
			b.Fatal(err)
		}
	}
}

// TestBuildV2VnodeRangesMatchesFullWalk checks the walk that stops at the
// replication factor against one that visits every token, as the builder did
// before it stopped early.
func TestBuildV2VnodeRangesMatchesFullWalk(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	strategies := []*metadata2.Keyspace{
		{StrategyClass: "SimpleStrategy", StrategyOptions: map[string]string{"replication_factor": "0"}},
		{StrategyClass: "SimpleStrategy", StrategyOptions: map[string]string{"replication_factor": "1"}},
		{StrategyClass: "org.apache.cassandra.locator.SimpleStrategy", StrategyOptions: map[string]string{"replication_factor": "3"}},
		{StrategyClass: "SimpleStrategy", StrategyOptions: map[string]string{"replication_factor": "99"}},
		{StrategyClass: "NetworkTopologyStrategy", StrategyOptions: map[string]string{"dc0": "3"}},
		{StrategyClass: "NetworkTopologyStrategy", StrategyOptions: map[string]string{"dc0": "2", "dc1": "3", "dc2": "1"}},
		{StrategyClass: "NetworkTopologyStrategy", StrategyOptions: map[string]string{"dc0": "5", "dc1": "0", "dcX": "3"}},
		{StrategyClass: "NetworkTopologyStrategy", StrategyOptions: map[string]string{"dc0": "99", "dc1": "99"}},
		{StrategyClass: "EverywhereStrategy"},
		{StrategyClass: "LocalStrategy"},
	}
	for range 40 {
		nodes := 1 + r.Intn(9)
		ring := vnodeTestRing(t, r, nodes, 1+r.Intn(3), 1+r.Intn(4), 1+r.Intn(8))
		for _, ks := range strategies {
			want, wantErr := referenceVnodeRanges(ring, ks)
			got, err := InternalBuildV2VnodeRanges(ring, ks)
			if (err != nil) != (wantErr != nil) {
				t.Fatalf("%s %v: err %v, want %v", ks.StrategyClass, ks.StrategyOptions, err, wantErr)
			}
			for i := range want {
				if len(want[i].Replicas) == 0 {
					want[i].Replicas = nil
				}
				if len(got[i].Replicas) == 0 {
					got[i].Replicas = nil
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%d nodes, %s %v:\n got %v\nwant %v", nodes, ks.StrategyClass, ks.StrategyOptions, got, want)
			}
		}
	}
}

// TestVnodeRangeCache checks that keyspaces with one replication share their
// ranges, that a build on an unchanged ring reuses them, and that a changed
// ring or replication builds them again.
func TestVnodeRangeCache(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	ring := vnodeTestRing(t, r, 6, 2, 2, 4)
	nts := func(name, rf string) *metadata2.Keyspace {
		return &metadata2.Keyspace{Name: name, StrategyClass: "NetworkTopologyStrategy", StrategyOptions: map[string]string{"dc0": rf, "dc1": rf}}
	}
	same := func(a, b []metadata2.TokenRange) bool { return len(a) > 0 && &a[0] == &b[0] }
	var c vnodeRangeCache
	first, errs := c.build(ring, map[string]*metadata2.Keyspace{"a": nts("a", "2"), "b": nts("b", "2"), "c": nts("c", "1"), "nil": nil})
	if len(errs) != 0 || len(first) != 3 {
		t.Fatalf("ranges %d, errs %v", len(first), errs)
	}
	if !same(first["a"], first["b"]) || same(first["a"], first["c"]) {
		t.Fatal("keyspaces with one replication do not share ranges, or different ones do")
	}
	if want, _ := InternalBuildV2VnodeRanges(ring, nts("a", "2")); !reflect.DeepEqual(first["a"], want) {
		t.Fatal("cached ranges differ from built ones")
	}
	// The same ring, rebuilt as each publication rebuilds it.
	again := &InternalTokenRing{InternalPartitioner: ring.InternalPartitioner, InternalTokens: append([]InternalHostToken(nil), ring.InternalTokens...), InternalHosts: ring.InternalHosts}
	second, _ := c.build(again, map[string]*metadata2.Keyspace{"a": nts("a", "2"), "d": nts("d", "1")})
	if !same(first["a"], second["a"]) || !same(first["c"], second["d"]) {
		t.Fatal("unchanged ring did not reuse ranges")
	}
	if len(c.ranges) != 2 {
		t.Fatalf("cache keeps %d replications, want the 2 in use", len(c.ranges))
	}
	moved := vnodeTestRing(t, r, 6, 2, 2, 4)
	third, _ := c.build(moved, map[string]*metadata2.Keyspace{"a": nts("a", "2")})
	if same(first["a"], third["a"]) {
		t.Fatal("changed ring reused ranges")
	}
	if want, _ := InternalBuildV2VnodeRanges(moved, nts("a", "2")); !reflect.DeepEqual(third["a"], want) {
		t.Fatal("ranges of the changed ring differ from built ones")
	}
	_, errs = c.build(moved, map[string]*metadata2.Keyspace{"bad": {StrategyClass: "SimpleStrategy", StrategyOptions: map[string]string{"replication_factor": "x"}}})
	if len(errs) != 1 {
		t.Fatalf("errs %v, want one", errs)
	}
}

// referenceVnodeRanges is the builder as it was before its walk stopped at
// the replication factor.
func referenceVnodeRanges(ring *InternalTokenRing, ks *metadata2.Keyspace) ([]metadata2.TokenRange, error) {
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
	factor := func(value string, cap int) (int, error) {
		n, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0, cqltype.TypeValidation("replication factor", "invalid value %q", value)
		}
		if n > uint64(cap) {
			return cap, nil
		}
		return int(n), nil
	}
	nts := strings.HasSuffix(ks.StrategyClass, "NetworkTopologyStrategy")
	rf := 1
	dcs := make(map[string]int)
	var err error
	switch {
	case nts:
		for dc, value := range ks.StrategyOptions {
			if dc == "class" {
				continue
			}
			dcs[dc], err = factor(value, dcHosts[dc])
			if err != nil {
				return nil, err
			}
		}
	case strings.HasSuffix(ks.StrategyClass, "SimpleStrategy"):
		rf, err = factor(ks.StrategyOptions["replication_factor"], len(hosts))
		if err != nil {
			return nil, err
		}
	case strings.HasSuffix(ks.StrategyClass, "EverywhereStrategy"):
		rf = len(hosts)
	}
	out := make([]metadata2.TokenRange, len(ring.InternalTokens))
	for i, entry := range ring.InternalTokens {
		token, ok := entry.InternalToken.(implEndpoint.InternalInt64Token)
		if !ok {
			return nil, cqltype.TypeValidation("replica placement", "non-int64 token")
		}
		out[i].End = int64(token)
		seen := make(map[cql.UUID]bool)
		counts := make(map[string]int)
		racks := make(map[string]map[string]bool)
		repeats := make(map[string]int)
		if nts {
			for dc, rf := range dcs {
				repeats[dc] = max(rf-len(dcRacks[dc]), 0)
			}
		}
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
			} else if len(out[i].Replicas) >= rf {
				break
			}
			out[i].Replicas = append(out[i].Replicas, metadata2.Target{HostID: id})
		}
	}
	for i := range out {
		out[i].Start = out[(i+len(out)-1)%len(out)].End
	}
	return out, nil
}
