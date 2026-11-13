// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/model"
)

func countRackLookups(t *testing.T) *int {
	t.Helper()
	n := new(int)
	validateRackLookup = func() { *n++ }
	t.Cleanup(func() { validateRackLookup = nil })
	return n
}

func TestDCAwareValidateSkipsRackLookupWhenUnset(t *testing.T) {
	p := codecTestMust(NewRoutingPolicy(RoutingOptions{Datacenter: "a"}))
	n := countRackLookups(t)
	m := v2TestMetadata()
	for i := 0; i < 3; i++ {
		if err := p.Validate(m); err != nil {
			t.Fatal(err)
		}
	}
	if *n != 0 {
		t.Fatalf("rack looked up %d times without a rack", *n)
	}
}

func TestDCAwareValidateRackLookupOncePerIndex(t *testing.T) {
	p := codecTestMust(NewRoutingPolicy(RoutingOptions{Datacenter: "a", Rack: "r1"}))
	n := countRackLookups(t)
	m := v2TestMetadata()
	for i := 0; i < 3; i++ {
		if err := p.Validate(m); err != nil {
			t.Fatal(err)
		}
	}
	if *n != 1 {
		t.Fatalf("rack looked up %d times on one index, want 1", *n)
	}
}

// TestDCAwareValidateCacheFollowsIndex checks that a DC or rack that was
// valid in an old index is rejected once the snapshot has a new index
// without it, and accepted again when it returns.
func TestDCAwareValidateCacheFollowsIndex(t *testing.T) {
	for _, o := range []RoutingOptions{{Datacenter: "a"}, {Datacenter: "a", Rack: "r1"}} {
		p := codecTestMust(NewRoutingPolicy(o))
		old := v2TestMetadata()
		if err := p.Validate(old); err != nil {
			t.Fatal(err)
		}
		hosts := func(dc, rack string) *MetadataSnapshot {
			h := []model.HostSnapshot{v2TestHost(1, dc, rack), v2TestHost(2, "b", "r9")}
			return NewMetadataSnapshot(SnapshotState{InternalHostIndex: InternalBuildRoutingHostIndex(h), InternalHosts: h})
		}
		noDC := hosts("c", "r1")
		for name, m := range map[string]*MetadataSnapshot{"no dc": noDC, "no rack": hosts("a", "r2")} {
			if o.Rack == "" && name == "no rack" {
				continue
			}
			if err := p.Validate(m); err == nil {
				t.Fatalf("%+v: %s validated after the index changed", o, name)
			}
			// the failure must not poison the cache
			if err := p.Validate(m); err == nil {
				t.Fatalf("%+v: %s validated on the second call", o, name)
			}
		}
		if err := p.Validate(hosts("a", "r1")); err != nil {
			t.Fatal(err)
		}
		if err := p.Validate(old); err != nil {
			t.Fatal(err)
		}
		// an index rejected earlier is rejected again after the cache
		// holds another one
		if err := p.Validate(noDC); err == nil {
			t.Fatalf("%+v: stale acceptance", o)
		}
	}
}
