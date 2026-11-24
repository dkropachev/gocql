// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"slices"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/metadata"
	publicRouting "github.com/scylladb/gocql/v2/routing"
)

// TestUnsetLocality checks when Connect warns that a multi-datacenter
// cluster has no local datacenter.
func TestUnsetLocality(t *testing.T) {
	hosts := []metadata.HostSnapshot{
		{ID: cql.UUID{15: 1}, Datacenter: "b", Enabled: true},
		{ID: cql.UUID{15: 2}, Datacenter: "a", Enabled: true},
		{ID: cql.UUID{15: 3}, Datacenter: "a", Enabled: true},
		{ID: cql.UUID{15: 4}, Datacenter: "c"}, // filtered out
	}
	m := routing.NewMetadataSnapshot(routing.SnapshotState{InternalHosts: hosts})
	withPolicy := func(p routing.RoutingPolicy) settings.Config {
		c := settings.DefaultConfig("h")
		profile := *c.DefaultProfile
		profile.Routing = p
		c.DefaultProfile = &profile
		return c
	}
	if got := unsetLocality(settings.DefaultConfig("h"), m); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("default config: %v", got)
	}
	local := settings.DefaultConfig("h")
	local.LocalDatacenter = "a"
	own := codecMustPolicy(t, publicRouting.Options{Datacenter: "a"})
	for name, c := range map[string]settings.Config{
		"session locality": local,
		"policy locality":  withPolicy(own),
		"custom policy":    withPolicy(routing.EngineOf(fakeRoutingPolicy{})),
	} {
		if got := unsetLocality(c, m); got != nil {
			t.Errorf("%s: %v", name, got)
		}
	}
}

func codecMustPolicy(t *testing.T, o publicRouting.Options) routing.RoutingPolicy {
	t.Helper()
	p, err := routing.NewRoutingPolicy(o)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
