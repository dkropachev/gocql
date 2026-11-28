// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata_test

import (
	"testing"

	"github.com/scylladb/gocql/v2/metadata"
)

func TestReplicationFactor(t *testing.T) {
	t.Parallel()
	nts := map[string]string{"dc1": "3", "dc2": "0", "bad": "three"}
	for _, c := range []struct {
		name    string
		class   string
		options map[string]string
		dc      string
		want    int
		ok      bool
	}{
		{"NTS", "org.apache.cassandra.locator.NetworkTopologyStrategy", nts, "dc1", 3, true},
		{"NTS zero", "NetworkTopologyStrategy", nts, "dc2", 0, true},
		{"NTS other dc", "NetworkTopologyStrategy", nts, "dc3", 0, true},
		{"NTS no dc", "NetworkTopologyStrategy", nts, "", 0, false},
		{"NTS invalid", "NetworkTopologyStrategy", nts, "bad", 0, false},
		{"Simple", "org.apache.cassandra.locator.SimpleStrategy", map[string]string{"replication_factor": "2"}, "any", 2, true},
		{"Simple missing", "SimpleStrategy", nil, "dc1", 0, false},
		{"Local", "org.apache.cassandra.locator.LocalStrategy", nil, "dc1", 1, true},
		{"Everywhere", "org.apache.cassandra.locator.EverywhereStrategy", nil, "dc1", 0, false},
		{"none", "", nil, "dc1", 0, false},
	} {
		got, ok := metadata.Replication{Class: c.class, Options: c.options}.Factor(c.dc)
		if got != c.want || ok != c.ok {
			t.Errorf("%s: Factor(%q) = %d, %v; want %d, %v", c.name, c.dc, got, ok, c.want, c.ok)
		}
		ks := &metadata.Keyspace{StrategyClass: c.class, StrategyOptions: c.options}
		if got2, ok2 := ks.ReplicationFactor(c.dc); got2 != got || ok2 != ok {
			t.Errorf("%s: Keyspace.ReplicationFactor = %d, %v; Replication.Factor = %d, %v", c.name, got2, ok2, got, ok)
		}
	}
	var nilKeyspace *metadata.Keyspace
	if _, ok := nilKeyspace.ReplicationFactor("dc1"); ok {
		t.Error("a nil keyspace has a replication factor")
	}
}
