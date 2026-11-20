// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"bytes"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/routing"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

// leaderTestMetadata is v2TestMetadata with a tablet of ks.table over
// (0, 100] whose replicas are leader first, then the others, as
// TABLETS_ROUTING_V2 reports them. A tablet without a version is one learned
// over TABLETS_ROUTING_V1, which reports no leader.
func leaderTestMetadata(t *testing.T, mode string, versioned bool, replicas ...byte) *implRouting.MetadataSnapshot {
	t.Helper()
	m := v2TestMetadata()
	routing.SnapshotData(m).InternalKeyspaces["ks"].ConsistencyMode = mode
	targets := make([]routing.Target, len(replicas))
	for i, id := range replicas {
		targets[i] = hTarget(id, uint32(i+1))
	}
	h := testHint(t, 0, 100, cql.Option[uint64]{Value: 0x1234, Set: versioned}, targets...)
	r, _ := implRouting.InternalUpdateTabletRange(nil, h)
	routing.SnapshotData(m).InternalPlacements = routing.SnapshotData(m).InternalPlacements.Put(implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"}, implRouting.InternalTabletPlacement{InternalRanges: r})
	return m
}

// TestLeaderAwarePlanOrder checks which replica a plan tries first for a
// table whose tablet reports leader 2 (local datacenter a, rack r2) and
// followers 1 (rack r1, the preferred rack) and 3 (datacenter b): the leader
// comes first for writes and strongly consistent reads of a globally
// consistent keyspace, ahead of the nearer replica 1, and nowhere else.
func TestLeaderAwarePlanOrder(t *testing.T) {
	strong := []cql.Consistency{cql.Any, cql.Two, cql.Three, cql.Quorum, cql.All, cql.LocalQuorum, cql.EachQuorum, cql.Serial, cql.LocalSerial}
	weak := []cql.Consistency{cql.One, cql.LocalOne}
	local := routing2.Options{TokenAware: true, LeaderAware: true, ShuffleReplicas: true, Datacenter: "a", Rack: "r1"}
	info := func(c cql.Consistency) routing2.Info {
		return routing2.Info{Keyspace: "ks", Table: "table", Token: cql.Option[int64]{Value: 50, Set: true}, Consistency: c, Kind: cql.OrdinaryStatement}
	}
	type test struct {
		name        string
		options     routing2.Options
		mode        string
		versioned   bool
		replicas    []byte
		consistency []cql.Consistency
		conditional bool
		edit        func(m *implRouting.MetadataSnapshot)
		first       byte
		absent      byte
	}
	tests := []test{
		{name: "GlobalStrong", options: local, mode: "global", versioned: true, replicas: []byte{2, 1, 3}, consistency: strong, first: 2},
		{name: "GlobalConditional", options: local, mode: "global", versioned: true, replicas: []byte{2, 1, 3}, consistency: []cql.Consistency{cql.Quorum}, conditional: true, first: 2},
		{name: "GlobalWeakRead", options: local, mode: "global", versioned: true, replicas: []byte{2, 1, 3}, consistency: weak, first: 1},
		{name: "Eventual", options: local, mode: "", versioned: true, replicas: []byte{2, 1, 3}, consistency: strong, first: 1},
		{name: "TabletWithoutVersion", options: local, mode: "global", versioned: false, replicas: []byte{2, 1, 3}, consistency: strong, first: 1},
		{name: "LeaderAwareOff", options: routing2.Options{TokenAware: true, ShuffleReplicas: true, Datacenter: "a", Rack: "r1"}, mode: "global", versioned: true, replicas: []byte{2, 1, 3}, consistency: strong, first: 1},
		{name: "LeaderFiltered", options: local, mode: "global", versioned: true, replicas: []byte{2, 1, 3}, consistency: strong, first: 1, absent: 2,
			edit: func(m *implRouting.MetadataSnapshot) { routing.SnapshotData(m).InternalHosts[1].Enabled = false }},
		{name: "LeaderDown", options: local, mode: "global", versioned: true, replicas: []byte{2, 1, 3}, consistency: strong, first: 1,
			edit: func(m *implRouting.MetadataSnapshot) { routing.SnapshotData(m).InternalHosts[1].Up = false }},
		{name: "RemoteLeaderNotAllowed", options: local, mode: "global", versioned: true, replicas: []byte{3, 1, 2}, consistency: strong, first: 1, absent: 3},
		{name: "RemoteLeaderAllowed", options: routing2.Options{TokenAware: true, LeaderAware: true, ShuffleReplicas: true, Datacenter: "a", Rack: "r1", AllowRemote: true}, mode: "global", versioned: true, replicas: []byte{3, 1, 2}, consistency: strong, first: 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m := leaderTestMetadata(t, test.mode, test.versioned, test.replicas...)
			if test.edit != nil {
				test.edit(m)
			}
			policy := codecTestMust(implRouting.NewRoutingPolicy(test.options))
			for _, c := range test.consistency {
				i := info(c)
				i.Conditional = test.conditional
				// Repeat: shuffled replicas must not move the leader.
				for range 32 {
					plan := implRouting.NewPlan(policy, i, m)
					first, ok := implRouting.InternalNextTarget(plan)
					if !ok || first.HostID[15] != test.first {
						t.Fatalf("%v: first target %+v, want host %d", c, first, test.first)
					}
					if test.first == test.replicas[0] && test.versioned && (!first.ShardID.Set || first.ShardID.Value != 1 || !first.StrictShard) {
						t.Fatalf("%v: leader target %+v, want its tablet shard 1, strict", c, first)
					}
					rest := v2PlanIDs(plan)
					if test.absent != 0 && bytes.IndexByte(append([]byte{first.HostID[15]}, rest...), test.absent) >= 0 {
						t.Fatalf("%v: plan %d %v reaches excluded host %d", c, first.HostID[15], rest, test.absent)
					}
				}
			}
		})
	}
}

// TestDefaultRoutingIsLeaderAware checks that the session's default policy
// routes strongly consistent requests to the tablet leader.
func TestDefaultRoutingIsLeaderAware(t *testing.T) {
	policy, ok := settings.DefaultConfig().DefaultProfile.Routing.(*implRouting.InternalRoutingPolicy)
	if !ok {
		t.Fatalf("default routing policy %T", settings.DefaultConfig().DefaultProfile.Routing)
	}
	if !policy.InternalOptions.LeaderAware || !policy.InternalOptions.TokenAware {
		t.Fatalf("default routing options %+v", policy.InternalOptions)
	}
}

// TestTabletsRoutingNegotiation checks which tablet routing extension a
// connection asks for in STARTUP: V2 whenever the server offers it, V1 when
// it offers only V1, and none for a server, such as Cassandra, that offers
// neither.
func TestTabletsRoutingNegotiation(t *testing.T) {
	for _, test := range []struct {
		name   string
		offers []string
		v1, v2 bool
	}{
		{"None", nil, false, false},
		{"V1", []string{transport.InternalTabletsRoutingV1}, true, false},
		{"V2", []string{transport.InternalTabletsRoutingV2}, false, true},
		{"Both", []string{transport.InternalTabletsRoutingV1, transport.InternalTabletsRoutingV2}, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			supported := map[string][]string{}
			for _, key := range test.offers {
				supported[key] = nil
			}
			exts := transport.InternalTypedTabletExtensions(transport.InternalParseCQLProtocolExtensions(supported, 4, logging.Nop()), supported)
			startup := map[string]string{}
			for _, ext := range exts {
				for k, v := range ext.InternalSerialize() {
					startup[k] = v
				}
			}
			_, v1 := startup[transport.InternalTabletsRoutingV1]
			_, v2 := startup[transport.InternalTabletsRoutingV2]
			if v1 != test.v1 || v2 != test.v2 {
				t.Fatalf("STARTUP options %v: V1 %v V2 %v, want %v %v", startup, v1, v2, test.v1, test.v2)
			}
			c := &transport.Conn{InternalVersion: 4, InternalCqlProtoExts: exts}
			c.InternalInitFramerCache()
			if d := c.InternalFramers.InternalDefaults; d.InternalTabletsRoutingV1 != test.v1 || d.InternalTabletsRoutingV2 != test.v2 {
				t.Fatalf("connection features V1 %v V2 %v, want %v %v", d.InternalTabletsRoutingV1, d.InternalTabletsRoutingV2, test.v1, test.v2)
			}
		})
	}
}
