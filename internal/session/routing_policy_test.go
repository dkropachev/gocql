// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/routing"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
	metadata2 "github.com/scylladb/gocql/v2/metadata"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

func TestRoutingPolicy(t *testing.T) {
	t.Run("Binding", func(t *testing.T) {
		t.Run("CapturesPartitionerAndBorrowedSpans", func(t *testing.T) {
			s := testSession(t)
			payload := []byte{1, 2, 3}
			old := &capturedPartitioner{expected: payload}
			newer := &capturedPartitioner{expected: payload}
			peripheral := s.Peripheral()
			peripheral.InternalMu.Lock()
			config := peripheral.InternalConfig
			peripheral.InternalMu.Unlock()
			config.Partitioners = map[string]routing2.TokenPartitioner{"old": old, "new": newer}
			s.ConfigureV2Peripheral(config)
			typ := codecTestMust(cql.ScalarType(cql.KindBlob))
			columns := cql.NewColumns(cql.ColumnSpec{Name: "key", Type: typ, Keyspace: "ks", Table: "table"})
			encoder := cql.OneParam(cql.BytesCodec)
			params := codecTestMust(encoder.Compile(columns))
			state := &InternalPreparedState{InternalSession: s, InternalProtocol: 4, InternalParameters: columns, partitioner: "old", InternalInfo: PreparedInfo{Keyspace: "ks", Table: "table", PartitionKeyIndexes: []int{0}}}
			prepared := func() *Prepared[cql.Bytes,
				cql.EmptyRow] {
				opaqueStorageValuexxxxxxxx := new(Prepared[cql.Bytes,
					cql.EmptyRow])
				PreparedData(opaqueStorageValuexxxxxxxx).InternalState = state
				PreparedData(opaqueStorageValuexxxxxxxx).params = params
				PreparedData(opaqueStorageValuexxxxxxxx).rows = cql.EmptyRowDecoder
				PreparedData(opaqueStorageValuexxxxxxxx).
					// Later metadata selects a different partitioner. Existing preparation and
					// binding still consume the canonical original parameter spans exactly once.
					plans = &InternalResultPlanCache[cql.EmptyRow]{}
				return opaqueStorageValuexxxxxxxx
			}()

			next := implRouting.InternalClonePlacementSnapshot(s.Metadata())
			routing.SnapshotData(next).InternalKeyspaces = map[string]*metadata2.Keyspace{"ks": metadata.InternalCloneV2Keyspace(routing.SnapshotData(s.Metadata()).InternalKeyspaces["ks"])}
			routing.SnapshotData(next).InternalKeyspaces["ks"].Tables["table"].Options.Partitioner = "new"
			peripheral.InternalSnapshot.Store(next)
			b := codecTestMust(BindBorrowed(prepared, cql.BorrowBytes(payload)))
			defer b.Close()
			if old.calls != 1 || newer.calls != 0 || !b.Routing().Token.Set || b.Routing().Token.Value != 77 {
				t.Fatal("binding refreshed captured partitioner", old.calls, newer.calls, b.Routing())
			}
		})
	})

	t.Run("PartitionerByTable", func(t *testing.T) {
		// A request hashes its key with the partitioner its table names, or
		// the cluster's; a partitioner the session does not know, or a table
		// it has no metadata for, gives no partitioner and so no token.
		s := testSession(t)
		peripheral := s.Peripheral()
		peripheral.InternalMu.Lock()
		config := peripheral.InternalConfig
		peripheral.InternalMu.Unlock()
		config.Partitioners = settings.DefaultConfig().Partitioners
		s.ConfigureV2Peripheral(config)
		next := implRouting.InternalClonePlacementSnapshot(s.Metadata())
		ks := metadata.InternalCloneV2Keyspace(routing.SnapshotData(next).InternalKeyspaces["ks"])
		ks.Tables["cdc"] = &metadata2.Table{Name: "cdc", Options: metadata2.TableOptions{Partitioner: "com.scylladb.dht.CDCPartitioner"}}
		ks.Tables["random"] = &metadata2.Table{Name: "random", Options: metadata2.TableOptions{Partitioner: "org.apache.cassandra.dht.RandomPartitioner"}}
		routing.SnapshotData(next).InternalKeyspaces = map[string]*metadata2.Keyspace{"ks": ks}
		routing.SnapshotData(next).InternalPartitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
		peripheral.InternalSnapshot.Store(next)
		for _, test := range []struct {
			table string
			want  routing2.TokenPartitioner
		}{
			{"table", routing2.Murmur3TokenPartitioner},
			{"cdc", routing2.CDCTokenPartitioner},
			{"random", nil},
			{"missing", nil},
		} {
			if got := s.CapturedRoutingPartitioner(s.PreparedPartitionerName("ks", test.table)); got != test.want {
				t.Errorf("table %s: partitioner %T, want %T", test.table, got, test.want)
			}
		}
	})

	t.Run("Latency", func(t *testing.T) {
		t.Run("ExplicitThresholdChangesDecision", func(t *testing.T) {
			policy := codecTestMust(implRouting.NewRoutingPolicy(routing2.Options{Latency: routing2.LatencyOptions{Enabled: true, ExclusionThreshold: 3, MinimumSamples: 1}, NoTokenAware: true, FixedReplicaOrder: true, NoLeaderAware: true})).(*implRouting.InternalRoutingPolicy)
			now := time.Now()
			for id, value := range map[byte]float64{1: float64(time.Millisecond), 2: float64(2500 * time.Microsecond), 3: float64(3500 * time.Microsecond)} {
				policy.InternalLatencies.Store(cql.UUID{15: id}, &implRouting.InternalRoutingLatency{InternalValue: value, InternalSamples: 1, InternalUpdated: now})
			}
			policy.Observe(routing2.AttemptEvent{Target: metadata2.Target{HostID: cql.UUID{15: 4}}, Started: now, Latency: time.Millisecond})
			decisions := policy.InternalDecisions.Load()
			if decisions == nil {
				t.Fatal("no latency decision")
			}
			if _, slow := decisions.InternalSlow[cql.UUID{15: 2}]; slow {
				t.Fatal("custom threshold3 was ignored")
			}
			if _, slow := decisions.InternalSlow[cql.UUID{15: 3}]; !slow {
				t.Fatal("custom threshold3 did not exclude slower host")
			}
		})

		t.Run("LargePublicationInterval", func(t *testing.T) {
			policy := codecTestMust(implRouting.NewRoutingPolicy(routing2.Options{Latency: routing2.LatencyOptions{Enabled: true, MinimumSamples: 1, UpdateInterval: time.Duration(1<<63 - 1)}, NoTokenAware: true, FixedReplicaOrder: true, NoLeaderAware: true})).(*implRouting.InternalRoutingPolicy)
			now := time.Now()
			policy.Observe(routing2.AttemptEvent{Target: metadata2.Target{HostID: cql.UUID{15: 1}}, Started: now, Latency: time.Millisecond})
			first := policy.InternalDecisions.Load()
			policy.Observe(routing2.AttemptEvent{Target: metadata2.Target{HostID: cql.UUID{15: 2}}, Started: now, Latency: 3 * time.Millisecond})
			if policy.InternalDecisions.Load() != first {
				t.Fatal("valid MaxDuration UpdateInterval republished before deadline without a new minimum")
			}
		})

		t.Run("LargeRetryWholePlan", func(t *testing.T) {
			period := time.Duration(1<<63 - 1)
			policy := codecTestMust(implRouting.NewRoutingPolicy(routing2.Options{Latency: routing2.LatencyOptions{Enabled: true, MinimumSamples: 1, RetryPeriod: period, UpdateInterval: time.Hour}, FixedReplicaOrder: true, NoLeaderAware: true})).(*implRouting.InternalRoutingPolicy)
			metadata := v2TestMetadata()
			slow := cql.UUID{15: 1}
			healthy := cql.UUID{15: 2}
			routing.SnapshotData(metadata).InternalRanges["ks"][0].Replicas = []metadata2.Target{{HostID: slow}}
			now := time.Now()
			old := now.Add(-period).Add(-time.Minute)
			policy.Observe(routing2.AttemptEvent{Target: metadata2.Target{HostID: healthy}, Started: old, Latency: time.Millisecond})
			policy.Observe(routing2.AttemptEvent{Target: metadata2.Target{HostID: slow}, Started: old, Latency: 10 * time.Millisecond})
			policy.InternalDecisions.Store(&routing.InternalRoutingDecisions{InternalSlow: map[cql.UUID]time.Time{slow: now.Add(-time.Minute)}})
			info := routing2.Info{Keyspace: "ks", Table: "table", Token: cql.Option[int64]{Set: true}, Kind: cql.OrdinaryStatement}
			start := make(chan struct{})
			var wg sync.WaitGroup
			var probes atomic.Int32
			for range 32 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					target, ok := implRouting.InternalNextTarget(implRouting.NewPlan(policy, info, metadata))
					if !ok {
						t.Error("missing target")
					}
					if target.HostID == slow {
						probes.Add(1)
					}
				}()
			}
			close(start)
			wg.Wait()
			if probes.Load() != 1 {
				t.Fatalf("valid MaxDuration retry admitted %d probes, want1", probes.Load())
			}
		})

		t.Run("ThresholdExpiryAndSamples", func(t *testing.T) {
			policy := codecTestMust(implRouting.NewRoutingPolicy(routing2.Options{Latency: routing2.LatencyOptions{Enabled: true, MinimumSamples: 2, RetryPeriod: time.Minute}, NoTokenAware: true, FixedReplicaOrder: true, NoLeaderAware: true})).(*implRouting.InternalRoutingPolicy)
			now := time.Now()
			for id, value := range map[byte]float64{1: float64(time.Millisecond), 2: float64(2 * time.Millisecond), 3: float64(3 * time.Millisecond), 4: float64(9 * time.Millisecond)} {
				samples := 2
				if id == 4 {
					samples = 1
				}
				policy.InternalLatencies.Store(cql.UUID{15: id}, &implRouting.InternalRoutingLatency{InternalValue: value, InternalSamples: samples, InternalUpdated: now})
			}
			policy.Observe(routing2.AttemptEvent{Target: metadata2.Target{HostID: cql.UUID{15: 5}}, Started: now, Latency: time.Millisecond})
			decisions := policy.InternalDecisions.Load()
			if decisions == nil {
				t.Fatal("no decisions")
			}
			if _, ok := decisions.InternalSlow[cql.UUID{15: 2}]; ok {
				t.Fatal("threshold equality excluded")
			}
			if _, ok := decisions.InternalSlow[cql.UUID{15: 3}]; !ok {
				t.Fatal("strictly slow host retained")
			}
			if _, ok := decisions.InternalSlow[cql.UUID{15: 4}]; ok {
				t.Fatal("insufficient samples excluded")
			}
			plan := &implRouting.InternalLazyRoutingPlan{InternalSlow: decisions, InternalNow: now.Add(2 * time.Minute)}
			if plan.InternalPenalty(metadata2.Target{HostID: cql.UUID{15: 3}}) {
				t.Fatal("penalty did not expire")
			}
			if !implRouting.InternalRoutingLatencySample(cqltype.NewFrameError("bad response")) || implRouting.InternalRoutingLatencySample(context.Canceled) {
				t.Fatal("latency error classification")
			}
		})
	})

	t.Run("Plan", func(t *testing.T) {
		t.Run("AllDisconnectedAndMissingFollowers", func(t *testing.T) {
			m := v2TestMetadata()
			for i := range routing.SnapshotData(m).InternalHosts {
				routing.SnapshotData(m).InternalHosts[i].Connected = false
			}
			policy := codecTestMust(implRouting.NewRoutingPolicy(routing2.Options{Datacenter: "a", FixedReplicaOrder: true, NoLeaderAware: true}))
			if ids := v2PlanIDs(implRouting.NewPlan(policy, routing2.Info{Kind: cql.OrdinaryStatement}, m)); !bytes.Equal(ids, []byte{2, 4, 1}) {
				t.Fatal("enabled disconnected fallback", ids)
			}
			m = v2TestMetadata()
			routing.SnapshotData(m).InternalKeyspaces["ks"].ConsistencyMode = "global"
			h := testHint(t, 0, 100, cql.Option[uint64]{Set: true}, hTarget(2, 1), hTarget(99, 2), hTarget(1, 1))
			r, _ := implRouting.InternalUpdateTabletRange(nil, h)
			routing.SnapshotData(m).InternalPlacements = routing.SnapshotData(m).InternalPlacements.Put(implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"}, implRouting.InternalTabletPlacement{InternalRanges: r})
			leader := codecTestMust(implRouting.NewRoutingPolicy(routing2.Options{FixedReplicaOrder: true}))
			first, _ := implRouting.InternalNextTarget(implRouting.NewPlan(leader, routing2.Info{Keyspace: "ks", Table: "table", Token: cql.Option[int64]{Set: true, Value: 1}, Consistency: cql.Quorum}, m))
			if first.HostID[15] != 2 {
				t.Fatal("missing follower hid resolved leader")
			}
		})

		t.Run("VnodeDistinctNodesAndRackBudget", func(t *testing.T) {
			hosts := []*endpoint.HostInfo{func() *endpoint.HostInfo {
				opaqueStorageValuexxx := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexxx).InternalHostId = cql.UUID{15: 1}
				implEndpoint.HostInfoData(opaqueStorageValuexxx).InternalDataCenter = "a"
				implEndpoint.HostInfoData(opaqueStorageValuexxx).InternalRack = "r1"
				implEndpoint.HostInfoData(opaqueStorageValuexxx).InternalTokens = []string{"0", "1"}
				return opaqueStorageValuexxx
			}(), func() *endpoint.HostInfo {
				opaqueStorageValuexxxx := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalHostId = cql.UUID{15: 2}
				implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalDataCenter = "a"
				implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalRack = "r1"
				implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalTokens = []string{"2"}
				return opaqueStorageValuexxxx
			}(), func() *endpoint.HostInfo {
				opaqueStorageValuexxxxx := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexxxxx).InternalHostId = cql.UUID{15: 3}
				implEndpoint.HostInfoData(opaqueStorageValuexxxxx).InternalDataCenter = "a"
				implEndpoint.HostInfoData(opaqueStorageValuexxxxx).InternalRack = "r2"
				implEndpoint.HostInfoData(opaqueStorageValuexxxxx).InternalTokens = []string{"3"}
				return opaqueStorageValuexxxxx
			}(), func() *endpoint.HostInfo {
				opaqueStorageValuexxxxxx := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxx).InternalHostId = cql.UUID{15: 4}
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxx).InternalDataCenter = "a"
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxx).InternalRack = "r3"
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxx).InternalTokens = []string{"4"}
				return opaqueStorageValuexxxxxx
			}(), func() *endpoint.HostInfo {
				opaqueStorageValuexxxxxxx := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxx).InternalHostId = cql.UUID{15: 5}
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxx).InternalDataCenter = "b"
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxx).InternalRack = "r1"
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxx).InternalTokens = []string{"5"}
				return opaqueStorageValuexxxxxxx
			}(),
			}
			ring := codecTestMust(cluster.InternalNewTokenRing("Murmur3Partitioner", hosts))
			for _, test := range []struct {
				strategy string
				options  map[string]string
				want     []byte
			}{
				{"NetworkTopologyStrategy", map[string]string{"a": "4", "b": "1"}, []byte{1, 2, 3, 4, 5}},
				{"NetworkTopologyStrategy", map[string]string{"a": "3"}, []byte{1, 3, 4}},
				{"SimpleStrategy", map[string]string{"replication_factor": "999999999999999999"}, []byte{1, 2, 3, 4, 5}},
				{"SimpleStrategy", map[string]string{"replication_factor": "0"}, nil},
				{"UnknownStrategy", nil, []byte{1}},
			} {
				ranges, err := cluster.InternalBuildV2VnodeRanges(ring, &metadata2.Keyspace{StrategyClass: test.strategy, StrategyOptions: test.options})
				if err != nil {
					t.Fatal(err)
				}
				var ids []byte
				for _, r := range ranges[0].Replicas {
					ids = append(ids, r.HostID[15])
				}
				if !bytes.Equal(ids, test.want) {
					t.Fatal(test.strategy, ids, test.want)
				}
			}
		})
	})
}
