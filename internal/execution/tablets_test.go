/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package execution

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/model"
	implModel "github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/routing"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestTablets(t *testing.T) {
	t.Run("Feedback", func(t *testing.T) {
		t.Run("ConcurrentPlans", func(t *testing.T) {
			s := testSession(t)
			info := implModel.RoutingInfo{Keyspace: "ks", Table: "table", Token: implCqltype.Option[int64]{Value: 1, Set: true}, Conditional: true}
			policy := codecTestMust(implRouting.NewRoutingPolicy(implRouting.RoutingOptions{TokenAware: true, ShuffleReplicas: true}))
			var wg sync.WaitGroup
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < 200; j++ {
						m := s.Metadata()
						p := implRouting.NewPlan(policy, info, m)
						_ = v2PlanIDs(p)
						_ = routing.InternalMetadataSnapshotTabletVersionBlock(m, info)
					}
				}()
			}
			for i := 0; i < 100; i++ {
				implCluster.InternalSessionQueueTabletFeedback(s, testHint(t, 0, 100, implCqltype.Option[uint64]{Value: uint64(i), Set: true}, hTarget(byte(i%3+1), 0)), implCluster.InternalSessionCaptureTabletFeedback(s, info, s.Metadata()))
				testDrain(s)
			}
			wg.Wait()
		})

		t.Run("EnqueueDoesNotWaitForPublication", func(t *testing.T) {
			s := testSession(t)
			p := implCluster.InternalSessionPeripheral(s)
			info := implModel.RoutingInfo{Keyspace: "ks", Table: "table"}
			stamp := implCluster.InternalSessionCaptureTabletFeedback(s, info, s.Metadata())
			h := testHint(t, 0, 100, implCqltype.Option[uint64]{}, hTarget(1, 0))
			p.InternalMu.Lock()
			done := make(chan struct{})
			go func() { implCluster.InternalSessionQueueTabletFeedback(s, h, stamp); close(done) }()
			select {
			case <-done:
				p.InternalMu.Unlock()
			case <-time.After(time.Second):
				p.InternalMu.Unlock()
				<-done
				t.Fatal("successful feedback enqueue waited for publication mutex")
			}
		})

		t.Run("NegotiationVersionAndFailedExecute", func(t *testing.T) {
			for _, both := range []bool{false, true} {
				supported := map[string][]string{endpoint.InternalTabletsRoutingV1: {}}
				if both {
					supported[endpoint.InternalTabletsRoutingV2] = []string{}
				}
				exts := endpoint.InternalTypedTabletExtensions(endpoint.InternalParseCQLProtocolExtensions(supported, 4, logging.InternalNopLogger{}), supported)
				c := &transport.Conn{InternalVersion: 4, InternalCqlProtoExts: exts}
				c.InternalInitFramerCache()
				if c.InternalFramers.InternalDefaults.InternalTabletsRoutingV2 != both || c.InternalFramers.InternalDefaults.InternalTabletsRoutingV1 == both {
					t.Fatal("V2 did not take precedence")
				}
				for _, ext := range exts {
					if _, ok := ext.InternalSerialize()[endpoint.InternalTabletsRoutingV1]; ok && both {
						t.Fatal("both extensions negotiated")
					}
				}
			}
			s := testSession(t)
			info := implModel.RoutingInfo{Keyspace: "ks", Table: "table", Token: implCqltype.Option[int64]{Set: true, Value: 1}}
			h := testHint(t, 0, 100, implCqltype.Option[uint64]{Set: true, Value: 0xfedcba9876543210}, hTarget(1, 1))
			stamp := implCluster.InternalSessionCaptureTabletFeedback(s, info, s.Metadata())
			// applyTypedFeedbackStamped is deliberately before execute-error/UNPREPARED
			// interpretation; a response page can carry payload independently of rows.
			page := &transport.InternalResponsePage{InternalResponseData: implModel.ResponseData{Extras: &model.ResponseExtras{Payload: []implModel.PayloadEntry{{Key: "tablets-routing-v2", Value: implCqltype.Nullable[implCqltype.Bytes]{Valid: true, Value: implCqltype.BorrowBytes(testPayload(h))}}}}}}
			implCluster.InternalSessionApplyTypedFeedbackStamped(s, page, info, stamp, transport.InternalFramerConfig{InternalTabletsRoutingV1: true, InternalTabletsRoutingV2: true})
			testDrain(s)
			m := s.Metadata()
			for i := 0; i < 1024; i++ {
				b := routing.InternalMetadataSnapshotTabletVersionBlock(m, info)
				index := b >> 4
				if b&15 != byte(uint64(0xfedcba9876543210)>>uint(index*4)&15) {
					t.Fatal("wrong nibble direction", b)
				}
			}
			if b := routing.InternalMetadataSnapshotTabletVersionBlock(m, implModel.RoutingInfo{}); b != 0 {
				t.Fatal("absent identity/token hint is not zero")
			}
			// A malformed V2 body never falls back to the simultaneously supplied V1.
			page.Extras.Payload = append([]implModel.PayloadEntry{{Key: "tablets-routing-v1", Value: implCqltype.Nullable[implCqltype.Bytes]{Valid: true, Value: implCqltype.BorrowBytes(testPayload(testHint(t, 100, 200, implCqltype.Option[uint64]{}, hTarget(2, 0))))}}}, page.Extras.Payload...)
			page.Extras.Payload[1].Value.Value = implCqltype.BorrowBytes([]byte{0})
			implCluster.InternalSessionApplyTypedFeedbackStamped(s, page, info, implCluster.InternalSessionCaptureTabletFeedback(s, info, m), transport.InternalFramerConfig{InternalTabletsRoutingV1: true, InternalTabletsRoutingV2: true})
			testDrain(s)
			if s.Metadata() != m {
				t.Fatal("malformed V2 fell back to V1")
			}
		})
	})

	t.Run("Payload", func(t *testing.T) {
		t.Run("V3NegotiatedEnvelope", func(t *testing.T) {
			for _, streaming := range []bool{false, true} {
				for _, negotiated := range []bool{false, true} {
					t.Run(fmt.Sprintf("stream=%t/negotiated=%t", streaming, negotiated), func(t *testing.T) {
						s, c, peer := rowStreamSession(t, 3, nil)
						c.InternalFramers.InternalDefaults.InternalTabletsRoutingV1 = negotiated
						done := make(chan struct{})
						go func() {
							defer close(done)
							id := typedTestReadRequest(t, c, peer)
							body := append([]byte{0, 0}, runtimeRowsBody(nil, "value")...)
							frame := typedTestResponse(t, 3, id, body)
							frame[1] = 4
							_, _ = peer.Write(frame)
						}()
						b := runtimeQuery(t)
						defer b.Close()
						ctx, cancel := context.WithTimeout(context.Background(), time.Second)
						defer cancel()
						var err error
						if streaming {
							var cursor *RowStream[[]byte]
							cursor, err = RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
							if err == nil {
								dst := make([]byte, 0, 10)
								if !cursor.NextInto(&dst) || string(dst) != "value" {
									t.Fatal(cursor.Err())
								}
								cursor.NextInto(&dst)
								cursor.Close()
							}
						}
						if !streaming {
							var page *Page[cqltype.Bytes]
							page, err = RunPage(ctx, s, b, model.StartPage(), BoundOptions{})
							if err == nil {
								var row cqltype.Bytes
								if !page.NextInto(&row) || string(row.OwnedBytes()) != "value" {
									t.Fatal(page.Err())
								}
								page.Close()
							}
						}
						if (err == nil) != negotiated {
							t.Fatal("negotiated flag fence", err)
						}
						<-done
					})
				}
			}
		})

		t.Run("Wire", func(t *testing.T) {
			target := implModel.Target{HostID: implCqltype.UUID{15: 7}, ShardID: implCqltype.Option[uint32]{Value: math.MaxInt32, Set: true}}
			for _, v2 := range []bool{false, true} {
				h := testHint(t, math.MinInt64, math.MaxInt64, implCqltype.Option[uint64]{Value: 0xfedcba9876543210, Set: v2}, target)
				raw := testPayload(h)
				for split := 0; split <= len(raw); split++ {
					chain := typedTestChain(t, raw[:split], raw[split:])
					parsed, err := transport.InternalParseTypedTabletPayload(cqltype.BytesView(chain, wire.Ref{}), "ks", "table", v2)
					if err != nil || !implRouting.InternalSameTabletObservation(&implRouting.InternalTabletRange{InternalSpan: implRouting.TokenRange{Start: h.FirstToken(), End: h.LastToken(), Replicas: []implModel.Target{target}}, InternalVersion: implCqltype.Option[uint64]{Value: 0xfedcba9876543210, Set: v2}}, parsed) {
						t.Fatalf("v2=%t split=%d: %v", v2, split, err)
					}
				}
				for n := 0; n < len(raw); n++ {
					if _, err := transport.InternalParseTypedTabletPayload(implCqltype.BorrowBytes(raw[:n]), "ks", "table", v2); err == nil {
						t.Fatalf("accepted truncated prefix %d v2=%t", n, v2)
					}
				}
				for _, offset := range []int{4, 16} {
					bad := append([]byte(nil), raw...)
					copy(bad[offset:offset+8], raw[4:12])
					if _, err := transport.InternalParseTypedTabletPayload(implCqltype.BorrowBytes(bad), "ks", "table", v2); offset == 16 && err == nil {
						t.Fatal("accepted empty range")
					}
				}
				// A contiguous payload takes a byte parser and a split one the
				// cursor parser; both must agree on every corrupted byte.
				for offset := range raw {
					for _, v := range []byte{0, 1, 0x7f, 0xff, raw[offset] + 1} {
						bad := append([]byte(nil), raw...)
						bad[offset] = v
						got, gotErr := transport.InternalParseTypedTabletPayload(implCqltype.BorrowBytes(bad), "ks", "table", v2)
						want, wantErr := transport.InternalParseTypedTabletPayload(cqltype.BytesView(typedTestChain(t, bad[:1], bad[1:]), wire.Ref{}), "ks", "table", v2)
						if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) || !reflect.DeepEqual(got, want) {
							t.Fatalf("v2=%t offset=%d value=%#x: contiguous %v %v, split %v %v", v2, offset, v, got, gotErr, want, wantErr)
						}
					}
				}
				bad := append([]byte(nil), raw...)
				binary.BigEndian.PutUint32(bad[60:64], math.MaxUint32)
				if _, err := transport.InternalParseTypedTabletPayload(implCqltype.BorrowBytes(bad), "ks", "table", v2); err == nil {
					t.Fatal("accepted negative shard")
				}
			}
			for _, proto := range []byte{3, 4, 5} {
				for _, kind := range []string{"query", "execute", "batch", "prepare"} {
					c := &transport.Conn{InternalVersion: proto}
					c.InternalInitFramerCache()
					r := &implModel.AttemptRequest{Statement: "SELECT * FROM ks.table", Consistency: implCqltype.One, TabletVersionBlock: 0xa7}
					switch kind {
					case "execute":
						r.PreparedID = []byte{1}
						r.ResultMetadataID = []byte{2}
					case "prepare":
						r.Prepare = true
					case "batch":
						r.Batch = &implModel.TypedBatchRequest{Kind: implCqltype.LoggedBatch}
					}
					flatten := func(chain wire.Chain) []byte {
						raw := typedTestBytes(t, chain)
						if proto < 5 {
							return raw
						}
						reader := bytes.NewReader(raw)
						header, err := segment.ReadHeader(reader, false)
						if err != nil {
							t.Fatal(err)
						}
						p, err := segment.ReadOwnedPayload(reader, header, nil)
						if err != nil {
							t.Fatal(err)
						}
						defer p.Release()
						return typedTestBytes(t, p.Chain())
					}
					base, err := c.InternalBuildTypedRequest(r, 23)
					if err != nil {
						t.Fatal(err)
					}
					plain := flatten(base)
					c.InternalFramers.InternalDefaults.InternalTabletsRoutingV2 = true
					c.InternalRefreshNativeProtocol()
					extended, err := c.InternalBuildTypedRequest(r, 23)
					if err != nil {
						t.Fatal(err)
					}
					got := flatten(extended)
					if kind == "execute" {
						if len(got) != len(plain)+1 || got[len(got)-1] != 0xa7 || !bytes.Equal(got[9:len(got)-1], plain[9:]) || binary.BigEndian.Uint32(got[5:9]) != binary.BigEndian.Uint32(plain[5:9])+1 {
							t.Fatalf("v%d execute block malformed", proto)
						}
					} else if !bytes.Equal(got, plain) {
						t.Fatalf("v%d %s gained version block", proto, kind)
					}
				}
			}
		})
	})

	t.Run("Placement", func(t *testing.T) {
		t.Run("FencesSharingAndOverflow", func(t *testing.T) {
			s := testSession(t)
			p := implCluster.InternalSessionPeripheral(s)
			info := implModel.RoutingInfo{Keyspace: "ks", Table: "table"}
			first := s.Metadata()
			stamp := implCluster.InternalSessionCaptureTabletFeedback(s, info, first)
			h := testHint(t, 0, 100, implCqltype.Option[uint64]{Value: 7, Set: true}, implModel.Target{HostID: routing.SnapshotData(first).InternalHosts[0].ID, ShardID: implCqltype.Option[uint32]{Set: true}})
			implCluster.InternalSessionQueueTabletFeedback(s, h, stamp)
			testDrain(s)
			a := s.Metadata()
			if a == first || routing.SnapshotData(a).InternalHostIndex != routing.SnapshotData(first).InternalHostIndex || routing.SnapshotData(a).InternalKeyspaces["ks"] != routing.SnapshotData(first).InternalKeyspaces["ks"] {
				t.Fatal("placement rebuilt host/schema graph")
			}
			if root, _ := routing.InternalMetadataSnapshotTabletRoot(first, implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"}); root != nil {
				t.Fatal("mutated retained snapshot")
			}
			implCluster.InternalSessionQueueTabletFeedback(s, h, implCluster.InternalSessionCaptureTabletFeedback(s, info, a))
			testDrain(s)
			if s.Metadata() != a {
				t.Fatal("duplicate advanced snapshot")
			}
			// A numerically smaller opaque version is a valid update. The stale request
			// captured before the first update cannot overwrite its overlapping range.
			moved := testHint(t, 0, 100, implCqltype.Option[uint64]{Value: 1, Set: true}, implModel.Target{HostID: routing.SnapshotData(first).InternalHosts[1].ID, ShardID: implCqltype.Option[uint32]{Value: 1, Set: true}})
			implCluster.InternalSessionQueueTabletFeedback(s, moved, stamp)
			testDrain(s)
			if s.Metadata() != a {
				t.Fatal("stale response overwrote newer range")
			}
			implCluster.InternalSessionQueueTabletFeedback(s, moved, implCluster.InternalSessionCaptureTabletFeedback(s, info, a))
			testDrain(s)
			b := s.Metadata()
			r, _ := routing.InternalMetadataSnapshotTabletRoot(b, implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"})
			if n := implRouting.InternalTabletRangeFor(r, 1); n == nil || n.InternalValue.InternalVersion.Value != 1 {
				t.Fatal("ordered opaque versions numerically")
			}
			// Disjoint observations captured together merge, preserving the old record.
			left := implCluster.InternalSessionCaptureTabletFeedback(s, info, b)
			for _, bounds := range [][2]int64{{100, 200}, {200, 300}} {
				implCluster.InternalSessionQueueTabletFeedback(s, testHint(t, bounds[0], bounds[1], implCqltype.Option[uint64]{}, hTarget(3, 2)), left)
			}
			testDrain(s)
			c := s.Metadata()
			newRoot, _ := routing.InternalMetadataSnapshotTabletRoot(c, implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"})
			if implRouting.InternalTabletRangeFor(newRoot, 1).InternalValue != implRouting.InternalTabletRangeFor(r, 1).InternalValue || implRouting.InternalTabletRangeFor(newRoot, 150) == nil || implRouting.InternalTabletRangeFor(newRoot, 250) == nil {
				t.Fatal("disjoint ranges were copied or lost")
			}
			implCluster.
				// A split removes the old overlapping range entirely; adjacent ranges survive.
				InternalSessionQueueTabletFeedback(s, testHint(t, 10, 50, implCqltype.Option[uint64]{}, hTarget(2, 1)), implCluster.InternalSessionCaptureTabletFeedback(s, info, c))
			testDrain(s)
			root, _ := routing.InternalMetadataSnapshotTabletRoot(s.Metadata(), implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"})
			if implRouting.InternalTabletRangeFor(root, 1) != nil || implRouting.InternalTabletRangeFor(root, 51) != nil || implRouting.InternalTabletRangeFor(root, 150) == nil {
				t.Fatal("split retained old fragments or removed adjacent range")
			}
			beforeEpoch := s.Metadata()
			oldStamp := implCluster.InternalSessionCaptureTabletFeedback(s, info, beforeEpoch)
			lazy := implRouting.NewPlan(codecTestMust(implRouting.NewRoutingPolicy(implRouting.RoutingOptions{TokenAware: true})), implModel.RoutingInfo{Keyspace: "ks", Table: "table", Token: implCqltype.Option[int64]{Value: 20, Set: true}}, beforeEpoch)

			p.InternalMu.Lock()
			p.InternalBumpTableRevisionLocked(implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"})
			p.InternalMu.Unlock()
			implCluster.InternalSessionQueueTabletFeedback(s, h, oldStamp)
			testDrain(s)
			if s.Metadata() != beforeEpoch {
				t.Fatal("table epoch fence failed")
			}
			// Even before snapshot publication, a source drop/recreate rejects old replies.
			before := s.Metadata()
			sourceStamp := implCluster.InternalSessionCaptureTabletFeedback(s, info, before)
			dropped := implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGet()["ks"].Clone()
			delete(dropped.Tables, "table")
			implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks", dropped)
			implCluster.InternalSessionQueueTabletFeedback(s, h, sourceStamp)
			testDrain(s)
			if s.Metadata() != before {
				t.Fatal("dropped source table resurrected")
			}
			implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks", routing.SnapshotData(before).InternalKeyspaces["ks"])
			ks := implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGet()["ks"]
			ks = ks.Clone()
			table := *ks.Tables["table"]
			table.ID = "identity-2"
			ks.Tables["table"] = &table
			implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks", ks)
			implCluster.InternalSessionQueueTabletFeedback(s, h, sourceStamp)
			testDrain(s)
			if s.Metadata() != before {
				t.Fatal("source identity fence failed")
			}
			implCluster.
				// Restore the known source, then saturation invalidates immediately, before
				// a worker can publish, with bounded queue ownership and later relearning.
				MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks", routing.SnapshotData(before).InternalKeyspaces["ks"])
			for i := 0; i < 1025; i++ {
				implCluster.InternalSessionQueueTabletFeedback(s, testHint(t, int64(i*2), int64(i*2+1), implCqltype.Option[uint64]{}, hTarget(1, 0)), implCluster.InternalSessionCaptureTabletFeedback(s, info, before))
			}
			if target, ok := implRouting.InternalNextTarget(lazy); !ok || target.ShardID.Set {
				t.Fatal("preexisting plan used invalidated placement", target)
			}
			if replicas, _ := before.Replicas("ks", "table", 150); len(replicas) != 0 {
				t.Fatal("overflow retained known-stale mapping")
			}
			p.InternalFeedbackMu.Lock()
			if len(p.InternalFeedback) > 1024 {
				t.Fatal("unbounded queue")
			}
			for _, u := range p.InternalFeedback {
				if u.InternalStamp.InternalSnapshot != nil {
					t.Fatal("queue retained cluster snapshot")
				}
			}
			p.InternalFeedbackMu.Unlock()
			testDrain(s)
			implCluster.InternalSessionQueueTabletFeedback(s, h, implCluster.InternalSessionCaptureTabletFeedback(s, info, s.Metadata()))
			testDrain(s)
			if got, _ := s.Metadata().Replicas("ks", "table", 1); len(got) != 1 {
				t.Fatal("overflow could not relearn")
			}
		})
	})

	t.Run("Plan", func(t *testing.T) {
		t.Run("LeaderLocalityLWTAndFallback", func(t *testing.T) {
			m := v2TestMetadata()
			routing.SnapshotData(m).InternalKeyspaces["ks"].ConsistencyMode = "global"
			k := implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"}
			set := func(targets ...implModel.Target) {
				h := testHint(t, 0, 100, implCqltype.Option[uint64]{Value: 9, Set: true}, targets...)
				r, _ := implRouting.InternalUpdateTabletRange(nil, h)
				routing.SnapshotData(m).InternalPlacements = implRouting.InternalPlacementPut(routing.SnapshotData(m).InternalPlacements, k, implRouting.InternalTabletPlacement{InternalRanges: r}, implRouting.InternalComparePlacementTable)
			}
			set(hTarget(3, 1), hTarget(2, 2), hTarget(1, 3))
			info := implModel.RoutingInfo{Keyspace: "ks", Table: "table", Token: implCqltype.Option[int64]{Value: 1, Set: true}, Consistency: implCqltype.LocalQuorum, Conditional: true}
			policy := codecTestMust(implRouting.NewRoutingPolicy(implRouting.RoutingOptions{TokenAware: true, LeaderAware: true, Datacenter: "a", Rack: "r1", AllowRemote: true, ShuffleReplicas: true}))
			for i := 0; i < 10; i++ {
				plan := implRouting.NewPlan(policy, info, m)
				first, ok := implRouting.InternalNextTarget(plan)
				if !ok || first.HostID != routing.SnapshotData(m).InternalHosts[2].ID || !first.StrictShard || first.ShardID.Value != 1 {
					t.Fatal("eligible remote leader was not strict first", first)
				}
				if got := v2PlanIDs(plan); !bytes.Equal(got, []byte{1, 2, 4}) {
					t.Fatal("LWT locality order", got)
				}
			}
			for _, test := range []struct {
				name    string
				change  func()
				want    byte
				restore func()
			}{
				{"one", func() { info.Consistency = implCqltype.One }, 1, func() { info.Consistency = implCqltype.LocalQuorum }},
				{"local-one", func() { info.Consistency = implCqltype.LocalOne }, 1, func() { info.Consistency = implCqltype.LocalQuorum }},
				{"unknown-original-leader", func() { set(hTarget(99, 1), hTarget(2, 2), hTarget(1, 3)) }, 1, func() { set(hTarget(3, 1), hTarget(2, 2), hTarget(1, 3)) }},
				{"disconnected-leader", func() { routing.SnapshotData(m).InternalHosts[2].Connected = false }, 1, func() { routing.SnapshotData(m).InternalHosts[2].Connected = true }},
				{"disabled-leader", func() { routing.SnapshotData(m).InternalHosts[2].Enabled = false }, 1, func() { routing.SnapshotData(m).InternalHosts[2].Enabled = true }},
				{"invalid-leader-shard", func() { set(hTarget(3, 999), hTarget(2, 2), hTarget(1, 3)) }, 1, func() { set(hTarget(3, 1), hTarget(2, 2), hTarget(1, 3)) }},
			} {
				t.Run(test.name, func(t *testing.T) {
					test.change()
					defer test.restore()
					first, _ := implRouting.InternalNextTarget(implRouting.NewPlan(policy, info, m))
					if first.HostID[15] != test.want {
						t.Fatal(first)
					}
				})
			}
			local := codecTestMust(implRouting.NewRoutingPolicy(implRouting.RoutingOptions{TokenAware: true, LeaderAware: true, Datacenter: "a", Rack: "r1"}))
			if ids := v2PlanIDs(implRouting.NewPlan(local, info, m)); bytes.Contains(ids, []byte{3}) {
				t.Fatal("excluded remote leader admitted", ids)
			}
			// Explicit latency penalties apply to leaders and LWT without admitting hosts.
			p := policy.(*implRouting.InternalRoutingPolicy)
			p.InternalDecisions.Store(&implRouting.InternalRoutingDecisions{InternalSlow: map[implCqltype.UUID]time.Time{routing.SnapshotData(m).InternalHosts[2].ID: time.Now().Add(time.Hour)}})
			if got := v2PlanIDs(implRouting.NewPlan(p, info, m)); !bytes.Equal(got, []byte{1, 2, 4, 3}) {
				t.Fatal("penalized leader order", got)
			}
			// Preserve distinct explicit shards, but suppress host-only fallback duplicates.
			p.InternalDecisions.Store(nil)
			set(hTarget(1, 0), hTarget(1, 1), hTarget(2, 2))
			ids := v2PlanIDs(implRouting.NewPlan(p, info, m))
			if !bytes.Equal(ids, []byte{1, 1, 2, 4, 3}) {
				t.Fatal("shard destinations deduplicated by host", ids)
			}
		})
	})
}
