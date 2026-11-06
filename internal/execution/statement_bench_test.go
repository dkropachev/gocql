// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/transport"
)

// The discovery LRU lock stays held throughout warm concurrent bindings and
// host-state selection. Unrelated schema/placement publication runs concurrently;
// neither may cause reprepare or serialize warm calls through the discovery LRU.
func BenchmarkWarmConcurrentBindings(b *testing.B) {
	for _, updates := range []bool{false, true} {
		for _, owned := range []bool{false, true} {
			b.Run(fmt.Sprintf("updates%t/owned%t", updates, owned), func(b *testing.B) {
				s := testSession(b)
				rt := InternalRuntimeFor(s)
				columns := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: cqltype.BlobType})
				tracker := rt.InternalSchemaTracker()
				deps := tracker.InternalDependencies("ks", columns, columns)
				hp := &InternalHostPrepared{InternalEpoch: deps.InternalSnapshot()}
				id := cqltype.UUID{15: 1}
				state := &InternalPreparedState{InternalSession: s, InternalProtocol: 4, InternalStatement: "SELECT value FROM ks.tbl WHERE value=?", InternalParameters: columns, InternalColumns: columns, InternalHosts: map[cqltype.UUID]*InternalHostPrepared{id: hp}, InternalDependencies: deps, InternalInitialEpoch: deps.InternalSnapshot(), InternalInfo: PreparedInfo{Keyspace: "ks", Table: "tbl", PartitionKeyIndexes: []int{0}}}
				prepared := new(Prepared[[]byte, cqltype.Bytes])
				PreparedData(prepared).InternalState = state
				PreparedData(prepared).InternalKind = cqltype.OrdinaryStatement
				PreparedData(prepared).InternalParams = codecTestMust(codec.OneParam(codec.BytesEncoder).Compile(columns))
				PreparedData(prepared).InternalRows = codec.OneColumn(codec.BlobCodec)
				host := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(host).InternalHostId = id
				conn := &transport.Conn{InternalHost: host}
				input := make([]byte, 4096)
				stop := make(chan struct{})
				var updater sync.WaitGroup
				if updates {
					updater.Add(1)
					go func() {
						defer updater.Done()
						for i := uint64(1); ; i++ {
							select {
							case <-stop:
								return
							default:
							}
							implCluster.InternalSessionInvalidatePreparedSchema(s, cluster.Event{Keyspace: "other", Object: "unrelated", Target: "TABLE"})
							p := implCluster.InternalSessionPeripheral(s)
							current := s.Metadata()
							info := model.RoutingInfo{Keyspace: "ks", Table: "table"}
							stamp := implCluster.InternalSessionCaptureTabletFeedback(s, info, current)
							hint := testHint(b, 0, 1, cqltype.Option[uint64]{Set: true, Value: i}, hTarget(1, 0))
							implCluster.InternalSessionPublishTabletFeedback(s, p, map[cluster.InternalV2TabletFeedbackKey]cluster.InternalV2PendingTabletFeedback{{InternalKeyspace: "ks", InternalTable: "table", InternalEnd: 1}: {InternalHint: hint, InternalStamp: stamp}}, false)
							runtime.Gosched()
						}
					}()
				}
				rt.InternalMu.Lock()
				b.ReportAllocs()
				b.SetBytes(4096)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						var bound *Bound[cqltype.Bytes]
						var e error
						if owned {
							bound, e = BindOwned(prepared, input)
						} else {
							bound, e = BindBorrowed(prepared, input)
						}
						if e != nil {
							b.Error(e)
							return
						}
						got, e := rt.InternalPrepareOnConn(context.Background(), conn, state, model.EncodedValues{}, false)
						if e != nil || got != hp {
							b.Error("unrelated reprepare", e)
							return
						}
						bound.Close()
						if e = bound.Wait(context.Background()); e != nil {
							b.Error(e)
							return
						}
					}
				})
				b.StopTimer()
				rt.InternalMu.Unlock()
				close(stop)
				updater.Wait()
			})
		}
	}
}
