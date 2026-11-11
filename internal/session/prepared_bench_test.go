/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package session

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
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func BenchmarkPrepared(b *testing.B) {
	// The discovery LRU lock stays held throughout warm concurrent bindings and
	// host-state selection. Unrelated schema/placement publication runs concurrently;
	// neither may cause reprepare or serialize warm calls through the discovery LRU.
	b.Run("WarmConcurrentBindings", func(b *testing.B) {
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
					PreparedData(prepared).kind = cqltype.OrdinaryStatement
					PreparedData(prepared).params = codecTestMust(codec.OneParam(codec.BytesEncoder).Compile(columns))
					PreparedData(prepared).rows = codec.OneColumn(codec.BlobCodec)
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
								s.InvalidatePreparedSchema(cluster.Event{Keyspace: "other", Object: "unrelated", Target: "TABLE"})
								p := s.Peripheral()
								current := s.Metadata()
								info := model.RoutingInfo{Keyspace: "ks", Table: "table"}
								stamp := s.CaptureTabletFeedback(info, current)
								hint := testHint(b, 0, 1, cqltype.Option[uint64]{Set: true, Value: i}, hTarget(1, 0))
								s.PublishTabletFeedback(p, map[cluster.InternalV2TabletFeedbackKey]cluster.InternalV2PendingTabletFeedback{{InternalKeyspace: "ks", InternalTable: "table", InternalEnd: 1}: {InternalHint: hint, InternalStamp: stamp}}, false)
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
							got, e := rt.prepareOnConn(context.Background(), conn, state, model.EncodedValues{}, false)
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
	})

	b.Run("WarmSelection", func(b *testing.B) {
		s := func() *cluster.Core {
			opaqueStorageValue := new(cluster.Core)
			implCluster.SessionData(opaqueStorageValue).InternalLogger = logging.Nop()
			return opaqueStorageValue
		}()
		columns := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: cqltype.BlobType})
		tracker := InternalRuntimeFor(s).InternalSchemaTracker()
		deps := tracker.InternalDependencies("ks", cqltype.NewColumns(), columns)
		hp := &InternalHostPrepared{InternalEpoch: deps.InternalSnapshot()}
		id := cqltype.UUID{15: 1}
		state := &InternalPreparedState{InternalHosts: map[cqltype.UUID]*InternalHostPrepared{id: hp}, InternalDependencies: deps}
		conn := &transport.Conn{InternalHost: func() *endpoint.HostInfo {
			opaqueStorageValuex := new(endpoint.HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValuex).InternalHostId = id
			return opaqueStorageValuex
		}()}
		rt := InternalRuntimeFor(s)
		rt.InternalMu.Lock()
		defer rt.InternalMu.Unlock()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			got, err := InternalRuntimeFor(s).prepareOnConn(context.Background(), conn, state, model.EncodedValues{}, false)
			if err != nil || got != hp {
				b.Fatal(err)
			}
		}
	})
}
