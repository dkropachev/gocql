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
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/model"
	implModel "github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func TestPrepared(t *testing.T) {
	t.Run("Conflict", func(t *testing.T) {
		t.Run("OverflowPropagatesAndRetries", func(t *testing.T) {
			s := func() *cluster.Session {
				opaqueStorageValue := new(cluster.Session)
				implCluster.SessionData(opaqueStorageValue).InternalLogger = logging.InternalNopLogger{}
				return opaqueStorageValue
			}()
			implCluster.SessionData(s).InternalCtx, implCluster.SessionData(s).InternalCancel = context.WithCancel(context.Background())
			defer s.Close()
			cols := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: cqltype.BlobType})
			state, old := preparedState(t, s, cols)
			host := cqltype.UUID{15: 2}
			other := *old
			state.InternalHosts[host] = &other
			page := testPage(t, "retained")
			defer page.Close()
			var view cqltype.Bytes
			if !page.NextInto(&view) {
				t.Fatal(page.Err())
			}
			retained := codecTestMust(page.Retain())
			defer retained.Close()
			p := implCluster.InternalSessionPeripheral(s)
			p.InternalMu.Lock()
			p.InternalPreparedRefreshScopes = make(map[string]uint64)
			for i := 0; i < cluster.InternalMaxPreparedRefreshScopes; i++ {
				p.InternalPreparedRefreshScopes[fmt.Sprint(i)] = 1
			}
			p.InternalMu.Unlock()
			changed := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: cqltype.IntType})
			response := &transport.InternalResponsePage{InternalResponseData: model.ResponseData{Kind: 2, Columns: changed, MetadataID: []byte("conflict")}}
			err := InternalSelectPreparedResponse(state, host, &other, other.InternalMetadataID, true, response)
			if protocol, ok := errors.AsType[*cqltype.ProtocolError](err); !ok || protocol.Stage != "prepared schema reconciliation" {
				t.Fatal("overflow disappeared from logical result", err)
			}
			if !state.InternalConflictDirty.Load() || state.InternalConflictEpoch.Load() != 0 {
				t.Fatal("unscheduled conflict not left dirty")
			}
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := InternalRuntimeFor(s).InternalPrepareOnConnExpected(cancelled, nil, state, model.EncodedValues{}, false, nil); !errors.Is(err, context.Canceled) {
				t.Fatal("canceled waiter performed reconciliation work", err)
			}
			if err := state.InternalReconcilePendingConflict(); !hasErrorType[*cqltype.ProtocolError](err) {
				t.Fatal("future operation silently bypassed unresolved conflict", err)
			}
			if string(view.OwnedBytes()) != "retained" {
				t.Fatal("earlier delivered page invalidated")
			}
			p.InternalMu.Lock()
			delete(p.InternalPreparedRefreshScopes, "0")
			p.InternalMu.Unlock()
			if err := state.InternalReconcilePendingConflict(); err != nil {
				t.Fatal("available scope did not retry", err)
			}
			p.InternalMu.Lock()
			_, queued := p.InternalPreparedRefreshScopes["ks"]
			p.InternalMu.Unlock()
			if !queued || state.InternalConflictEpoch.Load() == 0 {
				t.Fatal("retry was not registered")
			}
		})

		t.Run("RefreshRetriesDirtyScope", func(t *testing.T) {
			s, mock := refreshAllSchemaSession(t)
			backend := &scopedSchemaBackend{schemaDataMock: mock, keyspace: "ks1", queried: make(chan struct{})}
			backend.fail.Store(true)
			implCluster.SessionData(s).InternalControl = backend
			p := implCluster.InternalSessionPeripheral(s)
			p.InternalMu.Lock()
			p.InternalConfig = settings.DefaultConfig("localhost")
			p.InternalConfig.Metadata.Mode = settings.MetadataDisabled
			p.InternalMu.Unlock()
			state := &InternalPreparedState{InternalSession: s, InternalInfo: PreparedInfo{Keyspace: "ks1", Table: "t1"}}
			if err := state.InternalNoteMetadataConflict(); err != nil {
				t.Fatal(err)
			}
			<-backend.queried
			p.InternalMu.Lock()
			_, dirty := p.InternalPreparedRefreshScopes["ks1"]
			p.InternalMu.Unlock()
			if !dirty {
				t.Fatal("failed scope was dropped")
			}
			waitReconciliation(t, p)
			if backend.calls.Load() != 2 {
				t.Fatal("failed required scope did not retry", backend.calls.Load())
			}
		})

		t.Run("RescheduleAfterReconciliation", func(t *testing.T) {
			s := func() *cluster.Session {
				opaqueStorageValuex := new(cluster.Session)
				implCluster.SessionData(opaqueStorageValuex).InternalLogger = logging.InternalNopLogger{}
				return opaqueStorageValuex
			}()
			implCluster.SessionData(s).InternalCtx, implCluster.SessionData(s).InternalCancel = context.WithCancel(context.Background())
			defer s.Close()
			state := &InternalPreparedState{InternalSession: s, InternalInfo: PreparedInfo{Keyspace: "ks", Table: "tbl"}}
			if err := state.InternalNoteMetadataConflict(); err != nil {
				t.Fatal(err)
			}
			first := state.InternalConflictEpoch.Load()
			if err := state.InternalNoteMetadataConflict(); err != nil || state.InternalConflictEpoch.Load() != first {
				t.Fatal("same conflict not coalesced")
			}
			p := implCluster.InternalSessionPeripheral(s)
			p.InternalMu.Lock()
			delete(p.InternalPreparedRefreshScopes, "ks")
			p.InternalSchemaReconciled.Add(1)
			p.InternalMu.Unlock()
			if err := state.InternalNoteMetadataConflict(); err != nil || state.InternalConflictEpoch.Load() == first {
				t.Fatal("future conflicts permanently suppressed", err)
			}
		})

		t.Run("RunsOnlyRelevantTypedRefresh", func(t *testing.T) {
			for _, mode := range []string{"disabled", "excluded", "minimal"} {
				t.Run(mode, func(t *testing.T) {
					s, mock := refreshAllSchemaSession(t)
					p := implCluster.InternalSessionPeripheral(s)
					p.InternalMu.Lock()
					p.InternalConfig = settings.DefaultConfig("localhost")
					if mode == "disabled" {
						p.InternalConfig.Metadata.Mode = settings.MetadataDisabled
					} else if mode == "excluded" {
						p.InternalConfig.Metadata.Keyspaces = []string{"ks2"}
					} else {
						p.InternalConfig.Metadata.Mode = settings.MetadataMinimal
					}
					p.InternalMu.Unlock()
					before := s.Metadata()
					backend := &scopedSchemaBackend{schemaDataMock: mock, keyspace: "ks1", queried: make(chan struct{})}
					implCluster.SessionData(s).InternalControl = backend
					mock.mu.Lock()
					mock.knownKeyspaces["ks1"] = append(mock.knownKeyspaces["ks1"], tableInfo{name: "from_conflict", columns: []columnInfo{{name: "id", kind: "partition_key"}}})
					mock.mu.Unlock()
					state := &InternalPreparedState{InternalSession: s, InternalInfo: PreparedInfo{Keyspace: "ks1", Table: "t1"}}
					for i := 0; i < 1000; i++ {
						if err := state.InternalNoteMetadataConflict(); err != nil {
							t.Fatal(err)
						}
					}
					select {
					case <-backend.queried:
					case <-time.After(3 * time.Second):
						t.Fatal("required relevant refresh did not query typed source")
					}
					waitReconciliation(t, p)
					if backend.calls.Load() != 1 {
						t.Fatal("required refresh not coalesced", backend.calls.Load())
					}
					if after := s.Metadata(); routing.SnapshotData(after).InternalKeyspaces["ks1"].Tables["from_conflict"] != nil || mode != "minimal" && after != before || mode == "minimal" && len(routing.SnapshotData(after).InternalKeyspaces["ks1"].Tables) != 0 {
						t.Fatal("required refresh broadened public metadata")
					}
					if err := state.InternalReconcilePendingConflict(); err != nil || state.InternalConflictDirty.Load() {
						t.Fatal("successful reconciliation left conflict dirty", err)
					}
				})
			}
		})
	})

	t.Run("Discovery", func(t *testing.T) {
		t.Run("CapacityZeroAndConcurrentMisses", func(t *testing.T) {
			for _, capacity := range []int{0, 1} {
				t.Run(fmt.Sprint(capacity), func(t *testing.T) {
					s, conns, peers := runtimeTestSession(t, 1)
					implCluster.SessionData(s).InternalCfg.Keyspace = "ks"
					rt := InternalRuntimeFor(s)
					rt.InternalSetProfile(settings.DefaultConfig("localhost").DefaultProfile)
					rt.InternalConfig.PreparedCacheSize = capacity
					columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
					serverDone := make(chan error, 1)
					go func() {
						streams := make([]int, 2)
						for i := range streams {
							_, stream, _, err := readRequest(peers[0])
							if err != nil {
								serverDone <- err
								return
							}
							streams[i] = stream
						}
						for i, stream := range streams {
							if _, err := peers[0].Write(typedTestResponse(t, 4, stream, preparedBody([]byte{byte(i + 1)}, implCqltype.NewColumns(), columns))); err != nil {
								serverDone <- err
								return
							}
						}
						serverDone <- nil
					}()
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					done := make(chan error, 2)
					for _, cql := range []string{"A", "B"} {
						go func(cql string) {
							_, err := InternalRuntimeFor(s).InternalPrepareTyped(ctx, cql, "ks", *func() *PrepareOptions {
								opaqueStorageValuexxxxxxxxxxxxxx := new(PrepareOptions)
								PrepareOptionsData(opaqueStorageValuexxxxxxxxxxxxxx).InternalConnection = conns[0]
								return opaqueStorageValuexxxxxxxxxxxxxx
							}())
							done <- err
						}(cql)
					}
					for i := 0; i < 2; i++ {
						if err := <-done; err != nil {
							t.Fatal(err)
						}
					}
					if err := <-serverDone; err != nil {
						t.Fatal(err)
					}
					rt.InternalMu.Lock()
					count, order := len(rt.InternalPrepared), len(rt.InternalPrepareOrder)
					rt.InternalMu.Unlock()
					if count != capacity || order != capacity {
						t.Fatal("concurrent discovery exceeded capacity", count, order, capacity)
					}
				})
			}
		})

		t.Run("OrderRemovesAllDuplicates", func(t *testing.T) {
			a, b := InternalPrepareCacheKey{InternalCql: "a"}, InternalPrepareCacheKey{InternalCql: "b"}
			rt := &InternalSessionRuntime{InternalPrepareOrder: []InternalPrepareCacheKey{a, b, a}}
			rt.InternalTouchPrepare(a)
			if len(rt.InternalPrepareOrder) != 2 || rt.InternalPrepareOrder[0] != b || rt.InternalPrepareOrder[1] != a {
				t.Fatal("duplicate LRU order", rt.InternalPrepareOrder)
			}
			rt.InternalForgetPrepareOrder(a)
			if len(rt.InternalPrepareOrder) != 1 || rt.InternalPrepareOrder[0] != b {
				t.Fatal("forget did not remove key")
			}
		})

		t.Run("StaleFailurePreservesBound", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			implCluster.SessionData(s).InternalCfg.Keyspace = "ks"
			cols := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
			state, _ := preparedState(t, s, cols)
			rt := InternalRuntimeFor(s)
			rt.InternalConfig.PreparedCacheSize = 1
			key := InternalPrepareCacheKey{InternalCql: "A", InternalKeyspace: "ks"}
			rt.InternalPrepared[key] = state
			rt.InternalPrepareOrder = []InternalPrepareCacheKey{key}
			implCluster.InternalSessionInvalidatePreparedSchema(s, cluster.Event{Keyspace: "ks", Target: "TABLE", Object: "tbl"})
			done := make(chan error, 1)
			go func() {
				for i := 0; i < 3; i++ {
					_, stream, _, err := readRequest(peers[0])
					if err != nil {
						done <- err
						return
					}
					var response []byte
					if i == 0 {
						response = errorResponse(t, stream, protocol.ErrCodeServer, nil)
					} else {
						response = typedTestResponse(t, 4, stream, preparedBody([]byte{byte(i)}, implCqltype.NewColumns(), cols))
					}
					if _, err = peers[0].Write(response); err != nil {
						done <- err
						return
					}
				}
				done <- nil
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			options := *func() *PrepareOptions {
				opaqueStorageValuexxxxxxxxxxxxx := new(PrepareOptions)
				PrepareOptionsData(opaqueStorageValuexxxxxxxxxxxxx).InternalConnection = conns[0]
				return opaqueStorageValuexxxxxxxxxxxxx
			}()
			if _, err := InternalRuntimeFor(s).InternalPrepareTyped(ctx, "A", "ks", options); err == nil {
				t.Fatal("failed prepare accepted")
			}
			rt.InternalMu.Lock()
			staleOrder := len(rt.InternalPrepareOrder)
			rt.InternalMu.Unlock()
			if staleOrder != 0 {
				t.Fatal("stale failed entry retained LRU record")
			}
			if _, err := InternalRuntimeFor(s).InternalPrepareTyped(ctx, "B", "ks", options); err != nil {
				t.Fatal(err)
			}
			if _, err := InternalRuntimeFor(s).InternalPrepareTyped(ctx, "C", "ks", options); err != nil {
				t.Fatal(err)
			}
			rt.InternalMu.Lock()
			count, order := len(rt.InternalPrepared), len(rt.InternalPrepareOrder)
			_, latest := rt.InternalPrepared[InternalPrepareCacheKey{InternalCql: "C", InternalKeyspace: "ks"}]
			rt.InternalMu.Unlock()
			if count != 1 || order != 1 || !latest {
				t.Fatal("discovery cache exceeded bound", count, order)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})

		t.Run("WarmHandleSurvivesCacheLock", func(t *testing.T) {
			s, conns, _ := runtimeTestSession(t, 1)
			cols := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
			state, hp := preparedState(t, s, cols)
			rt := InternalRuntimeFor(s)
			rt.InternalMu.Lock()
			done := make(chan error, 1)
			go func() {
				got, e := InternalRuntimeFor(s).InternalPrepareOnConn(context.Background(), conns[0], state, implModel.EncodedValues{}, false)
				if e == nil && got != hp {
					e = errors.New("wrong entry")
				}
				done <- e
			}()
			select {
			case e := <-done:
				if e != nil {
					t.Fatal(e)
				}
			case <-time.After(time.Second):
				rt.InternalMu.Unlock()
				t.Fatal("warm execution used session discovery lock")
			}
			rt.InternalMu.Unlock()
		})
	})

	t.Run("HostFlights", func(t *testing.T) {
		t.Run("CancellationAndSchemaCompatibility", func(t *testing.T) {
			for _, incompatible := range []bool{false, true} {
				t.Run(fmt.Sprint(incompatible), func(t *testing.T) {
					s, conns, peers := runtimeTestSession(t, 1)
					conn := conns[0]
					columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
					state, old := preparedState(t, s, columns)
					entered, release := make(chan struct{}), make(chan struct{})
					serverDone := make(chan error, 1)
					go func() {
						op, stream, _, err := readRequest(peers[0])
						if err != nil {
							serverDone <- err
							return
						}
						if op != 9 {
							serverDone <- fmt.Errorf("opcode %d", op)
							return
						}
						close(entered)
						<-release
						params := implCqltype.NewColumns()
						if incompatible {
							params = implCqltype.NewColumns(implCqltype.ColumnSpec{Name: "new", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeInt))})
						}
						_, err = peers[0].Write(typedTestResponse(t, 4, stream, preparedBody([]byte("new"), params, columns)))
						serverDone <- err
					}()
					ctx, cancel := context.WithCancel(context.Background())
					done := make(chan error)
					go func() {
						_, e := InternalRuntimeFor(s).InternalPrepareOnConnExpected(ctx, conn, state, implModel.EncodedValues{Columns: implCqltype.NewColumns()}, true, old)
						done <- e
					}()
					<-entered
					second := make(chan error)
					go func() {
						_, e := InternalRuntimeFor(s).InternalPrepareOnConnExpected(context.Background(), conn, state, implModel.EncodedValues{Columns: implCqltype.NewColumns()}, true, old)
						second <- e
					}()
					deadline := time.Now().Add(time.Second)
					for {
						state.InternalMu.RLock()
						flight := state.InternalPreparing[implEndpoint.InternalHostInfoHostUUID(conn.InternalHost)]
						waiters := int32(0)
						if flight != nil {
							waiters = flight.InternalWaiters.Load()
						}
						state.InternalMu.RUnlock()
						if waiters >= 2 {
							break
						}
						if time.Now().After(deadline) {
							t.Fatal("waiter did not join")
						}
						runtime.Gosched()
					}
					cancel()
					if e := <-done; !errors.Is(e, context.Canceled) {
						t.Fatal(e)
					}
					close(release)
					err := <-second
					if incompatible {
						if _, ok := errors.AsType[*runtimeerrors.SchemaChangedError](err); !ok {
							t.Fatal(err)
						}
					} else if err != nil {
						t.Fatal(err)
					}
					if err := <-serverDone; err != nil {
						t.Fatal(err)
					}
					if !incompatible {
						if hp := state.InternalHosts[implEndpoint.InternalHostInfoHostUUID(conn.InternalHost)]; hp == old || !bytes.Equal(hp.InternalId, []byte("new")) {
							t.Fatal("host ID not updated")
						}
					}
				})
			}
		})

		t.Run("ColdHostsDoNotSerialize", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 2)
			columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
			state, old := preparedState(t, s, columns)
			firstRead, release := make(chan struct{}), make(chan struct{})
			servers := make(chan error, 2)
			go func() {
				_, stream, _, err := readRequest(peers[0])
				close(firstRead)
				<-release
				if err == nil {
					_, err = peers[0].Write(typedTestResponse(t, 4, stream, preparedBody([]byte("first"), implCqltype.NewColumns(), columns)))
				}
				servers <- err
			}()
			go func() {
				_, stream, _, err := readRequest(peers[1])
				if err == nil {
					_, err = peers[1].Write(typedTestResponse(t, 4, stream, preparedBody([]byte("second"), implCqltype.NewColumns(), columns)))
				}
				servers <- err
			}()
			first := make(chan error, 1)
			go func() {
				_, err := InternalRuntimeFor(s).InternalPrepareOnConnExpected(context.Background(), conns[0], state, implModel.EncodedValues{Columns: implCqltype.NewColumns()}, true, old)
				first <- err
			}()
			<-firstRead
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			entry, err := InternalRuntimeFor(s).InternalPrepareOnConn(ctx, conns[1], state, implModel.EncodedValues{Columns: implCqltype.NewColumns()}, false)
			if err != nil || !bytes.Equal(entry.InternalId, []byte("second")) {
				t.Fatal("unrelated host serialized", err)
			}
			close(release)
			if err := <-first; err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if err := <-servers; err != nil {
					t.Fatal(err)
				}
			}
		})

		t.Run("ConflictingHostsCoalesce", func(t *testing.T) {
			s := func() *cluster.Session {
				opaqueStorageValuexxxxxxxxxx := new(cluster.Session)
				implCluster.SessionData(opaqueStorageValuexxxxxxxxxx).InternalLogger = logging.InternalNopLogger{}
				return opaqueStorageValuexxxxxxxxxx
			}()
			implCluster.SessionData(s).InternalCtx, implCluster.SessionData(s).InternalCancel = context.WithCancel(context.Background())
			defer s.Close()
			columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
			state, old := preparedState(t, s, columns)
			host := implCqltype.UUID{15: 2}
			second := *old
			state.InternalHosts[host] = &second
			changed := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeInt))})
			for i := 0; i < 100; i++ {
				response := &transport.InternalResponsePage{InternalResponseData: implModel.ResponseData{Kind: 2, Columns: changed, MetadataID: []byte("conflict")}}
				if err := InternalSelectPreparedResponse(state, host, &second, second.InternalMetadataID, true, response); err != nil {
					t.Fatal(err)
				}
				if !response.Columns.Equal(changed) {
					t.Fatal("matching response metadata lost")
				}
			}
			if !state.InternalConflictRefresh.Load() || !implCluster.InternalSessionPeripheral(s).InternalRetryScheduled.Load() {
				t.Fatal("conflict not scheduled")
			}
			state.InternalMu.RLock()
			latest := state.InternalResult
			state.InternalMu.RUnlock()
			if latest != old.InternalResult {
				t.Fatal("remote order guessed")
			}
			if old.InternalEpoch.InternalCurrent() {
				t.Fatal("conflicting cache entries not invalidated")
			}
		})

		t.Run("WorkerShutdownJoins", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			cols := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
			state, old := preparedState(t, s, cols)
			requested := make(chan struct{})
			go func() { _, _, _, _ = readRequest(peers[0]); close(requested) }()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				_, err := InternalRuntimeFor(s).InternalPrepareOnConnExpected(ctx, conns[0], state, implModel.EncodedValues{Columns: implCqltype.NewColumns()}, true, old)
				done <- err
			}()
			<-requested
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			closed := make(chan error, 1)
			go func() { closed <- s.Close() }()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("session did not join prepare worker")
			}
			state.InternalMu.RLock()
			pending := len(state.InternalPreparing)
			state.InternalMu.RUnlock()
			if pending != 0 {
				t.Fatal("worker retained after close")
			}
		})
	})

	t.Run("Metadata", func(t *testing.T) {
		t.Run("ConditionalAbsentResult", func(t *testing.T) {
			for _, tc := range []struct {
				name         string
				kind         implCqltype.StatementKind
				initial      bool
				badResult    bool
				prepareError bool
			}{
				{name: "unknown", kind: implCqltype.UnknownStatement},
				{name: "conditional", kind: implCqltype.ConditionalStatement},
				{name: "invalid-execute-metadata", kind: implCqltype.ConditionalStatement, badResult: true},
				{name: "ordinary-still-validates", kind: implCqltype.OrdinaryStatement, prepareError: true},
				{name: "present-metadata-still-validates", kind: implCqltype.UnknownStatement, initial: true, prepareError: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					s, _, peers := runtimeTestSession(t, 1)
					defer s.Close()
					defer peers[0].Close()
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					boolType := codecTestMust(implCqltype.ScalarType(implCqltype.TypeBoolean))
					columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "wrong", Type: boolType})
					initial := implCqltype.NewColumns()
					if tc.initial {
						initial = columns
					}
					done := make(chan error, 1)
					go func() {
						op, stream, _, err := readRequest(peers[0])
						if err != nil {
							done <- err
							return
						}
						if op != 9 {
							done <- fmt.Errorf("expected PREPARE got %d", op)
							return
						}
						if _, err = peers[0].Write(typedTestResponse(t, 4, stream, preparedBody([]byte("cas"), implCqltype.NewColumns(), initial))); err != nil || tc.prepareError {
							done <- err
							return
						}
						op, stream, _, err = readRequest(peers[0])
						if err != nil {
							done <- err
							return
						}
						if op != 10 {
							done <- fmt.Errorf("expected EXECUTE got %d", op)
							return
						}
						name := "[applied]"
						if tc.badResult {
							name = "wrong"
						}
						body := binary.BigEndian.AppendUint32(nil, 2)
						body = binary.BigEndian.AppendUint32(body, 1)
						body = binary.BigEndian.AppendUint32(body, 1)
						for _, v := range []string{"ks", "tbl", name} {
							body = binary.BigEndian.AppendUint16(body, uint16(len(v)))
							body = append(body, v...)
						}
						body = binary.BigEndian.AppendUint16(body, uint16(implCqltype.TypeBoolean))
						body = binary.BigEndian.AppendUint32(body, 1)
						body = binary.BigEndian.AppendUint32(body, 1)
						body = append(body, 1)
						_, err = peers[0].Write(typedTestResponse(t, 4, stream, body))
						done <- err
					}()
					prepared, err := Prepare(ctx, s, binding.Statement[codec.EmptyRecord, codec.CASResult[codec.DynamicRow]]{CQL: "INSERT INTO ks.tbl(id)VALUES(1)IF NOT EXISTS", Kind: tc.kind, Params: codec.EmptyParamsEncoder, Rows: codec.CASRows(codec.DynamicRows)}, PrepareOptions{})
					if tc.prepareError {
						if !hasErrorType[*implCqltype.ValidationError](err) {
							t.Fatalf("eager metadata validation: %v", err)
						}
					} else {
						if err != nil {
							t.Fatal(err)
						}
						if PreparedData(prepared).InternalPlans.InternalPlan != nil {
							t.Fatal("absent shape compiled eagerly")
						}
						bound := codecTestMust(BindOwned(prepared, codec.EmptyRecord{}))
						defer bound.Close()
						rows, err := Run(ctx, s, bound, Options{})
						if tc.badResult {
							if !hasErrorType[*implCqltype.ValidationError](err) {
								t.Fatalf("actual metadata was not validated before output: %v", err)
							}
						} else {
							if err != nil {
								t.Fatal(err)
							}
							var out codec.CASResult[codec.DynamicRow]
							if !rows.NextInto(&out) || !out.Applied || out.Previous.Set {
								t.Fatal("conditional result", out, rows.Err())
							}
							if rows.NextInto(&out) || rows.Close() != nil {
								t.Fatal("conditional result completion", rows.Err())
							}
						}
					}
					if err := <-done; err != nil {
						t.Fatal(err)
					}
				})
			}
		})

		t.Run("DependencyTypesAndSnapshotChanges", func(t *testing.T) {
			s := func() *cluster.Session {
				opaqueStorageValuexx := new(cluster.Session)
				implCluster.SessionData(opaqueStorageValuexx).InternalLogger = logging.InternalNopLogger{}
				return opaqueStorageValuexx
			}()
			integer := codecTestMust(implCqltype.ScalarType(implCqltype.TypeInt))
			udt := codecTestMust(implCqltype.UDTType("ks", "wanted", implCqltype.ColumnSpec{Name: "n", Type: integer}))
			columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "v", Type: udt})
			_, hp := preparedState(t, s, columns)
			implCluster.InternalSessionInvalidatePreparedSchema(s, cluster.Event{Keyspace: "ks", Object: "unrelated", Target: "TYPE"})
			if !hp.InternalEpoch.InternalCurrent() {
				t.Fatal("unrelated UDT invalidated")
			}
			implCluster.InternalSessionInvalidatePreparedSchema(s, cluster.Event{Keyspace: "ks", Object: "wanted", Target: "TYPE"})
			if hp.InternalEpoch.InternalCurrent() {
				t.Fatal("UDT dependency ignored")
			}
			tracker := InternalRuntimeFor(s).InternalSchemaTracker()
			d := tracker.InternalDependencies("ks", implCqltype.NewColumns(), columns)
			stamp := d.InternalSnapshot()
			old := routing.NewMetadataSnapshot(routing.SnapshotState{InternalPartitioner: "p", InternalKeyspaces: map[string]*metadata.KeyspaceMetadata{"ks": {Name: "ks", Tables: map[string]*metadata.TableMetadata{"tbl": {Name: "tbl", Keyspace: "ks"}}}}})
			next := routing.NewMetadataSnapshot(routing.SnapshotState{InternalPartitioner: "p", InternalKeyspaces: map[string]*metadata.KeyspaceMetadata{"ks": metadata.InternalCloneV2Keyspace(routing.SnapshotData(old).InternalKeyspaces["ks"])}})
			implCluster.InternalSessionUpdatePreparedSchemaSnapshot(s, old, next)
			if !stamp.InternalCurrent() {
				t.Fatal("unchanged refresh invalidated")
			}
			routing.SnapshotData(next).InternalKeyspaces["ks"].Tables["other"] = &metadata.TableMetadata{Keyspace: "ks", Name: "other"}
			implCluster.InternalSessionUpdatePreparedSchemaSnapshot(s, old, next)
			if !stamp.InternalCurrent() {
				t.Fatal("unrelated refresh invalidated")
			}
			delete(routing.SnapshotData(next).InternalKeyspaces["ks"].Tables, "tbl")
			implCluster.InternalSessionUpdatePreparedSchemaSnapshot(s, old, next)
			if stamp.InternalCurrent() {
				t.Fatal("table drop refresh ignored")
			}
		})

		t.Run("IntroducesUDTDependency", func(t *testing.T) {
			s := func() *cluster.Session {
				opaqueStorageValuexxx := new(cluster.Session)
				implCluster.SessionData(opaqueStorageValuexxx).InternalLogger = logging.InternalNopLogger{}
				return opaqueStorageValuexxx
			}()
			columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
			state, old := preparedState(t, s, columns)
			udt := codecTestMust(implCqltype.UDTType("ks", "introduced", implCqltype.ColumnSpec{Name: "n", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeInt))}))
			changed := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: udt})
			response := &transport.InternalResponsePage{InternalResponseData: implModel.ResponseData{Kind: 2, Columns: changed, MetadataID: []byte("next")}}
			if err := InternalSelectPreparedResponse(state, implCqltype.UUID{15: 1}, old, old.InternalMetadataID, true, response, state.InternalDependencies.InternalTracker.InternalStart()); err != nil {
				t.Fatal(err)
			}
			current := state.InternalHosts[implCqltype.UUID{15: 1}]
			if current == old || !current.InternalEpoch.InternalCurrent() {
				t.Fatal("changed result not installed")
			}
			implCluster.InternalSessionInvalidatePreparedSchema(s, cluster.Event{Keyspace: "ks", Target: "TYPE", Object: "introduced"})
			if current.InternalEpoch.InternalCurrent() || state.InternalCacheCurrent() {
				t.Fatal("new UDT dependency was not registered")
			}
		})

		t.Run("RetainedAcrossSchemaChange", func(t *testing.T) {
			s := func() *cluster.Session {
				opaqueStorageValue := new(cluster.Session)
				implCluster.SessionData(opaqueStorageValue).InternalLogger = logging.InternalNopLogger{}
				return opaqueStorageValue
			}()
			columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
			state, hp := preparedState(t, s, columns)
			implCluster.InternalSessionInvalidatePreparedSchema(s, cluster.Event{Keyspace: "other", Object: "other", Target: "TABLE"})
			if !hp.InternalEpoch.InternalCurrent() {
				t.Fatal("unrelated keyspace invalidated")
			}
			implCluster.InternalSessionInvalidatePreparedSchema(s, cluster.Event{Keyspace: "ks", Object: "other", Target: "TABLE"})
			if !hp.InternalEpoch.InternalCurrent() {
				t.Fatal("unrelated table invalidated")
			}
			implCluster.InternalSessionInvalidatePreparedSchema(s, cluster.Event{Keyspace: "ks", Object: "tbl", Target: "TABLE"})
			if hp.InternalEpoch.InternalCurrent() {
				t.Fatal("related table not invalidated")
			}
			response := &transport.InternalResponsePage{InternalResponseData: implModel.ResponseData{NoMetadata: true, ColumnCount: 1}}
			if err := InternalSelectPreparedResponse(state, implCqltype.UUID{15: 1}, hp, hp.InternalMetadataID, true, response); err != nil || response.InternalMetadata != hp.InternalResult || !response.Columns.Equal(columns) {
				t.Fatal("retained response rejected", err)
			}
			for _, bad := range []*transport.InternalResponsePage{{InternalResponseData: implModel.ResponseData{NoMetadata: true, ColumnCount: 2}}, {InternalResponseData: implModel.ResponseData{NoMetadata: true, ColumnCount: 1, MetadataID: []byte("bad")}}} {
				if InternalSelectPreparedResponse(state, implCqltype.UUID{15: 1}, hp, hp.InternalMetadataID, true, bad) == nil {
					t.Fatal("mismatched metadata accepted")
				}
			}
		})
	})

	t.Run("Publication", func(t *testing.T) {
		t.Run("ExactEntryEpochAndOpaqueIDs", func(t *testing.T) {
			s := func() *cluster.Session {
				opaqueStorageValuex := new(cluster.Session)
				implCluster.SessionData(opaqueStorageValuex).InternalLogger = logging.InternalNopLogger{}
				return opaqueStorageValuex
			}()
			blob := codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))
			columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: blob})
			state, old := preparedState(t, s, columns)
			id := implCqltype.UUID{15: 1}
			newer := *old
			newer.InternalRevision++
			newer.InternalMetadataID = []byte("a")
			state.InternalHosts[id] = &newer
			response := &transport.InternalResponsePage{InternalResponseData: implModel.ResponseData{Kind: 2, Columns: columns, MetadataID: []byte("z")}}
			if err := InternalSelectPreparedResponse(state, id, old, old.InternalMetadataID, true, response); err != nil {
				t.Fatal(err)
			}
			if state.InternalHosts[id] != &newer {
				t.Fatal("stale response replaced newer entry")
			}
			response = &transport.InternalResponsePage{InternalResponseData: implModel.ResponseData{Kind: 2, Columns: columns, MetadataID: []byte("0")}}
			if err := InternalSelectPreparedResponse(state, id, &newer, newer.InternalMetadataID, true, response); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(state.InternalHosts[id].InternalMetadataID, []byte("0")) {
				t.Fatal("opaque IDs treated as ordered")
			}
			current := state.InternalHosts[id]
			implCluster.InternalSessionInvalidatePreparedSchema(s, cluster.Event{Keyspace: "ks", Object: "tbl", Target: "TABLE"})
			response = &transport.InternalResponsePage{InternalResponseData: implModel.ResponseData{Kind: 2, Columns: columns, MetadataID: []byte("later")}}
			_ = InternalSelectPreparedResponse(state, id, current, current.InternalMetadataID, true, response)
			if state.InternalHosts[id] != current {
				t.Fatal("stale schema publication")
			}
		})

		t.Run("InitialDoesNotCacheChangedEpoch", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			implCluster.SessionData(s).InternalCfg.Keyspace = "ks"
			columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
			serverDone := make(chan error, 1)
			go func() {
				_, stream, _, err := readRequest(peers[0])
				if err == nil {
					implCluster.InternalSessionInvalidatePreparedSchema(s, cluster.Event{Keyspace: "ks", Target: "TABLE", Object: "tbl"})
					_, err = peers[0].Write(typedTestResponse(t, 4, stream, preparedBody([]byte("old"), implCqltype.NewColumns(), columns)))
				}
				serverDone <- err
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			state, err := InternalRuntimeFor(s).InternalPrepareTyped(ctx, "SELECT value FROM ks.tbl", "ks", *func() *PrepareOptions {
				opaqueStorageValuexxxxxxxxxxxx := new(PrepareOptions)
				PrepareOptionsData(opaqueStorageValuexxxxxxxxxxxx).InternalConnection = conns[0]
				return opaqueStorageValuexxxxxxxxxxxx
			}())
			if err != nil {
				t.Fatal(err)
			}
			if state.InternalCacheCurrent() {
				t.Fatal("prepare crossing relevant epoch accepted as current")
			}
			rt := InternalRuntimeFor(s)
			rt.InternalMu.Lock()
			count := len(rt.InternalPrepared)
			rt.InternalMu.Unlock()
			if count != 0 {
				t.Fatal("stale initial prepare published")
			}
			if err := <-serverDone; err != nil {
				t.Fatal(err)
			}
		})

		t.Run("NewDependencyEventFences", func(t *testing.T) {
			s := func() *cluster.Session {
				opaqueStorageValuexxxx := new(cluster.Session)
				implCluster.SessionData(opaqueStorageValuexxxx).InternalLogger = logging.InternalNopLogger{}
				return opaqueStorageValuexxxx
			}()
			columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
			state, old := preparedState(t, s, columns)
			started := state.InternalDependencies.InternalTracker.InternalStart()
			implCluster.InternalSessionInvalidatePreparedSchema(s, cluster.Event{Keyspace: "ks", Target: "TYPE", Object: "introduced"})
			udt := codecTestMust(implCqltype.UDTType("ks", "introduced", implCqltype.ColumnSpec{Name: "n", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeInt))}))
			changed := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: udt})
			response := &transport.InternalResponsePage{InternalResponseData: implModel.ResponseData{Kind: 2, Columns: changed, MetadataID: []byte("stale")}}
			if err := InternalSelectPreparedResponse(state, implCqltype.UUID{15: 1}, old, old.InternalMetadataID, true, response, started); err != nil {
				t.Fatal(err)
			}
			if state.InternalHosts[implCqltype.UUID{15: 1}] != old {
				t.Fatal("new dependency event lost")
			}
			if !response.Columns.Equal(changed) {
				t.Fatal("losing response lost supplied metadata")
			}
		})

		t.Run("ResultDoesNotOverwriteReplacement", func(t *testing.T) {
			for _, change := range []string{"entry", "epoch"} {
				t.Run(change, func(t *testing.T) {
					s, conns, peers := runtimeTestSession(t, 1)
					cols := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
					state, old := preparedState(t, s, cols)
					received, release := make(chan struct{}), make(chan struct{})
					serverDone := make(chan error, 1)
					go func() {
						_, stream, _, err := readRequest(peers[0])
						close(received)
						<-release
						if err == nil {
							_, err = peers[0].Write(typedTestResponse(t, 4, stream, preparedBody([]byte("late"), implCqltype.NewColumns(), cols)))
						}
						serverDone <- err
					}()
					result := make(chan *InternalHostPrepared, 1)
					failure := make(chan error, 1)
					go func() {
						entry, err := InternalRuntimeFor(s).InternalPrepareOnConnExpected(context.Background(), conns[0], state, implModel.EncodedValues{Columns: implCqltype.NewColumns()}, true, old)
						result <- entry
						failure <- err
					}()
					<-received
					expected := old
					if change == "entry" {
						replacement := *old
						replacement.InternalId = []byte("current")
						expected = &replacement
						state.InternalMu.Lock()
						state.InternalHosts[implCqltype.UUID{15: 1}] = expected
						state.InternalMu.Unlock()
					} else {
						implCluster.InternalSessionInvalidatePreparedSchema(s, cluster.Event{Keyspace: "ks", Target: "TABLE", Object: "tbl"})
					}
					close(release)
					entry := <-result
					if err := <-failure; err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(entry.InternalId, []byte("late")) {
						t.Fatal("losing response unavailable")
					}
					state.InternalMu.RLock()
					actual := state.InternalHosts[implCqltype.UUID{15: 1}]
					state.InternalMu.RUnlock()
					if actual != expected {
						t.Fatal("late prepare overwrote replacement")
					}
					if err := <-serverDone; err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	})

	t.Run("Reprepare", func(t *testing.T) {
		t.Run("PagingNeverReencodes", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			implCluster.SessionData(s).InternalCfg.Keyspace = "ks"
			typ := codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))
			columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: typ})
			state, old := preparedState(t, s, columns)
			state.InternalParameters = columns
			state.InternalInfo.PartitionKeyIndexes = []int{0}
			count := 0
			params := codecTestMust(codec.OneParam[[]byte](countingBlobEncoder{calls: &count}).Compile(columns))
			handle := func() *Prepared[[]byte,
				implCqltype.Bytes] {
				opaqueStorageValuexxxxx := new(Prepared[[]byte,
					implCqltype.Bytes])
				PreparedData(opaqueStorageValuexxxxx).InternalState = state
				PreparedData(opaqueStorageValuexxxxx).InternalParams = params
				PreparedData(opaqueStorageValuexxxxx).InternalRows = codec.OneColumn(codec.BlobCodec)
				PreparedData(opaqueStorageValuexxxxx).InternalPlans = &InternalResultPlanCache[implCqltype.Bytes]{}
				return opaqueStorageValuexxxxx
			}()
			input := []byte("canonical-payload")
			bound := codecTestMust(BindBorrowed(handle, input))
			spy := &canonicalWriter{testWriter: conns[0].InternalW, payload: input}
			conns[0].InternalW = spy
			defer bound.Close()
			serverDone := make(chan error, 1)
			go func() {
				for step := 0; step < 5; step++ {
					op, stream, body, err := readRequest(peers[0])
					if err != nil {
						serverDone <- err
						return
					}
					if step == 2 {
						if op != 9 {
							serverDone <- fmt.Errorf("wanted PREPARE got %d", op)
							return
						}
						_, err = peers[0].Write(typedTestResponse(t, 4, stream, preparedBody([]byte("new"), columns, columns, 0)))
					} else {
						if op != 10 || !bytes.Contains(body, input) {
							serverDone <- fmt.Errorf("missing original execution bytes step %d", step)
							return
						}
						switch step {
						case 0:
							_, err = peers[0].Write(errorResponse(t, stream, protocol.ErrCodeServer, nil))
						case 1:
							_, err = peers[0].Write(errorResponse(t, stream, protocol.ErrCodeUnprepared, old.InternalId))
						case 3:
							_, err = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody([]byte("a"), "one")))
						case 4:
							_, err = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "two")))
						}
					}
					if err != nil {
						serverDone <- err
						return
					}
				}
				serverDone <- nil
			}()
			profile := settings.InternalDefaultProfile()
			profile.Retry = retryOnce{}
			options := testOptions(OptionValues{Profile: profileOf(profile)})
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			rows := codecTestMust(Run(ctx, s, bound, options))
			defer rows.Close()
			bound.Close()
			var dst implCqltype.Bytes
			var values []string
			for rows.NextInto(&dst) {
				values = append(values, string(dst.OwnedBytes()))
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(values) != "[one two]" || count != 1 {
				t.Fatal(values, count)
			}
			if err := bound.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-serverDone; err != nil {
				t.Fatal(err)
			}
			if spy.bytes.Load() != int64(4*len(input)) {
				t.Fatal("canonical writer bytes", spy.bytes.Load())
			}
			t.Logf("canonical writer attempts=4 payload_backing_bytes=%d", spy.bytes.Load())
			_ = conns
		})

		// Result-only schema evolution cannot invalidate an input with no partition-key
		// components. The existing parameterized hasher/order regressions stay strict.
		t.Run("ResultOnlyShapeWithoutRoutingHasher", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			defer s.Close()
			defer peers[0].Close()
			typ := codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))
			oldColumns := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: typ})
			state, old := preparedState(t, s, oldColumns)
			state.InternalPartitioner = "known-before-schema-invalidation"
			newColumns := implCqltype.NewColumns(cqltype.ColumnSlice(oldColumns)[0], implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "added", Type: typ})
			done := make(chan error, 1)
			go func() {
				_, stream, _, err := readRequest(peers[0])
				if err == nil {
					_, err = peers[0].Write(typedTestResponse(t, 4, stream, preparedBody([]byte("changed"), implCqltype.NewColumns(), newColumns)))
				}
				done <- err
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			updated, err := InternalRuntimeFor(s).InternalPrepareOnConnExpected(ctx, conns[0], state, model.EncodedValues{Columns: implCqltype.NewColumns()}, true, old)
			if err != nil {
				t.Fatal(err)
			}
			if !updated.InternalColumns.Equal(newColumns) || state.InternalParameters.Len() != 0 || len(state.InternalInfo.PartitionKeyIndexes) != 0 {
				t.Fatal("result-only reprepare changed canonical inputs")
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})

		t.Run("RoutingChangesFailBeforeResend", func(t *testing.T) {
			for _, mode := range []string{"partition-key-order", "table", "type", "partitioner", "conditional"} {
				t.Run(mode, func(t *testing.T) {
					s, conns, peers := runtimeTestSession(t, 1)
					typ := codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))
					parameters := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "a", Type: typ}, implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "b", Type: typ})
					columns := implCqltype.NewColumns(cqltype.ColumnSlice(parameters)[0])
					state, old := preparedState(t, s, columns)
					state.InternalParameters = parameters
					state.InternalInfo.PartitionKeyIndexes = []int{0, 1}
					current, pk := parameters, []int{0, 1}
					switch mode {
					case "partition-key-order":
						pk = []int{1, 0}
					case "table":
						changed := append([]implCqltype.ColumnSpec(nil), cqltype.ColumnSlice(parameters)...)
						changed[0].Table = "other"
						current = implCqltype.NewColumns(changed...)
					case "type":
						changed := append([]implCqltype.ColumnSpec(nil), cqltype.ColumnSlice(parameters)...)
						changed[1].Type = codecTestMust(implCqltype.ScalarType(implCqltype.TypeInt))
						current = implCqltype.NewColumns(changed...)
					case "partitioner":
						state.InternalPartitioner = "different"
					case "conditional":
						state.InternalInfo.Conditional = true
					}
					done := make(chan error, 1)
					go func() {
						_, stream, _, err := readRequest(peers[0])
						if err == nil {
							_, err = peers[0].Write(typedTestResponse(t, 4, stream, preparedBody([]byte("new"), current, columns, pk...)))
						}
						done <- err
					}()
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					_, err := InternalRuntimeFor(s).InternalPrepareOnConnExpected(ctx, conns[0], state, implModel.EncodedValues{Columns: parameters}, true, old)
					if _, ok := errors.AsType[*runtimeerrors.SchemaChangedError](err); !ok {
						t.Fatal(mode, err)
					}
					if state.InternalHosts[implCqltype.UUID{15: 1}] != old {
						t.Fatal("incompatible prepare installed")
					}
					if err := <-done; err != nil {
						t.Fatal(err)
					}
				})
			}
		})

		t.Run("StreamReconcileBeforeContinuation", func(t *testing.T) {
			for _, fail := range []bool{false, true} {
				t.Run(fmt.Sprintf("continuation-error=%t", fail), func(t *testing.T) { streamReconcile(t, fail) })
			}
		})
	})
}
