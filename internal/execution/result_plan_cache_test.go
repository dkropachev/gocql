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
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/logging"
	implModel "github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/schemafence"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestResultPlanCache(t *testing.T) {
	t.Run("Compile", func(t *testing.T) {
		t.Run("OldPageKeepsMatchingPlan", func(t *testing.T) {
			s := func() *cluster.Core {
				opaqueStorageValuexxxxxx := new(cluster.Core)
				implCluster.SessionData(opaqueStorageValuexxxxxx).InternalLogger = logging.InternalNopLogger{}
				return opaqueStorageValuexxxxxx
			}()
			columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
			state, old := preparedState(t, s, columns)
			host := implCqltype.UUID{15: 1}
			changed := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeInt))})
			replacement := &transport.InternalResponsePage{ResponseData: implModel.ResponseData{Kind: 2, Columns: changed, MetadataID: []byte("new")}}
			if err := selectPreparedResponse(state, host, old, old.InternalMetadataID, true, replacement); err != nil {
				t.Fatal(err)
			}
			chain := typedTestChain(t, []byte{0, 0, 0, 3, 'o', 'l', 'd'})
			response := &transport.InternalResponsePage{ResponseData: implModel.ResponseData{NoMetadata: true, ColumnCount: 1, RowCount: 1, Rows: chain, Owner: wire.NewLease(chain, nil)}}
			if err := selectPreparedResponse(state, host, old, old.InternalMetadataID, true, response); err != nil {
				t.Fatal(err)
			}
			p := codecTestMust(newPageWithPlans(response, codec.OneColumn(codec.BlobCodec), &InternalResultPlanCache[implCqltype.Bytes]{}, nil))
			defer p.Close()
			var dst implCqltype.Bytes
			if !p.NextInto(&dst) || string(dst.OwnedBytes()) != "old" {
				t.Fatal("old matching page lost", p.Err())
			}
		})

		t.Run("SchemaFenceKeepsRetainedPage", func(t *testing.T) {
			s := func() *cluster.Core {
				opaqueStorageValuexxxxxxx := new(cluster.Core)
				implCluster.SessionData(opaqueStorageValuexxxxxxx).InternalLogger = logging.InternalNopLogger{}
				return opaqueStorageValuexxxxxxx
			}()
			columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
			_, hp := preparedState(t, s, columns)
			var calls atomic.Int32
			decoder := countingDecoder{calls: &calls, hook: func() {
				s.InvalidatePreparedSchema(cluster.Event{Keyspace: "ks", Target: "TABLE", Object: "tbl"})
			}}
			cache := &InternalResultPlanCache[implCqltype.Bytes]{}
			response := &transport.InternalResponsePage{InternalMetadata: hp.InternalResult, ResponseData: implModel.ResponseData{Columns: columns}}
			if _, err := cache.InternalSelectPlan(response, decoder); err != nil {
				t.Fatal(err)
			}
			cache.mu.Lock()
			cached := cache.plan
			cache.mu.Unlock()
			if cached != nil {
				t.Fatal("compile published through changed schema epoch")
			}
		})
	})

	t.Run("Publication", func(t *testing.T) {
		t.Run("SharesEpochGate", func(t *testing.T) {
			s := func() *cluster.Core {
				opaqueStorageValuexxxxxxxxxxx := new(cluster.Core)
				implCluster.SessionData(opaqueStorageValuexxxxxxxxxxx).InternalLogger = logging.InternalNopLogger{}
				return opaqueStorageValuexxxxxxxxxxx
			}()
			columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
			_, hp := preparedState(t, s, columns)
			tracker := hp.InternalResult.InternalTracker
			entered, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			decoder := countingDecoder{calls: &calls, hook: func() {
				if !tracker.InternalMu.TryLock() {
					t.Error("user Compile ran under epoch gate")
				} else {
					tracker.InternalMu.Unlock()
				}
				close(entered)
				<-release
			}}
			cache := &InternalResultPlanCache[implCqltype.Bytes]{}
			done := make(chan error, 1)
			go func() {
				_, err := cache.InternalSelectPlan(&transport.InternalResponsePage{InternalMetadata: hp.InternalResult, ResponseData: implModel.ResponseData{Columns: columns}}, decoder)
				done <- err
			}()
			<-entered
			cache.mu.Lock()
			close(release)
			deadline := time.Now().Add(time.Second)
			for tracker.InternalMu.TryLock() {
				tracker.InternalMu.Unlock()
				if time.Now().After(deadline) {
					cache.mu.Unlock()
					t.Fatal("cache publication did not acquire epoch gate")
				}
				runtime.Gosched()
			}
			invalidating, invalidated := make(chan struct{}), make(chan struct{})
			go func() {
				close(invalidating)
				tracker.InternalBump(schemafence.InternalPreparedDependencyKey{InternalKind: 'b', InternalKeyspace: "ks", InternalName: "tbl"})
				close(invalidated)
			}()
			<-invalidating
			select {
			case <-invalidated:
				cache.mu.Unlock()
				t.Fatal("invalidation interleaved with publication")
			default:
			}
			cache.mu.Unlock()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			<-invalidated
			if hp.InternalEpoch.InternalCurrent() {
				t.Fatal("invalidation was lost")
			}
		})
	})

	t.Run("SelectPlan", func(t *testing.T) {
		t.Run("ScopedReuseAndNoCompileLock", func(t *testing.T) {
			cols := implCqltype.NewColumns(implCqltype.ColumnSpec{Name: "v", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
			entry := &implModel.ResultSchemaIdentity{InternalColumns: cols}
			cache := &InternalResultPlanCache[implCqltype.Bytes]{}
			var calls atomic.Int32
			d := countingDecoder{calls: &calls}
			for i := 0; i < 100; i++ {
				if _, err := cache.InternalSelectPlan(&transport.InternalResponsePage{InternalMetadata: entry, ResponseData: implModel.ResponseData{Columns: cols}}, d); err != nil {
					t.Fatal(err)
				}
			}
			if calls.Load() != 1 {
				t.Fatal("recompiled exact entry", calls.Load())
			}
			equivalent := implCqltype.NewColumns(cqltype.ColumnSlice(cols)...)
			if _, err := cache.InternalSelectPlan(&transport.InternalResponsePage{ResponseData: implModel.ResponseData{Columns: equivalent}}, d); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 {
				t.Fatal("recompiled equal shape")
			}
			another := &InternalResultPlanCache[implCqltype.Bytes]{}
			_, _ = another.InternalSelectPlan(&transport.InternalResponsePage{InternalMetadata: entry, ResponseData: implModel.ResponseData{Columns: cols}}, d)
			if calls.Load() != 2 {
				t.Fatal("shared different decoder cache")
			}
			cold := &InternalResultPlanCache[implCqltype.Bytes]{}
			entered, done := make(chan struct{}), make(chan struct{})
			d.hook = func() { close(entered); <-done }
			completed := make(chan error)
			go func() {
				_, err := cold.InternalSelectPlan(&transport.InternalResponsePage{InternalMetadata: entry, ResponseData: implModel.ResponseData{Columns: cols}}, d)
				completed <- err
			}()
			<-entered
			cold.mu.Lock()
			cold.mu.Unlock()
			close(done)
			if err := <-completed; err != nil {
				t.Fatal(err)
			}
		})

		// TestResultPlanCache/SelectPlan/UnidentifiedSwitchRetiresIdentityHit checks that the
		// lock-free identity hit is withdrawn when the cache switches to an
		// unidentified plan, so a later response under the old identity is compiled
		// for its own columns instead of reusing the unidentified plan.
		t.Run("UnidentifiedSwitchRetiresIdentityHit", func(t *testing.T) {
			blob := codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))
			cols := implCqltype.NewColumns(implCqltype.ColumnSpec{Name: "v", Type: blob})
			other := implCqltype.NewColumns(implCqltype.ColumnSpec{Name: "w", Type: blob})
			entry := &implModel.ResultSchemaIdentity{InternalColumns: cols}
			cache := &InternalResultPlanCache[implCqltype.Bytes]{}
			var calls atomic.Int32
			d := countingDecoder{calls: &calls}
			identified := &transport.InternalResponsePage{InternalMetadata: entry, ResponseData: implModel.ResponseData{Columns: cols}}
			for _, step := range []struct {
				response *transport.InternalResponsePage
				calls    int32
			}{
				{identified, 1},
				{identified, 1},
				{&transport.InternalResponsePage{ResponseData: implModel.ResponseData{Columns: other}}, 2},
				{identified, 3},
				{identified, 3},
			} {
				if _, err := cache.InternalSelectPlan(step.response, d); err != nil {
					t.Fatal(err)
				}
				if calls.Load() != step.calls {
					t.Fatalf("compiled %d plans, want %d", calls.Load(), step.calls)
				}
			}
		})
	})
}
