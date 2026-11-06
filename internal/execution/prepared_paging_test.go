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
	"github.com/scylladb/gocql/v2/retry"

	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/metadata"
	implModel "github.com/scylladb/gocql/v2/internal/model"
	implPaging "github.com/scylladb/gocql/v2/internal/paging"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/schemafence"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

	routing "github.com/scylladb/gocql/v2/internal/routing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	model "github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/paging"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func testPage(t *testing.T, values ...string) *Page[implCqltype.Bytes] {
	t.Helper()
	typ := codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))
	columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Name: "value", Type: typ})
	var body []byte
	for _, v := range values {
		body = binary.BigEndian.AppendUint32(body, uint32(len(v)))
		body = append(body, v...)
	}
	chain := typedTestChain(t, body)
	return codecTestMust(InternalNewPage(&transport.InternalResponsePage{InternalResponseData: implModel.ResponseData{Columns: columns, Rows: chain, RowCount: int32(len(values)), Owner: wire.NewLease(chain, nil), State: implModel.CompletePageState()}}, codec.OneColumn(codec.BlobCodec), nil))
}
func TestV2PageNextIntoMixedCurrentAndRetention(t *testing.T) {
	p := testPage(t, "one", "two", "three")
	defer p.Close()
	var first, second implCqltype.Bytes
	if !p.NextInto(&first) || string(first.OwnedBytes()) != "one" {
		t.Fatal(p.Err())
	}
	retained := codecTestMust(p.Retain())
	defer retained.Close()
	original := first
	first = implCqltype.BorrowBytes([]byte("edited"))
	if string(p.Row().OwnedBytes()) != "edited" || string(codecTestMust(p.Clone()).OwnedBytes()) != "edited" {
		t.Fatal("current destination not observed")
	}
	if !p.Next() || string(p.Row().OwnedBytes()) != "two" || string(first.OwnedBytes()) != "edited" {
		t.Fatal("mixed Next modified old destination")
	}
	if !p.NextInto(&second) || string(second.OwnedBytes()) != "three" {
		t.Fatal(p.Err())
	}
	if p.NextInto(&second) || second.Len() != 0 || p.Err() != nil {
		t.Fatal("exhaustion", p.Err())
	}
	if p.NextInto(nil) || p.Err() != nil {
		t.Fatal("terminal nil replaced exhaustion", p.Err())
	}
	if string(original.OwnedBytes()) != "one" {
		t.Fatal("retention lost")
	}
	p.Close()
	if string(original.OwnedBytes()) != "one" {
		t.Fatal("close lost retained page")
	}
	retained.Close()
	if _, err := original.At(0); !errors.Is(err, implCqltype.ErrReleased) {
		t.Fatal(err)
	}
}
func TestV2PageNextIntoTerminalClearing(t *testing.T) {
	for _, mode := range []string{"empty", "nil", "closed", "malformed", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			p := testPage(t, "one")
			defer p.Close()
			dst := implCqltype.BorrowBytes([]byte("stale"))
			switch mode {
			case "empty":
				p = testPage(t)
				defer p.Close()
			case "nil":
				if p.NextInto(nil) {
					t.Fatal("nil accepted")
				}
				var e *implCqltype.ValidationError
				if !errors.As(p.Err(), &e) || PageData(p).InternalIndex != 0 {
					t.Fatal(p.Err())
				}
			case "closed":
				p.Close()
			case "malformed":
				PageData(p).InternalResponse.Rows = typedTestChain(t, []byte{0, 0, 0, 5, 1})
				PageData(p).InternalCursor = wire.CursorOver(PageData(p).InternalResponse.Rows)
			case "cancelled":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				PageData(p).InternalCtx = ctx
			}
			if p.NextInto(&dst) || dst.Len() != 0 {
				t.Fatal("false did not clear destination")
			}
			previous := p.Err()
			if p.NextInto(nil) || p.Err() != previous {
				t.Fatal("terminal error changed", p.Err())
			}
		})
	}
}
func TestV2PageNextIntoPartialErrorClears(t *testing.T) {
	integer := codecTestMust(implCqltype.ScalarType(implCqltype.TypeInt))
	blob := codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))
	type row struct {
		Number int32
		Value  implCqltype.Bytes
		Unused int
	}
	cols := implCqltype.NewColumns(implCqltype.ColumnSpec{Name: "n", Type: integer}, implCqltype.ColumnSpec{Name: "v", Type: blob})
	body := typedTestChain(t, []byte{0, 0, 0, 4, 0, 0, 0, 7, 0, 0, 0, 5, 1})
	decoder := codec.Row(codec.Column("n", codec.Int32, func(r *row) *int32 { return &r.Number }), codec.Column("v", codec.BlobCodec, func(r *row) *implCqltype.Bytes { return &r.Value }))
	p := codecTestMust(InternalNewPage(&transport.InternalResponsePage{InternalResponseData: implModel.ResponseData{Columns: cols, Rows: body, RowCount: 1, Owner: wire.NewLease(body, nil)}}, decoder, nil))
	defer p.Close()
	dst := row{Unused: 8}
	if p.NextInto(&dst) || dst.Number != 0 || dst.Unused != 0 || p.Err() == nil {
		t.Fatal("partial row published", dst, p.Err())
	}
}
func TestV2RowsNextIntoPagingAndCloseInterrupt(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	nextRequested := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for i, body := range [][]byte{runtimeRowsBody([]byte{}), runtimeRowsBody([]byte("next"), "one")} {
			stream := typedTestReadRequest(t, conns[0], peers[0])
			if _, err := peers[0].Write(typedTestResponse(t, 4, stream, body)); err != nil {
				t.Error(err)
				return
			}
			_ = i
		}
		typedTestReadRequest(t, conns[0], peers[0])
		close(nextRequested)
	}()
	b := runtimeQuery(t)
	defer b.Close()
	rows := codecTestMust(Run(context.Background(), s, b, Options{}))
	defer rows.Close()
	var dst implCqltype.Bytes
	if !rows.NextInto(&dst) || string(dst.OwnedBytes()) != "one" {
		t.Fatal(rows.Err())
	}
	retained := codecTestMust(rows.Retain())
	defer retained.Close()
	copyView := dst
	done := make(chan bool)
	go func() { done <- rows.NextInto(&dst) }()
	<-nextRequested
	closed := make(chan error)
	go func() { closed <- rows.Close() }()
	select {
	case ok := <-done:
		if ok || dst.Len() != 0 {
			t.Fatal("cancelled NextInto succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("close did not interrupt next page")
	}
	if err := <-closed; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if string(copyView.OwnedBytes()) != "one" {
		t.Fatal("retained view lost")
	}
	<-stopped
	if rows.NextInto(nil) || !errors.Is(rows.Err(), context.Canceled) {
		t.Fatal("terminal error changed")
	}
}
func TestV2RowsActiveNilDoesNotPage(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	go func() {
		stream := typedTestReadRequest(t, conns[0], peers[0])
		_, _ = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody([]byte("more"))))
	}()
	b := runtimeQuery(t)
	defer b.Close()
	rows := codecTestMust(Run(context.Background(), s, b, Options{}))
	defer rows.Close()
	if rows.NextInto(nil) {
		t.Fatal("nil accepted")
	}
	var e *implCqltype.ValidationError
	if !errors.As(rows.Err(), &e) {
		t.Fatal(rows.Err())
	}
	if PageData(RowsData(rows).InternalPage).InternalIndex != 0 {
		t.Fatal("nil consumed row")
	}
}
func TestV2PagingHistoryCyclesLimitsAndTokenIdentity(t *testing.T) {
	h := implPaging.InternalPagingHistory{InternalMaxCount: 4, InternalMaxBytes: 4}
	a, b := implModel.RestorePageState([]byte("a")), implModel.RestorePageState([]byte("b"))
	if h.InternalAdd(a) != nil || h.InternalAdd(b) != nil || !errors.Is(h.InternalAdd(a), runtimeerrors.ErrPagingNoProgress) {
		t.Fatal("nonconsecutive cycle")
	}
	if h.InternalCount != 2 || h.InternalBytes != 2 {
		t.Fatal("cycle stored twice")
	}
	entry := h.InternalEntries
	found := false
	for _, v := range entry {
		if bytes.Equal(model.PageToken(v.InternalState), model.PageToken(a)) {
			found = &model.PageToken(v.InternalState)[0] == &model.PageToken(a)[0]
		}
	}
	if !found {
		t.Fatal("history copied payload")
	}
	if h.InternalAdd(implModel.RestorePageState(nil)) != nil || !errors.Is(h.InternalAdd(implModel.RestorePageState([]byte{})), runtimeerrors.ErrPagingNoProgress) {
		t.Fatal("empty cycle")
	}
	if !errors.Is(h.InternalAdd(implModel.RestorePageState([]byte("long"))), runtimeerrors.ErrPagingStateLimit) || h.InternalCount != 3 {
		t.Fatal("byte limit")
	}
	h = implPaging.InternalPagingHistory{InternalMaxCount: 1, InternalMaxBytes: 100}
	if h.InternalAdd(a) != nil || !errors.Is(h.InternalAdd(b), runtimeerrors.ErrPagingStateLimit) {
		t.Fatal("count limit")
	}
}
func TestV2PagingLimitsConfigurationAndCompleteState(t *testing.T) {
	c := settings.DefaultConfig("localhost")
	c.Limits.MaxPagingStates = 0
	if settings.InternalValidateConfig(c) == nil {
		t.Fatal("zero state budget")
	}
	c = settings.DefaultConfig("localhost")
	c.Limits.MaxPagingStateBytes = -1
	if settings.InternalValidateConfig(c) == nil {
		t.Fatal("negative byte budget")
	}
	if _, err := RunPage[codec.EmptyRecord](nil, nil, nil, implModel.CompletePageState(), Options{}); !errors.Is(err, runtimeerrors.ErrPagingComplete) {
		t.Fatal("complete admitted", err)
	}
}
func TestV2RowsCycleAndBudgetBeforePrefetch(t *testing.T) {
	for _, prefetch := range []int{0, 1} {
		for _, mode := range []string{"cycle", "budget"} {
			t.Run(fmt.Sprintf("prefetch=%v/%s", prefetch, mode), func(t *testing.T) {
				s, conns, peers := runtimeTestSession(t, 1)
				rt := InternalRuntimeFor(s)
				rt.InternalConfig.Limits.MaxPagingStates = 2
				rt.InternalConfig.Limits.MaxPagingStateBytes = 2
				profile := settings.InternalDefaultProfile()
				profile.Prefetch = prefetch
				handle := profileOf(profile)
				tokens := []string{"a", "b", "a"}
				if mode == "budget" {
					tokens = tokens[:2]
				}
				serverDone := make(chan struct{})
				go func() {
					defer close(serverDone)
					for _, token := range tokens {
						stream := typedTestReadRequest(t, conns[0], peers[0])
						_, _ = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody([]byte(token), token)))
					}
				}()
				b := runtimeQuery(t)
				defer b.Close()
				rows := codecTestMust(Run(context.Background(), s, b, testOptions(OptionValues{Profile: handle})))
				defer rows.Close()
				if mode == "budget" {
					RowsData(rows).InternalHistory.InternalMaxCount = 1
				}
				count := 0
				for rows.Next() {
					count++
				}
				want := runtimeerrors.ErrPagingNoProgress
				if mode == "budget" {
					want = runtimeerrors.ErrPagingStateLimit
				}
				if !errors.Is(rows.Err(), want) {
					t.Fatal(count, rows.Err())
				}
				<-serverDone
			})
		}
	}
}

type countingDecoder struct {
	calls *atomic.Int32
	hook  func()
}

func (d countingDecoder) Compile(c implCqltype.Columns) (codec.RowPlan[implCqltype.Bytes], error) {
	d.calls.Add(1)
	if d.hook != nil {
		d.hook()
	}
	return codec.OneColumn(codec.BlobCodec).Compile(c)
}
func (d countingDecoder) Clone(v implCqltype.Bytes) implCqltype.Bytes { return v.Clone() }
func TestV2ResultPlanCacheScopedReuseAndNoCompileLock(t *testing.T) {
	cols := implCqltype.NewColumns(implCqltype.ColumnSpec{Name: "v", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
	entry := &implModel.ResultSchemaIdentity{InternalColumns: cols}
	cache := &InternalResultPlanCache[implCqltype.Bytes]{}
	var calls atomic.Int32
	d := countingDecoder{calls: &calls}
	for i := 0; i < 100; i++ {
		if _, err := cache.InternalSelectPlan(&transport.InternalResponsePage{InternalMetadata: entry, InternalResponseData: implModel.ResponseData{Columns: cols}}, d); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("recompiled exact entry", calls.Load())
	}
	equivalent := implCqltype.NewColumns(cqltype.ColumnSlice(cols)...)
	if _, err := cache.InternalSelectPlan(&transport.InternalResponsePage{InternalResponseData: implModel.ResponseData{Columns: equivalent}}, d); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("recompiled equal shape")
	}
	another := &InternalResultPlanCache[implCqltype.Bytes]{}
	_, _ = another.InternalSelectPlan(&transport.InternalResponsePage{InternalMetadata: entry, InternalResponseData: implModel.ResponseData{Columns: cols}}, d)
	if calls.Load() != 2 {
		t.Fatal("shared different decoder cache")
	}
	cold := &InternalResultPlanCache[implCqltype.Bytes]{}
	entered, done := make(chan struct{}), make(chan struct{})
	d.hook = func() { close(entered); <-done }
	completed := make(chan error)
	go func() {
		_, err := cold.InternalSelectPlan(&transport.InternalResponsePage{InternalMetadata: entry, InternalResponseData: implModel.ResponseData{Columns: cols}}, d)
		completed <- err
	}()
	<-entered
	cold.InternalMu.Lock()
	cold.InternalMu.Unlock()
	close(done)
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
}

// TestV2ResultPlanCacheUnidentifiedSwitchRetiresIdentityHit checks that the
// lock-free identity hit is withdrawn when the cache switches to an
// unidentified plan, so a later response under the old identity is compiled
// for its own columns instead of reusing the unidentified plan.
func TestV2ResultPlanCacheUnidentifiedSwitchRetiresIdentityHit(t *testing.T) {
	blob := codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))
	cols := implCqltype.NewColumns(implCqltype.ColumnSpec{Name: "v", Type: blob})
	other := implCqltype.NewColumns(implCqltype.ColumnSpec{Name: "w", Type: blob})
	entry := &implModel.ResultSchemaIdentity{InternalColumns: cols}
	cache := &InternalResultPlanCache[implCqltype.Bytes]{}
	var calls atomic.Int32
	d := countingDecoder{calls: &calls}
	identified := &transport.InternalResponsePage{InternalMetadata: entry, InternalResponseData: implModel.ResponseData{Columns: cols}}
	for _, step := range []struct {
		response *transport.InternalResponsePage
		calls    int32
	}{
		{identified, 1},
		{identified, 1},
		{&transport.InternalResponsePage{InternalResponseData: implModel.ResponseData{Columns: other}}, 2},
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
}
func preparedState(t *testing.T, s *cluster.Session, columns implCqltype.Columns) (*InternalPreparedState, *InternalHostPrepared) {
	t.Helper()
	tracker := InternalRuntimeFor(s).InternalSchemaTracker()
	dependencies := tracker.InternalDependencies("ks", implCqltype.NewColumns(), columns)
	epoch := dependencies.InternalSnapshot()
	result := &implModel.ResultSchemaIdentity{InternalColumns: columns, InternalEpoch: epoch, InternalTracker: tracker}
	hp := &InternalHostPrepared{InternalId: []byte("id"), InternalMetadataID: []byte("meta"), InternalColumns: columns, InternalResult: result, InternalEpoch: epoch, InternalRevision: 1}
	state := &InternalPreparedState{InternalSession: s, InternalProtocol: 4, InternalStatement: "SELECT value FROM ks.tbl", InternalKeyspace: "ks", InternalParameters: implCqltype.NewColumns(), InternalColumns: columns, InternalResult: result, InternalDependencies: dependencies, InternalInitialEpoch: epoch, InternalResultHost: implCqltype.UUID{15: 1}, InternalInfo: PreparedInfo{Keyspace: "ks", Table: "tbl"}, InternalHosts: map[implCqltype.UUID]*InternalHostPrepared{{15: 1}: hp}}
	return state, hp
}
func TestV2PreparedResponseRetainsMetadataAcrossSchemaChange(t *testing.T) {
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
}
func TestV2PreparedPublicationExactEntryEpochAndOpaqueIDs(t *testing.T) {
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
}
func TestV2PreparedDependencyTypesAndSnapshotChanges(t *testing.T) {
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
}

func preparedBody(id []byte, parameters, columns implCqltype.Columns, pk ...int) []byte {
	b := binary.BigEndian.AppendUint32(nil, 4)
	b = binary.BigEndian.AppendUint16(b, uint16(len(id)))
	b = append(b, id...)
	for part, c := range []implCqltype.Columns{parameters, columns} {
		b = binary.BigEndian.AppendUint32(b, 0)
		b = binary.BigEndian.AppendUint32(b, uint32(c.Len()))
		if part == 0 {
			b = binary.BigEndian.AppendUint32(b, uint32(len(pk)))
			for _, i := range pk {
				b = binary.BigEndian.AppendUint16(b, uint16(i))
			}
		}
		for _, col := range cqltype.ColumnSlice(c) {
			for _, text := range []string{col.Keyspace, col.Table, col.Name} {
				b = binary.BigEndian.AppendUint16(b, uint16(len(text)))
				b = append(b, text...)
			}
			b = binary.BigEndian.AppendUint16(b, uint16(col.Type.Kind()))
		}
	}
	return b
}
func readRequest(server net.Conn) (byte, int, []byte, error) {
	var h [9]byte
	if _, e := io.ReadFull(server, h[:]); e != nil {
		return 0, 0, nil, e
	}
	b := make([]byte, binary.BigEndian.Uint32(h[5:]))
	_, e := io.ReadFull(server, b)
	return h[4], int(binary.BigEndian.Uint16(h[2:4])), b, e
}
func TestV2PrepareHostFlightsCancellationAndSchemaCompatibility(t *testing.T) {
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
			var changed *runtimeerrors.SchemaChangedError
			if incompatible {
				if !errors.As(err, &changed) {
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
}
func TestV2WarmPreparedHandleSurvivesDiscoveryCacheLock(t *testing.T) {
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
}

func TestV2PreparedMetadataIntroducesUDTDependency(t *testing.T) {
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
}
func TestV2PreparedNewDependencyEventDuringAttemptFencesPublication(t *testing.T) {
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
}
func errorResponse(t *testing.T, stream, code int, id []byte) []byte {
	body := binary.BigEndian.AppendUint32(nil, uint32(code))
	body = binary.BigEndian.AppendUint16(body, 4)
	body = append(body, "test"...)
	if code == protocol.ErrCodeUnprepared {
		body = binary.BigEndian.AppendUint16(body, uint16(len(id)))
		body = append(body, id...)
	}
	response := typedTestResponse(t, 4, stream, body)
	response[4] = 0
	return response
}

type retryOnce struct{}

func (retryOnce) Decide(e retry.Attempt) retry.Decision {
	if e.Number == 1 {
		return retry.Decision{Action: retry.Same}
	}
	return retry.Decision{}
}
func TestV2PreparedRetryRepreparePagingNeverReencodes(t *testing.T) {
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
}
func TestV2PreparedHostRoutingChangesFailBeforeResend(t *testing.T) {
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
			var changed *runtimeerrors.SchemaChangedError
			if !errors.As(err, &changed) {
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
}
func TestV2PreparedWorkerShutdownJoinsWithoutInput(t *testing.T) {
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
}

func TestV2PreparedColdHostsDoNotSerialize(t *testing.T) {
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
}
func TestV2PreparedPrepareResultDoesNotOverwriteReplacement(t *testing.T) {
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
}

func TestV2PagingHistoryHashCollisionComparesFullBytes(t *testing.T) {
	a, b := implModel.RestorePageState([]byte("a")), implModel.RestorePageState([]byte("b"))
	h := implPaging.InternalPagingHistory{InternalMaxCount: 4, InternalMaxBytes: 8}
	_ = h.InternalAdd(b)
	h.InternalSpill()
	hash := maphash.Bytes(h.InternalSeed, model.PageToken(b))
	h.InternalEntries[hash] = implPaging.InternalPagingHistoryEntry{InternalState: a}
	h.InternalCount = 1
	h.InternalBytes = 1
	if err := h.InternalAdd(b); err != nil {
		t.Fatal("hash collision mistaken for equality", err)
	}
	if h.InternalCount != 2 || h.InternalEntries[hash].InternalNext == nil {
		t.Fatal("collision entry lost")
	}
	if !errors.Is(h.InternalAdd(b), runtimeerrors.ErrPagingNoProgress) {
		t.Fatal("exact duplicate lost")
	}
}
func TestV2ManualPageEmptyContinuationSurvivesClose(t *testing.T) {
	p := testPage(t)
	PageData(p).InternalResponse.State = implModel.RestorePageState(nil)
	state := p.State()
	p.Close()
	if !state.More() || state.Complete() || state.Bytes() == nil || len(state.Bytes()) != 0 {
		t.Fatal("empty continuation collapsed")
	}
	if !p.State().More() {
		t.Fatal("close lost state")
	}
}
func TestV2PreparedOldPageKeepsMatchingPlan(t *testing.T) {
	s := func() *cluster.Session {
		opaqueStorageValuexxxxxx := new(cluster.Session)
		implCluster.SessionData(opaqueStorageValuexxxxxx).InternalLogger = logging.InternalNopLogger{}
		return opaqueStorageValuexxxxxx
	}()
	columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
	state, old := preparedState(t, s, columns)
	host := implCqltype.UUID{15: 1}
	changed := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeInt))})
	replacement := &transport.InternalResponsePage{InternalResponseData: implModel.ResponseData{Kind: 2, Columns: changed, MetadataID: []byte("new")}}
	if err := InternalSelectPreparedResponse(state, host, old, old.InternalMetadataID, true, replacement); err != nil {
		t.Fatal(err)
	}
	chain := typedTestChain(t, []byte{0, 0, 0, 3, 'o', 'l', 'd'})
	response := &transport.InternalResponsePage{InternalResponseData: implModel.ResponseData{NoMetadata: true, ColumnCount: 1, RowCount: 1, Rows: chain, Owner: wire.NewLease(chain, nil)}}
	if err := InternalSelectPreparedResponse(state, host, old, old.InternalMetadataID, true, response); err != nil {
		t.Fatal(err)
	}
	p := codecTestMust(InternalNewPageWithPlans(response, codec.OneColumn(codec.BlobCodec), &InternalResultPlanCache[implCqltype.Bytes]{}, nil))
	defer p.Close()
	var dst implCqltype.Bytes
	if !p.NextInto(&dst) || string(dst.OwnedBytes()) != "old" {
		t.Fatal("old matching page lost", p.Err())
	}
}

func TestV2ResultPlanCompileSchemaFenceKeepsRetainedPage(t *testing.T) {
	s := func() *cluster.Session {
		opaqueStorageValuexxxxxxx := new(cluster.Session)
		implCluster.SessionData(opaqueStorageValuexxxxxxx).InternalLogger = logging.InternalNopLogger{}
		return opaqueStorageValuexxxxxxx
	}()
	columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
	_, hp := preparedState(t, s, columns)
	var calls atomic.Int32
	decoder := countingDecoder{calls: &calls, hook: func() {
		implCluster.InternalSessionInvalidatePreparedSchema(s, cluster.Event{Keyspace: "ks", Target: "TABLE", Object: "tbl"})
	}}
	cache := &InternalResultPlanCache[implCqltype.Bytes]{}
	response := &transport.InternalResponsePage{InternalMetadata: hp.InternalResult, InternalResponseData: implModel.ResponseData{Columns: columns}}
	if _, err := cache.InternalSelectPlan(response, decoder); err != nil {
		t.Fatal(err)
	}
	cache.InternalMu.Lock()
	cached := cache.InternalPlan
	cache.InternalMu.Unlock()
	if cached != nil {
		t.Fatal("compile published through changed schema epoch")
	}
}

func TestV2PagerCompletionCanInspectIterator(t *testing.T) {
	p := testPage(t)
	pageObserved := false
	PageData(p).InternalRelease = func() { _ = p.Err(); _ = p.Close(); pageObserved = true }
	p.Close()
	if !pageObserved {
		t.Fatal("page completion missing")
	}
	rows := func() Rows[implCqltype.Bytes] {
		opaqueStorageValuexxxxxxxx := InternalNewRows[implCqltype.Bytes]()
		RowsData(opaqueStorageValuexxxxxxxx).InternalPage = testPage(t)
		RowsData(opaqueStorageValuexxxxxxxx).InternalCtx = context.Background()
		return opaqueStorageValuexxxxxxxx
	}()
	rowsObserved := false
	RowsData(rows).InternalRelease = func() { _ = rows.Err(); _ = rows.Info(); _ = rows.Close(); rowsObserved = true }
	rows.Close()
	if !rowsObserved {
		t.Fatal("rows completion missing")
	}
}

func TestV2PagerConcurrentCloseReleasesExactlyOnce(t *testing.T) {
	p := testPage(t, "value")
	var releases atomic.Int32
	PageData(p).InternalRelease = func() { releases.Add(1); _ = p.Err(); _ = p.Close() }
	// A Page is not safe for concurrent use: Close is idempotent, also from
	// its own completion callback.
	for i := 0; i < 32; i++ {
		_ = p.Close()
	}
	if releases.Load() != 1 {
		t.Fatal("page release count", releases.Load())
	}
	var wg sync.WaitGroup
	rows := func() Rows[implCqltype.Bytes] {
		opaqueStorageValuexxxxxxxxx := InternalNewRows[implCqltype.Bytes]()
		RowsData(opaqueStorageValuexxxxxxxxx).InternalPage = testPage(t)
		RowsData(opaqueStorageValuexxxxxxxxx).InternalCtx = context.Background()
		return opaqueStorageValuexxxxxxxxx
	}()
	releases.Store(0)
	RowsData(rows).InternalRelease = func() { releases.Add(1); _ = rows.Info(); _ = rows.Close() }
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = rows.Close() }()
	}
	wg.Wait()
	if releases.Load() != 1 {
		t.Fatal("rows release count", releases.Load())
	}
}

func TestV2DeliveredPageSurvivesSessionCloseUntilAdvance(t *testing.T) {
	for _, manual := range []bool{false, true} {
		t.Run(fmt.Sprint(manual), func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				stream := typedTestReadRequest(t, conns[0], peers[0])
				_, _ = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody([]byte("more"), "delivered")))
			}()
			b := runtimeQuery(t)
			defer b.Close()
			var dst implCqltype.Bytes
			if manual {
				p := codecTestMust(RunPage(context.Background(), s, b, implModel.StartPage(), Options{}))
				defer p.Close()
				if !p.NextInto(&dst) {
					t.Fatal(p.Err())
				}
				before := dst
				_ = s.Close()
				if string(before.OwnedBytes()) != "delivered" {
					t.Fatal("shutdown recycled delivered page")
				}
				if p.NextInto(&dst) || dst.Len() != 0 || !errors.Is(p.Err(), runtimeerrors.ErrClosed) {
					t.Fatal("closed page advance", p.Err())
				}
			} else {
				r := codecTestMust(Run(context.Background(), s, b, Options{}))
				defer r.Close()
				if !r.NextInto(&dst) {
					t.Fatal(r.Err())
				}
				before := dst
				_ = s.Close()
				if string(before.OwnedBytes()) != "delivered" {
					t.Fatal("shutdown recycled delivered rows")
				}
				if r.NextInto(&dst) || dst.Len() != 0 || !errors.Is(r.Err(), runtimeerrors.ErrClosed) {
					t.Fatal("closed rows advance", r.Err())
				}
			}
			<-serverDone
		})
	}
}

func TestV2PreparedConflictingHostsRetainResponseAndCoalesce(t *testing.T) {
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
}

func TestV2ResultPlanPublicationSharesEpochGate(t *testing.T) {
	s := func() *cluster.Session {
		opaqueStorageValuexxxxxxxxxxx := new(cluster.Session)
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
		_, err := cache.InternalSelectPlan(&transport.InternalResponsePage{InternalMetadata: hp.InternalResult, InternalResponseData: implModel.ResponseData{Columns: columns}}, decoder)
		done <- err
	}()
	<-entered
	cache.InternalMu.Lock()
	close(release)
	deadline := time.Now().Add(time.Second)
	for tracker.InternalMu.TryLock() {
		tracker.InternalMu.Unlock()
		if time.Now().After(deadline) {
			cache.InternalMu.Unlock()
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
		cache.InternalMu.Unlock()
		t.Fatal("invalidation interleaved with publication")
	default:
	}
	cache.InternalMu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	<-invalidated
	if hp.InternalEpoch.InternalCurrent() {
		t.Fatal("invalidation was lost")
	}
}
func TestV2InitialPrepareDoesNotCacheChangedEpoch(t *testing.T) {
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
}

func TestV2StalePrepareFailurePreservesDiscoveryBound(t *testing.T) {
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
}

func TestV2DiscoveryOrderRemovesAllDuplicates(t *testing.T) {
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
}
func TestV2DiscoveryCapacityZeroAndConcurrentMisses(t *testing.T) {
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
}

// The inline history and its spill into the hashed map detect the same
// repeats and enforce the same limits on either side of the spill.
func TestV2PagingHistoryInlineSpill(t *testing.T) {
	token := func(i int) implModel.PageState { return implModel.RestorePageState([]byte{byte(i), 0xff}) }
	inline := new([paging.InternalInlineHistory]implModel.PageState)
	h := implPaging.InternalPagingHistory{InternalMaxCount: 100, InternalMaxBytes: 1000, InternalInline: inline}
	for i := 0; i < 12; i++ {
		if err := h.InternalAdd(token(i)); err != nil {
			t.Fatal(i, err)
		}
		if (h.InternalEntries != nil) != (i >= paging.InternalInlineHistory) {
			t.Fatal("spill at wrong entry", i)
		}
		for j := 0; j <= i; j++ {
			if !errors.Is(h.InternalAdd(token(j)), runtimeerrors.ErrPagingNoProgress) {
				t.Fatal("repeat lost", i, j)
			}
		}
	}
	if h.InternalCount != 12 || h.InternalBytes != 24 {
		t.Fatal(h.InternalCount, h.InternalBytes)
	}
	for _, held := range inline {
		if model.PageToken(held) != nil {
			t.Fatal("spilled history kept an inline token")
		}
	}
	for _, limit := range []int{3, paging.InternalInlineHistory, paging.InternalInlineHistory + 2} {
		h = implPaging.InternalPagingHistory{InternalMaxCount: limit, InternalMaxBytes: 1000, InternalInline: new([paging.InternalInlineHistory]implModel.PageState)}
		for i := 0; i < limit; i++ {
			if err := h.InternalAdd(token(i)); err != nil {
				t.Fatal(limit, i, err)
			}
		}
		if !errors.Is(h.InternalAdd(token(limit)), runtimeerrors.ErrPagingStateLimit) || !errors.Is(h.InternalAdd(token(0)), runtimeerrors.ErrPagingNoProgress) {
			t.Fatal("count limit", limit)
		}
		h = implPaging.InternalPagingHistory{InternalMaxCount: 100, InternalMaxBytes: int64(2 * limit), InternalInline: new([paging.InternalInlineHistory]implModel.PageState)}
		for i := 0; i < limit; i++ {
			if err := h.InternalAdd(token(i)); err != nil {
				t.Fatal(limit, i, err)
			}
		}
		if !errors.Is(h.InternalAdd(token(limit)), runtimeerrors.ErrPagingStateLimit) {
			t.Fatal("byte limit", limit)
		}
	}
	h = implPaging.InternalPagingHistory{InternalMaxCount: 100, InternalMaxBytes: 1000, InternalInline: inline}
	_ = h.InternalAdd(token(0))
	h.InternalClear()
	if h.InternalCount != 0 || h.InternalEntries != nil || model.PageToken(inline[0]) != nil || h.InternalAdd(token(0)) != nil {
		t.Fatal("clear")
	}
	// Without inline storage every token goes to the map.
	h = implPaging.InternalPagingHistory{InternalMaxCount: 100, InternalMaxBytes: 1000}
	if h.InternalAdd(token(0)) != nil || h.InternalEntries == nil || !errors.Is(h.InternalAdd(token(0)), runtimeerrors.ErrPagingNoProgress) {
		t.Fatal("history without inline storage")
	}
}
