// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/settings"
)

// TestRunPagePageSizeDoesNotLeak checks that RunPage's page size applies to
// that page only: the next execution on the same query sends the page size
// its options resolve to.
func TestRunPagePageSizeDoesNotLeak(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	defer s.Close()
	requests := make(chan oneShotRequest)
	p := oneShotPrepare(t, s, peers[0], requests)
	rt := InternalRuntimeFor(s)
	want := rt.defaultOptions.InternalProfile.PageSize
	if want == 7 {
		t.Fatal("the default page size must differ from the override")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// serve answers the next request with body and returns its page size.
	serve := func(body []byte) <-chan int32 {
		got := make(chan int32, 1)
		go func() {
			r := <-requests
			got <- r.pageSize
			if _, err := peers[0].Write(typedTestResponse(t, 4, r.stream, body)); err != nil {
				t.Error(err)
			}
		}()
		return got
	}
	void := binary.BigEndian.AppendUint32(nil, frm.ResultKindVoid)
	runPage := func(t *testing.T, run func() (*Page[[]byte], error)) {
		t.Helper()
		got := serve(runtimeRowsBody(nil, "v"))
		page, err := run()
		if err != nil {
			t.Fatal(err)
		}
		if size := <-got; size != 7 {
			t.Fatalf("RunPage sent page size %d, want 7", size)
		}
		page.Close()
	}
	exec := func(t *testing.T, exec func() (model.Result, error)) {
		t.Helper()
		got := serve(void)
		if _, err := exec(); err != nil {
			t.Fatal(err)
		}
		if size := <-got; size != want {
			t.Fatalf("Exec after RunPage sent page size %d, want %d", size, want)
		}
	}

	t.Run("Pooled", func(t *testing.T) {
		runPage(t, func() (*Page[[]byte], error) { return p.RunPage(ctx, 1, model.StartPage(), 7, CompiledOptions{}) })
		waitIdle(t, p, 1)
		exec(t, func() (model.Result, error) { return p.Exec(ctx, 1, CompiledOptions{}) })
	})
	t.Run("Query", func(t *testing.T) {
		q, err := p.Query(Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()
		runPage(t, func() (*Page[[]byte], error) { return q.RunPage(ctx, 1, model.StartPage(), 7) })
		exec(t, func() (model.Result, error) { return q.Exec(ctx, 1) })
	})
	t.Run("CompiledOptions", func(t *testing.T) {
		profile := settings.InternalDefaultProfile()
		options, err := NewCompiledOptions(s, Options{Profile: &profile})
		if err != nil {
			t.Fatal(err)
		}
		runPage(t, func() (*Page[[]byte], error) { return p.RunPage(ctx, 1, model.StartPage(), 7, options) })
		waitIdle(t, p, 1)
		exec(t, func() (model.Result, error) { return p.Exec(ctx, 1, options) })
		if size := options.p.resolved.InternalProfile.PageSize; size != want {
			t.Fatalf("compiled options' page size changed to %d", size)
		}
	})
	if size := rt.defaultOptions.InternalProfile.PageSize; size != want {
		t.Fatalf("session default page size changed to %d", size)
	}
}

// TestStreamOptionsOverrideIsolated checks that a stream's overrides, no
// prefetch and no speculation, stay with the stream: the session's default
// options, compiled options and a query's resolved options keep theirs.
func TestStreamOptionsOverrideIsolated(t *testing.T) {
	s, _, _ := runtimeTestSession(t, 0)
	defer s.Close()
	rt := InternalRuntimeFor(s)
	profile := settings.InternalDefaultProfile()
	profile.Prefetch = 1
	profile.Speculation = runtimeSpeculation{}
	rt.InternalSetProfile(&profile)
	isolated := func(t *testing.T, name string, r *InternalResolvedOptions) {
		t.Helper()
		if r.InternalProfile.Prefetch != 1 || r.InternalProfile.Speculation == nil || r.InternalOptions.Prefetch.Set {
			t.Fatalf("%s options changed by a stream: prefetch %d, speculation %v, prefetch option %v", name, r.InternalProfile.Prefetch, r.InternalProfile.Speculation, r.InternalOptions.Prefetch)
		}
	}
	// There are no hosts, so each stream fails after its overrides applied.
	stream := func(t *testing.T, bound *Bound[cqltype.Bytes], options CompiledOptions) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		if cursor, err := rt.runStream(ctx, bound, codec.StreamOneColumn(codec.StreamBlob), options, StreamOptions{}.Limits, false, nil); err == nil {
			cursor.Close()
			t.Fatal("stream without hosts succeeded")
		}
	}

	t.Run("Default", func(t *testing.T) {
		b := runtimeQuery(t)
		defer b.Close()
		stream(t, b, CompiledOptions{})
		isolated(t, "default", &rt.defaultOptions)
	})
	t.Run("Compiled", func(t *testing.T) {
		own := profile
		options, err := NewCompiledOptions(s, Options{Profile: &own})
		if err != nil {
			t.Fatal(err)
		}
		b := runtimeQuery(t)
		defer b.Close()
		stream(t, b, options)
		isolated(t, "compiled", &options.p.resolved)
	})
	t.Run("Query", func(t *testing.T) {
		q, err := unpreparedQuery(s, binding.Statement[codec.EmptyRecord, cqltype.Bytes]{Kind: cqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: codec.EmptyParamsEncoder, Rows: codec.OneColumn(codec.BlobCodec)}, Options{CQLTrace: true})
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()
		if err := q.capture(context.Background(), codec.EmptyRecord{}); err != nil {
			t.Fatal(err)
		}
		stream(t, &q.bound, q.options)
		q.state.owner.InternalRelease()
		isolated(t, "query", q.state.resolved)
	})
}
