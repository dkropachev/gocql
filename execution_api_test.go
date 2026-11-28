// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/execopt"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	"github.com/scylladb/gocql/v2/retry"
)

// TestPreparedRouting reads a statement's routing from the Prepared itself,
// with no Worker: the statement's table and kind, and the consistency and
// host pin of the options passed, or of the session's defaults without
// them. It sends nothing.
func TestPreparedRouting(t *testing.T) {
	f := newLifetimeFixture(t)
	info, err := f.p.Routing(gridKey)
	if err != nil {
		t.Fatal(err)
	}
	if info.Keyspace != "ks" || info.Table != "tbl" || info.Kind != cql.OrdinaryStatement || info.HostID.Set {
		t.Fatalf("Routing = %+v, want ks.tbl, ordinary, no host pin", info)
	}
	host := cql.UUID{15: 1}
	options, err := f.s.CompileOptions(execopt.Options{Consistency: cql.Some(cql.LocalOne), HostID: cql.Some(host)})
	if err != nil {
		t.Fatal(err)
	}
	info, err = f.p.Routing(gridKey, options)
	if err != nil {
		t.Fatal(err)
	}
	if info.Consistency != cql.LocalOne || info.HostID != cql.Some(host) {
		t.Fatalf("Routing with options = %+v, want LOCAL_ONE pinned to %s", info, host)
	}
	serial, err := f.s.CompileOptions(execopt.Options{Consistency: cql.Some(cql.Serial)})
	if err != nil {
		t.Fatal(err)
	}
	if info, err := f.p.Routing(gridKey, serial); err != nil || info.Kind != cql.ConditionalStatement {
		t.Fatalf("Routing of a SERIAL read = %+v, %v; want it routed as conditional", info, err)
	}
	// An execution rejects a keyspace override that differs from the
	// statement's, so Routing does too.
	other, err := f.s.CompileOptions(execopt.Options{Keyspace: "other"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.p.Routing(gridKey, other); !errors.As(err, new(*cql.ValidationError)) {
		t.Fatalf("Routing with another keyspace = %v, want a cql.ValidationError", err)
	}
	if _, _, err := f.p.One(f.ctx, gridKey, nil, other); !errors.As(err, new(*cql.ValidationError)) {
		t.Fatalf("One with another keyspace = %v, want the cql.ValidationError Routing matches", err)
	}
	same, err := f.s.CompileOptions(execopt.Options{Keyspace: "ks"})
	if err != nil {
		t.Fatal(err)
	}
	if info, err := f.p.Routing(gridKey, same); err != nil || info.Keyspace != "ks" {
		t.Fatalf("Routing with the statement's keyspace = %+v, %v", info, err)
	}
	if _, err := f.p.Routing(gridKey, options, options); err == nil {
		t.Fatal("Routing took two CompiledOptions")
	}
	if _, err := new(Prepared[int32, []byte]).Routing(gridKey); !errors.As(err, new(*cql.ValidationError)) {
		t.Fatalf("zero Prepared.Routing = %v, want a cql.ValidationError", err)
	}
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.p.Routing(gridKey); !errors.Is(err, cqlerr.ErrClosed) {
		t.Fatalf("Routing after Session.Close = %v, want cqlerr.ErrClosed", err)
	}
}

// casAppliedBody is a v4 ROWS result of the [applied] column alone, one row
// per value.
func casAppliedBody(applied ...bool) []byte {
	b := binary.BigEndian.AppendUint32(nil, 2)
	b = binary.BigEndian.AppendUint32(b, 1) // global table spec
	b = binary.BigEndian.AppendUint32(b, 1)
	for _, name := range []string{"ks", "tbl", "[applied]"} {
		b = binary.BigEndian.AppendUint16(b, uint16(len(name)))
		b = append(b, name...)
	}
	b = binary.BigEndian.AppendUint16(b, uint16(cql.KindBoolean))
	b = binary.BigEndian.AppendUint32(b, uint32(len(applied)))
	for _, a := range applied {
		b = binary.BigEndian.AppendUint32(b, 1)
		if a {
			b = append(b, 1)
		} else {
			b = append(b, 0)
		}
	}
	return b
}

// waitPooledIdle fails t unless every pooled query of s becomes idle soon.
func waitPooledIdle(t *testing.T, s *Session, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for implSession.InternalPooledBusy(s.internal().Core()) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%s: a pooled query is still busy", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestApplyBatch sends conditional batches with ApplyBatch: one that applied,
// one that did not, on Cassandra's one row or ScyllaDB's row per statement,
// and one whose rows disagree, then a result with no rows and a cancelled
// context, which fail. Each call releases its pooled query before it
// returns, so the session closes at once, and a batch with no conditional
// member is rejected before it is sent.
func TestApplyBatch(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	var answer atomic.Pointer[[]byte]
	srv := newGridServer(t, peers[0], func(r gridRequest) []byte {
		return typedTestResponse(t, 4, r.stream, *answer.Load())
	})
	defer srv.stop(t, peers[0])
	defer func() { _ = s.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	update, err := s.Prepare(ctx, gridUpdate(cql.ConditionalStatement))
	if err != nil {
		t.Fatal(err)
	}
	var batch Batch
	for range 2 {
		if err := update.AddToBatch(&batch, gridKey); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct {
		name    string
		body    []byte
		rows    int64
		applied bool
	}{
		{"applied", casAppliedBody(true), 1, true},
		{"applied per statement", casAppliedBody(true, true), 2, true},
		{"not applied", casAppliedBody(false, false), 2, false},
		{"rows disagree", casAppliedBody(true, false), 2, false},
	} {
		answer.Store(&c.body)
		applied, result, err := s.ApplyBatch(ctx, &batch)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if applied != c.applied || result.Attempts != 1 || result.FetchedRows != c.rows {
			t.Fatalf("%s: ApplyBatch = %v, %+v; want %v from %d rows of one attempt", c.name, applied, result, c.applied, c.rows)
		}
		waitPooledIdle(t, s, c.name)
	}
	// A response without the [applied] rows fails the call, and so does a
	// cancelled context; either way the call releases its pooled query.
	empty := casAppliedBody()
	answer.Store(&empty)
	if applied, result, err := s.ApplyBatch(ctx, &batch); err == nil || applied || result.Attempts != 1 {
		t.Fatalf("ApplyBatch of a result with no rows = %v, %+v, %v; want an error after one attempt", applied, result, err)
	}
	waitPooledIdle(t, s, "no rows")
	cancelled, cancelNow := context.WithCancel(ctx)
	cancelNow()
	if applied, _, err := s.ApplyBatch(cancelled, &batch); !errors.Is(err, context.Canceled) || applied {
		t.Fatalf("ApplyBatch with a cancelled context = %v, %v; want context.Canceled", applied, err)
	}
	waitPooledIdle(t, s, "cancelled")
	if _, _, err := s.ApplyBatch(ctx, &batch, execopt.CompiledOptions{}, execopt.CompiledOptions{}); !errors.As(err, new(*cql.ValidationError)) {
		t.Fatalf("ApplyBatch with two options = %v, want a cql.ValidationError", err)
	}

	ordinary, err := s.Prepare(ctx, gridUpdate(cql.OrdinaryStatement))
	if err != nil {
		t.Fatal(err)
	}
	var plain Batch
	if err := ordinary.AddToBatch(&plain, gridKey); err != nil {
		t.Fatal(err)
	}
	sent := len(srv.sent())
	if _, _, err := s.ApplyBatch(ctx, &plain); !errors.As(err, new(*cql.ValidationError)) {
		t.Fatalf("ApplyBatch of an ordinary batch = %v, want a cql.ValidationError", err)
	}
	if len(srv.sent()) != sent {
		t.Fatal("ApplyBatch sent an ordinary batch")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ApplyBatch(ctx, &batch); !errors.Is(err, cqlerr.ErrClosed) {
		t.Fatalf("ApplyBatch after Close = %v, want cqlerr.ErrClosed", err)
	}
}

// TestRowStreamAll ranges over a RowStream: the loop decodes every page into
// the seeded destination, and breaking out of it, or panicking in its body,
// closes the stream, so the worker runs again at once instead of reporting
// cqlerr.ErrBusy.
func TestRowStreamAll(t *testing.T) {
	f := newLifetimeFixture(t)
	w, err := f.ps.With(f.s.MustCompileOptions(gridOptions)).Worker()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	stream, err := w.Run(f.ctx, gridKey)
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 0, 16)
	var got string
	for row, err := range stream.All(&dst) {
		if err != nil {
			t.Fatal(err)
		}
		if row != &dst {
			t.Fatal("RowStream.All did not yield dst")
		}
		got += string(*row)
	}
	if got != "abc" {
		t.Fatalf("RowStream.All = %q, want every page's rows", got)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close after All = %v", err)
	}
	if p := stream.Progress(); p.Pages != 2 || !p.OutputStarted {
		t.Fatalf("Progress = %+v, want two pages with output", p)
	}
	if r := stream.Result(); r.FetchedRows != 3 || r.Attempts != 2 {
		t.Fatalf("Result = %+v, want three rows from two attempts", r)
	}

	stream, err = w.Run(f.ctx, gridKey)
	if err != nil {
		t.Fatal(err)
	}
	for row, err := range stream.All(&dst) {
		if err != nil || string(*row) != "a" {
			t.Fatalf("first row = %q, %v", *row, err)
		}
		break
	}
	if stream.NextInto(&dst) {
		t.Fatal("NextInto returned a row after the loop broke")
	}
	again, err := w.Run(f.ctx, gridKey)
	if err != nil {
		t.Fatalf("Run after a broken loop = %v, want the stream closed", err)
	}

	// A panic in the loop body also closes the stream.
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("the loop body's panic did not propagate")
			}
		}()
		for range again.All(&dst) {
			panic("loop body")
		}
	}()
	if again.NextInto(&dst) {
		t.Fatal("NextInto returned a row after the loop body panicked")
	}
	last, err := w.Run(f.ctx, gridKey)
	if err != nil {
		t.Fatalf("Run after a panicking loop = %v, want the stream closed", err)
	}
	_ = last.Close()
}

// TestPreparedWith runs a statement through views made by With: a call
// without options uses the view's defaults, options passed to a call are laid
// over them field by field, and p itself keeps the session defaults. Views
// share p's preparation and pool, and a Worker of a view takes its defaults.
func TestPreparedWith(t *testing.T) {
	f := newLifetimeFixture(t)
	quorum := f.s.MustCompileOptions(execopt.Options{Consistency: cql.Some(cql.Quorum), PageSize: 7})
	one := f.s.MustCompileOptions(execopt.Options{Consistency: cql.Some(cql.One)})
	view := f.p.With(quorum)
	var dst []byte
	sent := func(what string, want cql.Consistency, wantPageSize int32, call func() error) {
		t.Helper()
		before := len(f.srv.sent())
		if err := call(); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		requests := f.srv.sent()[before:]
		if len(requests) == 0 || requests[0].consistency != want || requests[0].pageSize != wantPageSize {
			t.Fatalf("%s sent %+v, want %v with page size %d", what, requests, want, wantPageSize)
		}
	}
	sent("view.One", cql.Quorum, 7, func() error { _, _, err := view.One(f.ctx, gridKey, &dst); return err })
	// The call's consistency wins; the view's page size stays.
	sent("view.One with options", cql.One, 7, func() error { _, _, err := view.One(f.ctx, gridKey, &dst, one); return err })
	sent("view.One with options again", cql.One, 7, func() error { _, _, err := view.One(f.ctx, gridKey, &dst, one); return err })
	sent("view.One with zero options", cql.Quorum, 7, func() error {
		_, _, err := view.One(f.ctx, gridKey, &dst, execopt.CompiledOptions{})
		return err
	})
	// Options that set neither field keep both of the view's.
	timeout := f.s.MustCompileOptions(execopt.Options{Timeout: cql.Some(time.Minute)})
	sent("view.One with a timeout", cql.Quorum, 7, func() error { _, _, err := view.One(f.ctx, gridKey, &dst, timeout); return err })
	sent("p.One", cql.LocalQuorum, 5000, func() error { _, _, err := f.p.One(f.ctx, gridKey, &dst); return err })
	sent("p.One with options", cql.One, 5000, func() error { _, _, err := f.p.One(f.ctx, gridKey, &dst, one); return err })
	sent("view.Each", cql.Quorum, 7, func() error {
		_, err := view.Each(f.ctx, gridKey, &dst, func(*[]byte) error { return nil })
		return err
	})
	// With on a view replaces its defaults.
	sent("view.With(one).Run", cql.One, 5000, func() error {
		rows, err := view.With(one).Run(f.ctx, gridKey)
		if err != nil {
			return err
		}
		return rows.Close()
	})
	if info, err := view.Routing(gridKey); err != nil || info.Consistency != cql.Quorum {
		t.Fatalf("view.Routing = %+v, %v; want QUORUM", info, err)
	}
	if info, err := view.Routing(gridKey, one); err != nil || info.Consistency != cql.One {
		t.Fatalf("view.Routing with options = %+v, %v; want ONE", info, err)
	}
	if view.Info().Generation != f.p.Info().Generation || view.Columns().Len() != 1 {
		t.Fatal("the view does not share p's preparation")
	}
	// A combination the options do not allow fails before sending.
	unpaged := f.p.With(f.s.MustCompileOptions(execopt.Options{PageSize: cql.Unpaged}))
	prefetch := f.s.MustCompileOptions(execopt.Options{Prefetch: cql.Some(true)})
	before := len(f.srv.sent())
	if _, _, err := unpaged.One(f.ctx, gridKey, &dst, prefetch); !errors.As(err, new(*cqlerr.UnsupportedSettingError)) || len(f.srv.sent()) != before {
		t.Fatalf("unpaged view with a prefetch = %v, sent %d; want an UnsupportedSettingError before sending", err, len(f.srv.sent())-before)
	}
	w, err := view.Worker()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	sent("view.Worker().One", cql.Quorum, 7, func() error { _, _, err := w.One(f.ctx, gridKey, &dst); return err })

	// Defaults with a Timestamp would stamp every write of the view alike:
	// the view's calls and its Worker fail before sending, while a
	// Timestamp passed per call to a view is allowed.
	stampOptions := f.s.MustCompileOptions(execopt.Options{Timestamp: cql.Some(time.Unix(1, 0))})
	stamped := f.p.With(stampOptions)
	if _, err := stamped.Worker(); !errors.As(err, new(*cql.ValidationError)) {
		t.Fatalf("Worker of a view with a Timestamp = %v, want a cql.ValidationError", err)
	}
	before = len(f.srv.sent())
	if _, _, err := stamped.One(f.ctx, gridKey, &dst); !errors.As(err, new(*cql.ValidationError)) || len(f.srv.sent()) != before {
		t.Fatalf("One on a view with a Timestamp = %v, sent %d; want a cql.ValidationError before sending", err, len(f.srv.sent())-before)
	}
	sent("view.One with a per-call Timestamp", cql.Quorum, 7, func() error { _, _, err := view.One(f.ctx, gridKey, &dst, stampOptions); return err })
	if _, _, err := new(Prepared[int32, []byte]).With(quorum).One(f.ctx, gridKey, &dst); !errors.As(err, new(*cql.ValidationError)) {
		t.Fatalf("One on a view of a zero Prepared = %v, want a cql.ValidationError", err)
	}

	// A view that speculates a statement that is not idempotent fails every
	// call, and its Worker, before sending.
	speculate := execopt.Options{Speculation: retry.FixedSpeculation{After: time.Hour, MaxAttempts: 2}}
	futile := f.p.With(f.s.MustCompileOptions(speculate))
	before = len(f.srv.sent())
	if _, _, err := futile.One(f.ctx, gridKey, &dst); !errors.As(err, new(*cql.ValidationError)) || len(f.srv.sent()) != before {
		t.Fatalf("One on a view speculating a non-idempotent statement = %v, sent %d; want a cql.ValidationError before sending", err, len(f.srv.sent())-before)
	}
	if _, _, err := futile.One(f.ctx, gridKey, &dst, one); !errors.As(err, new(*cql.ValidationError)) {
		t.Fatalf("One with options on the view = %v, want a cql.ValidationError", err)
	}
	if _, err := futile.Worker(); !errors.As(err, new(*cql.ValidationError)) {
		t.Fatalf("Worker of the view = %v, want a cql.ValidationError", err)
	}
	// The view's own Idempotent makes it eligible, and a view that turns
	// speculation off is not checked.
	speculate.Idempotent = cql.Some(true)
	sent("speculating idempotent view.One", cql.LocalQuorum, 5000, func() error {
		_, _, err := f.p.With(f.s.MustCompileOptions(speculate)).One(f.ctx, gridKey, &dst)
		return err
	})
	sent("NoSpeculation view.One", cql.LocalQuorum, 5000, func() error {
		_, _, err := f.p.With(f.s.MustCompileOptions(execopt.Options{Speculation: retry.NoSpeculation{}})).One(f.ctx, gridKey, &dst)
		return err
	})
}

// TestUnpreparedCommand sends CQL text with nothing to bind as one QUERY,
// with the options passed, and rejects a second options value before
// sending anything.
func TestUnpreparedCommand(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	void := binary.BigEndian.AppendUint32(nil, 1)
	srv := newGridServer(t, peers[0], func(r gridRequest) []byte {
		return typedTestResponse(t, 4, r.stream, void)
	})
	defer srv.stop(t, peers[0])
	defer func() { _ = s.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := s.UnpreparedCommand(ctx, "TRUNCATE ks.tbl", s.MustCompileOptions(execopt.Options{Consistency: cql.Some(cql.All)}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Attempts != 1 {
		t.Fatalf("Result = %+v, want one attempt", result)
	}
	if sent := srv.sent(); len(sent) != 1 || sent[0].consistency != cql.All || sent[0].batch {
		t.Fatalf("sent %+v, want one QUERY at ALL", sent)
	}
	if _, err := s.UnpreparedCommand(ctx, "TRUNCATE ks.tbl", execopt.CompiledOptions{}, execopt.CompiledOptions{}); !errors.As(err, new(*cql.ValidationError)) {
		t.Fatalf("UnpreparedCommand with two options = %v, want a cql.ValidationError", err)
	}
	if len(srv.sent()) != 1 {
		t.Fatal("UnpreparedCommand with two options sent a request")
	}
}

// TestSessionObserver reads back the observer a session was configured
// with: the zero Hooks for a session built without one, and for a zero
// Session.
func TestSessionObserver(t *testing.T) {
	if o := new(Session).Observer(); !o.IsZero() {
		t.Fatalf("zero Session.Observer() = %+v, want the zero Hooks", o)
	}
	s, _, _ := runtimeTestSession(t, 1)
	defer func() { _ = s.Close() }()
	if o := s.Observer(); !o.IsZero() {
		t.Fatalf("Session.Observer() = %+v, want the zero Hooks without WithObserver", o)
	}
}
