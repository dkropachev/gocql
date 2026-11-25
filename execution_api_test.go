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
	"github.com/scylladb/gocql/v2/exec"
	implSession "github.com/scylladb/gocql/v2/internal/session"
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
	options, err := f.s.CompileOptions(exec.Options{Consistency: cql.Some(cql.LocalOne), HostID: cql.Some(host)})
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
	serial, err := f.s.CompileOptions(exec.Options{Consistency: cql.Some(cql.Serial)})
	if err != nil {
		t.Fatal(err)
	}
	if info, err := f.p.Routing(gridKey, serial); err != nil || info.Kind != cql.ConditionalStatement {
		t.Fatalf("Routing of a SERIAL read = %+v, %v; want it routed as conditional", info, err)
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
// and one whose rows disagree. Each call releases its pooled query before it
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
	if _, _, err := s.ApplyBatch(ctx, &batch, exec.CompiledOptions{}, exec.CompiledOptions{}); !errors.As(err, new(*cql.ValidationError)) {
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
// the seeded destination, and breaking out of it closes the stream, so the
// worker runs again at once instead of reporting cqlerr.ErrBusy.
func TestRowStreamAll(t *testing.T) {
	f := newLifetimeFixture(t)
	w, err := f.ps.Worker(exec.StreamOptions{Options: gridOptions})
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
	if info := stream.Info(); info.Pages != 2 || !info.OutputStarted {
		t.Fatalf("Info = %+v, want two pages with output", info)
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
	_ = again.Close()
}
