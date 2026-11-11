// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"bytes"
	"context"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// recordingLogger collects a session logger's output at level.
type recordingLogger struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (r *recordingLogger) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.Write(p)
}

func (r *recordingLogger) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.String()
}

func (r *recordingLogger) logger(level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(r, &slog.HandlerOptions{Level: level}))
}

func registeredQueries(rt *InternalSessionRuntime) int {
	rt.InternalMu.Lock()
	defer rt.InternalMu.Unlock()
	return len(rt.queries)
}

// liveQueries reports how many of rt's registered queries are reachable.
func liveQueries(rt *InternalSessionRuntime) int {
	rt.InternalMu.Lock()
	defer rt.InternalMu.Unlock()
	return len(rt.openQueries())
}

// waitCollected runs the garbage collector until rt has no registered
// queries.
func waitCollected(t *testing.T, rt *InternalSessionRuntime) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for registeredQueries(rt) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d queries still registered", registeredQueries(rt))
		}
		runtime.GC()
		time.Sleep(time.Millisecond)
	}
}

// dropQuery creates a query of s and drops it without Close. With pending, it
// first admits a capture whose transport write stays accepted, and returns
// that write's lease.
//
//go:noinline
func dropQuery(t *testing.T, s *cluster.Core, pending bool) *wire.Lease {
	q, err := unpreparedQuery(s, binding.Statement[[]byte, codec.EmptyRecord]{Kind: cqltype.OrdinaryStatement, CQL: "INSERT INTO ks.t(v) VALUES(?)", Params: codec.OneParam(codec.BytesEncoder), Rows: codec.EmptyRowDecoder, BindMarkers: cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: cqltype.BlobType})}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !pending {
		return nil
	}
	if err := q.capture(context.Background(), []byte("pending")); err != nil {
		t.Fatal(err)
	}
	writer := q.state.owner.InternalWriterLease(q.bound.privateStorage.InternalRequest.InternalValues.Framed)
	q.state.owner.InternalRelease()
	return writer
}

func TestQueryRegistry(t *testing.T) {
	// TestQueryRegistry/CollectsUnclosedQuery checks that a query dropped
	// without Close leaves the session's registry once collected, and that
	// Session.Close then has nothing to wait for.
	t.Run("CollectsUnclosedQuery", func(t *testing.T) {
		for _, level := range []slog.Level{slog.LevelDebug, slog.LevelInfo} {
			t.Run(level.String(), func(t *testing.T) {
				s, rt := attachmentSession()
				log := new(recordingLogger)
				cluster.SessionData(s).InternalLogger = log.logger(level)
				dropQuery(t, s, false)
				if registeredQueries(rt) != 1 {
					t.Fatal("query not registered")
				}
				waitCollected(t, rt)
				done := make(chan struct{})
				go func() { rt.StopActive().Wait(); close(done) }()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("session close waited for a collected query")
				}
				debug := level <= slog.LevelDebug
				reported := strings.Contains(log.String(), "query collected without Close")
				if reported != debug {
					t.Fatalf("collection reported %v, want %v: %q", reported, debug, log.String())
				}
				if debug && !strings.Contains(log.String(), "dropQuery") {
					t.Fatalf("report lacks the creation stack: %q", log.String())
				}
			})
		}
	})

	// TestQueryRegistry/ClosedQueryIsNotReported checks that Close removes the
	// entry, so a closed query is never reported when collected.
	t.Run("ClosedQueryIsNotReported", func(t *testing.T) {
		s, rt := attachmentSession()
		log := new(recordingLogger)
		cluster.SessionData(s).InternalLogger = log.logger(slog.LevelDebug)
		func() {
			q, err := unpreparedQuery(s, binding.Statement[[]byte, codec.EmptyRecord]{Kind: cqltype.OrdinaryStatement, CQL: "INSERT INTO ks.t(v) VALUES(?)", Params: codec.OneParam(codec.BytesEncoder), Rows: codec.EmptyRowDecoder, BindMarkers: cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: cqltype.BlobType})}, Options{})
			if err != nil {
				t.Fatal(err)
			}
			q.Close()
		}()
		if registeredQueries(rt) != 0 {
			t.Fatal("closed query still registered")
		}
		for range 3 {
			runtime.GC()
		}
		time.Sleep(10 * time.Millisecond)
		if log.String() != "" {
			t.Fatalf("closed query reported: %q", log.String())
		}
	})

	// TestQueryRegistry/PendingWriteKeepsQueryAlive checks that an unclosed
	// query whose accepted write has not drained stays reachable and
	// registered, so Session.Close joins the write; once the write drains,
	// the query is collected.
	t.Run("PendingWriteKeepsQueryAlive", func(t *testing.T) {
		s, rt := attachmentSession()
		writer := dropQuery(t, s, true)
		for range 3 {
			runtime.GC()
		}
		time.Sleep(10 * time.Millisecond)
		if n := liveQueries(rt); n != 1 {
			t.Fatalf("%d live queries while a write is pending, want 1", n)
		}
		writer.Release()
		waitCollected(t, rt)
	})

	// TestQueryRegistry/SessionCloseJoinsUnclosedQuery checks that Session.Close
	// stops a live query that was never closed and waits for its drain.
	t.Run("SessionCloseJoinsUnclosedQuery", func(t *testing.T) {
		s, rt := attachmentSession()
		writer := dropQuery(t, s, true)
		done := make(chan struct{})
		go func() { rt.StopActive().Wait(); close(done) }()
		select {
		case <-done:
			t.Fatal("session close bypassed the pending write")
		case <-time.After(20 * time.Millisecond):
		}
		writer.Release()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("session close did not join the drain")
		}
	})
}
