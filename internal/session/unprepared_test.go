// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/routing"
)

func unpreparedQueries(s *Session) int {
	rt := InternalRuntimeFor(s.Core())
	rt.InternalMu.Lock()
	defer rt.InternalMu.Unlock()
	return len(rt.queries)
}

func TestUnpreparedCalls(t *testing.T) {
	t.Run("EachAndErrorsReleaseQueries", func(t *testing.T) {
		core := loopbackTestSession(t, "pone")
		s := InternalWrapSession(core)
		statement := binding.Statement[int32, cqltype.Bytes]{CQL: "SELECT value FROM ks.tbl WHERE k = ?", Params: codec.OneParam(codec.Int32), Rows: codec.OneColumn(codec.BlobCodec), BindMarkers: cqltype.NewColumns(cqltype.ColumnSpec{Name: "k", Type: cqltype.IntType})}
		sentinel := errors.New("stop consumer")
		for _, callbackError := range []error{nil, sentinel} {
			var dst cqltype.Bytes
			result, err := s.UnpreparedEach(context.Background(), statement, int32(1), &dst, func(row *cqltype.Bytes) error {
				if string(row.OwnedBytes()) != loopbackValue {
					t.Error("bad borrowed value during callback")
				}
				return callbackError
			})
			if !errors.Is(err, callbackError) || result.Rows != 1 {
				t.Fatal(result, err)
			}
			if _, err := dst.At(0); !errors.Is(err, cqltype.ErrReleased) {
				t.Fatalf("Each unexpectedly detached its borrowed row: %v", err)
			}
		}
		if _, err := s.UnpreparedEach(context.Background(), statement, int32(1), new(cqltype.Bytes), nil); err == nil {
			t.Fatal("nil callback accepted")
		}
		if _, _, err := s.UnpreparedOne(context.Background(), statement, int32(1), new(cqltype.Bytes), Options{}, Options{}); err == nil {
			t.Fatal("multiple options accepted")
		}
		deadline := time.Now().Add(5 * time.Second)
		for unpreparedQueries(s) != 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if n := unpreparedQueries(s); n != 0 {
			t.Fatalf("one-shot calls retained %d queries", n)
		}
	})
	t.Run("CancellationLeavesDrainWithSession", func(t *testing.T) {
		core, conns, peers := runtimeTestSession(t, 1)
		s := InternalWrapSession(core)
		entered, release := make(chan struct{}), make(chan struct{})
		defer close(release)
		observer := &execVoidObserver{onAttempt: func(routing.AttemptEvent) { close(entered); <-release }}
		options := Options{Observer: observer, Profile: generalPathProfile()}
		served := make(chan struct{})
		go func() {
			defer close(served)
			stream := typedTestReadRequest(t, conns[0], peers[0])
			if _, err := peers[0].Write(typedTestResponse(t, 4, stream, []byte{0, 0, 0, frm.ResultKindVoid})); err != nil {
				t.Error(err)
			}
		}()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		returned := make(chan error, 1)
		go func() {
			_, err := s.UnpreparedExec(ctx, binding.Statement[codec.EmptyRecord, codec.EmptyRecord]{CQL: "INSERT INTO ks.tbl(k) VALUES(1)"}, codec.EmptyRecord{}, options)
			returned <- err
		}()
		<-entered
		cancel()
		select {
		case err := <-returned:
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("one-shot cleanup blocked its cancelled caller")
		}
		if n := unpreparedQueries(s); n != 1 {
			t.Fatalf("session lost the draining execution: %d queries", n)
		}
		<-served
	})
}
