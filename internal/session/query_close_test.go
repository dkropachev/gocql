// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
)

// TestQueryCloseCancelsRunningCall checks that Close from another goroutine
// returns at once, cancels a call waiting for its response with
// context.Canceled,
// and that Wait then joins the drain.
func TestQueryCloseCancelsRunningCall(t *testing.T) {
	for _, op := range []string{"Exec", "One", "Each"} {
		t.Run(op, func(t *testing.T) {
			s, _, peers := runtimeTestSession(t, 1)
			defer peers[0].Close()
			requests := make(chan oneShotRequest, 1)
			var q *Query[int32, []byte]
			var w *Query[int32, cql.EmptyRow]
			var err error
			if op == "Exec" {
				// Exec rejects the SELECT of oneShotPrepare before sending.
				w, err = oneShotPrepareWrite(t, s, peers[0], requests).Query(Options{})
			} else {
				q, err = oneShotPrepare(t, s, peers[0], requests).Query(Options{})
			}
			if err != nil {
				t.Fatal(err)
			}
			closeQuery := func() {
				if w != nil {
					w.Close()
				} else {
					q.Close()
				}
			}
			call := make(chan error, 1)
			go func() {
				var dst []byte
				var err error
				switch op {
				case "Exec":
					_, err = w.Exec(context.Background(), 1)
				case "One":
					_, _, err = q.One(context.Background(), 1, &dst)
				default:
					_, err = q.Each(context.Background(), 1, &dst, func(*[]byte) error { return nil })
				}
				call <- err
			}()
			<-requests
			closeQuery()
			select {
			case err := <-call:
				// The call's own context is cancelled, so it reports
				// context.Canceled with the request's execution status.
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled call returned %v, want context.Canceled", err)
				}
				if status := cqlerr.ExecutionStatusOf(err); status != cqlerr.MaybeExecuted {
					t.Fatalf("cancelled call status %v, want MaybeExecuted", status)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not cancel the running call")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			wait, again := q.Wait, func() error { _, _, err := q.One(context.Background(), 2, nil); return err }
			if w != nil {
				wait, again = w.Wait, func() error { _, err := w.Exec(context.Background(), 2); return err }
			}
			if err := wait(ctx); err != nil {
				t.Fatal(err)
			}
			if err := again(); !errors.Is(err, cqlerr.ErrClosed) {
				t.Fatalf("call after Close: %v", err)
			}
		})
	}
}
