// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
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
			p := oneShotPrepare(t, s, peers[0], requests)
			q, err := p.Query(Options{})
			if err != nil {
				t.Fatal(err)
			}
			call := make(chan error, 1)
			go func() {
				var dst []byte
				var err error
				switch op {
				case "Exec":
					_, err = q.Exec(context.Background(), 1)
				case "One":
					_, _, err = q.One(context.Background(), 1, &dst)
				default:
					_, err = q.Each(context.Background(), 1, &dst, func(*[]byte) error { return nil })
				}
				call <- err
			}()
			<-requests
			q.Close()
			select {
			case err := <-call:
				// The call's own context is cancelled, so it reports
				// context.Canceled with the request's execution status.
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled call returned %v, want context.Canceled", err)
				}
				if status := runtimeerrors.ExecutionStatusOf(err); status != cqltype.MaybeExecuted {
					t.Fatalf("cancelled call status %v, want MaybeExecuted", status)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not cancel the running call")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := q.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := q.Exec(context.Background(), 2); !errors.Is(err, runtimeerrors.ErrClosed) {
				t.Fatalf("call after Close: %v", err)
			}
		})
	}
}
