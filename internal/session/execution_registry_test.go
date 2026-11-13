// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	implProtocol "github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

// TestCloseStopsBoundExecutions checks that Session.Close stops every
// session-registered execution: 64 goroutines bind and execute statements
// (some answered at once, some late, some never) until Close, and every call
// returns success, ErrClosed or, when Close cancelled it in flight,
// context.Canceled; none hangs, and the registry ends empty.
func TestCloseStopsBoundExecutions(t *testing.T) {
	srv := NewTestServer(t, implProtocol.ProtoVersion4, context.Background())
	defer srv.Stop()
	cluster := testCluster(implProtocol.ProtoVersion4, srv.Address)
	cluster.Timeout = 30 * time.Second
	cluster.NumConns = 2
	db, err := cluster.Open()
	if err != nil {
		t.Fatal(err)
	}
	// Close is idempotent: this ends the goroutines when the test fails
	// before its own Close.
	defer db.Close()
	// Every eighth goroutine sends a statement the server never answers, so
	// that Close finds executions it alone can end.
	statements := []string{"void", "void", "slow"}
	var (
		wg        sync.WaitGroup
		completed atomic.Int64
		failures  = make(chan error, 64)
	)
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				statement := statements[(g+i)%len(statements)]
				if g%8 == 0 {
					statement = "timeout"
				}
				err := execTestStatement(context.Background(), db, statement, CompiledOptions{})
				if err == nil {
					completed.Add(1)
					continue
				}
				// Close cancels the executions it finds in flight; those
				// report the cancellation. Later admissions report ErrClosed.
				if !errors.Is(err, runtimeerrors.ErrClosed) && !errors.Is(err, context.Canceled) {
					failures <- err
				}
				return
			}
		}(g)
	}
	for deadline := time.Now().Add(5 * time.Second); completed.Load() < 256; {
		if time.Now().After(deadline) {
			t.Fatal("executions did not start completing")
		}
		time.Sleep(time.Millisecond)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("an execution outlived Close")
	}
	close(failures)
	for err := range failures {
		t.Error("execution ended with", err)
	}
	if n := InternalRuntimeFor(db).activeExecutions(); n != 0 {
		t.Fatal("registry retained", n, "executions")
	}
	if err := execTestStatement(context.Background(), db, "void", CompiledOptions{}); !errors.Is(err, runtimeerrors.ErrClosed) {
		t.Fatal("registration after Close", err)
	}
}

// activeExecutions counts the registered executions.
func (s *InternalSessionRuntime) activeExecutions() int {
	n := 0
	for i := range s.registry.shards {
		shard := &s.registry.shards[i]
		shard.mu.Lock()
		for e := shard.head; e != nil; e = e.next {
			n++
		}
		shard.mu.Unlock()
	}
	return n
}

// holdRegistry locks every registry shard of rt. The returned release unlocks
// them once; defer it as well, so that a failed test does not leave Close
// blocked on a shard.
func holdRegistry(rt *InternalSessionRuntime) (release func()) {
	for i := range rt.registry.shards {
		rt.registry.shards[i].mu.Lock()
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			for i := range rt.registry.shards {
				rt.registry.shards[i].mu.Unlock()
			}
		})
	}
}

// spinUntil yields until cond holds, failing the test after 10 seconds.
func spinUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !cond(); runtime.Gosched() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting until", what)
		}
	}
}

// receiveWithin returns the next value from ch, failing the test after 10
// seconds.
func receiveWithin[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for", what)
		panic("unreachable")
	}
}
