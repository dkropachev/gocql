// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"bytes"
	"context"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/event"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/metadata"
)

// driverGoroutines returns the stacks of the live goroutines the driver
// started, keyed by goroutine ID. A goroutine counts as the driver's when
// its creator is in a non-test file of this module, or when the standard
// library created it (time.AfterFunc, context.AfterFunc) to run code of a
// non-test file of this module. The fake server's goroutines and the
// tests' own are created from test files or by package testing, so they
// do not count.
func driverGoroutines() map[string]string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	out := make(map[string]string)
	for _, g := range bytes.Split(buf, []byte("\n\n")) {
		stack := string(g)
		lines := strings.Split(stack, "\n")
		// The header is "goroutine N [state, wait]:"; only N is stable.
		id, _, _ := strings.Cut(lines[0], " [")
		runsDriverCode := false
		for i, line := range lines {
			if i+1 >= len(lines) {
				break
			}
			file := strings.TrimSpace(lines[i+1])
			driverFrame := strings.HasPrefix(line, "github.com/scylladb/gocql/v2") && !strings.Contains(file, "_test.go:")
			if !strings.HasPrefix(line, "created by ") {
				runsDriverCode = runsDriverCode || driverFrame
				continue
			}
			creator := strings.TrimPrefix(line, "created by ")
			switch {
			case strings.HasPrefix(creator, "github.com/scylladb/gocql/v2"):
				if !strings.Contains(file, "_test.go:") {
					out[id] = stack
				}
			case strings.HasPrefix(creator, "testing."):
			default:
				if runsDriverCode {
					out[id] = stack
				}
			}
		}
	}
	return out
}

// checkGoroutinesJoined fails t when a driver goroutine that was not in
// before is still alive after wait. Close stops every goroutine it does not
// join; wait bounds how long such a goroutine may take to notice.
func checkGoroutinesJoined(t *testing.T, before map[string]string, wait time.Duration) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		var leaked []string
		for id, stack := range driverGoroutines() {
			if _, ok := before[id]; !ok {
				leaked = append(leaked, stack)
			}
		}
		if len(leaked) == 0 {
			return
		}
		if time.Now().After(deadline) {
			slices.Sort(leaked)
			t.Fatalf("%d driver goroutines outlived Session.Close:\n\n%s", len(leaked), strings.Join(leaked, "\n\n"))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestSessionCloseStopsGoroutines connects a session with its control
// connection, event handling, periodic refresh, heartbeats and a
// connection pool, uses it, and checks that every goroutine the session
// started is gone shortly after Close: with a request the server never
// answers in flight, and after the server went away and the pool lost its
// connections.
func TestSessionCloseStopsGoroutines(t *testing.T) {
	for _, serverGone := range []bool{false, true} {
		name := "Healthy"
		if serverGone {
			name = "ServerGone"
		}
		t.Run(name, func(t *testing.T) { testSessionCloseStopsGoroutines(t, serverGone) })
	}
}

func testSessionCloseStopsGoroutines(t *testing.T, serverGone bool) {
	srv := newRingTestServer(t)
	before := driverGoroutines()

	config := settings.DefaultConfig(srv.Address)
	config.Protocol = 4
	config.Metadata.Mode = metadata.SchemaNone
	config.Metadata.InitialDiscovery = false
	config.Metadata.AutomaticAgreement = false
	config.Metadata.PeriodicRefresh = 50 * time.Millisecond
	config.KeepaliveInterval = 20 * time.Millisecond
	config.Pool.AdvancedShardAwareness.Enabled = false
	config.Pool.ConnectionsPerHost = 2
	config.Logger = logging.Nop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := Connect(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	// Close is idempotent: this ends the session when the test fails early.
	defer s.Close()
	if _, err := s.Subscribe(event.Filter{}); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		if err := execTestStatement(ctx, s.Core(), "void", CompiledOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.RefreshMetadata(ctx); err != nil {
		t.Fatal(err)
	}
	// The check below must see the session's goroutines to mean anything.
	started := 0
	for id := range driverGoroutines() {
		if _, ok := before[id]; !ok {
			started++
		}
	}
	if started == 0 {
		t.Fatal("found no goroutines of the open session; driverGoroutines misses them")
	}
	pending := make(chan error, 1)
	if serverGone {
		// The session sees its connections fail and starts reconnecting.
		srv.Stop()
		pending <- nil
		for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			if open := openConnections(s); open == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the session still has %d open connections", openConnections(s))
			}
		}
	} else {
		// A request the server never answers: Close must end it.
		go func() { pending <- execTestStatement(context.Background(), s.Core(), "timeout", CompiledOptions{}) }()
		time.Sleep(100 * time.Millisecond)
		select {
		case err := <-pending:
			t.Fatalf("the unanswered request ended before Close: %v", err)
		default:
		}
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-pending:
	case <-time.After(5 * time.Second):
		t.Fatal("the unanswered request did not end after Close")
	}
	// Close joins the session's own workers. A connection's reader, writer
	// and heartbeat goroutines end once its closed socket returns, which
	// may be just after Close: the bound covers that, not a leak.
	checkGoroutinesJoined(t, before, 2*time.Second)
}

// TestSessionCloseStopsPrefetch leaves Rows with a page prefetch in flight
// and never closes them: Session.Close must end the prefetch goroutine
// without waiting for the application.
func TestSessionCloseStopsPrefetch(t *testing.T) {
	before := driverGoroutines()
	s, conns, peers := runtimeTestSession(t, 1)
	t.Cleanup(func() { _ = peers[0].Close() })
	profile := settings.InternalDefaultProfile()
	profile.Prefetch = 1
	handle := new(profile)
	prefetching := make(chan struct{})
	go func() {
		stream := typedTestReadRequest(t, conns[0], peers[0])
		_, _ = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody([]byte("a"), "a")))
		// The prefetch of the next page arrives and is never answered.
		typedTestReadRequest(t, conns[0], peers[0])
		close(prefetching)
	}()
	b := runtimeQuery(t)
	defer b.Close()
	rows := codecTestMust(Run(context.Background(), s, b, testOptions(Options{Profile: handle})))
	var row cql.Bytes
	if !rows.NextInto(&row) {
		t.Fatal("first row", rows.Err())
	}
	select {
	case <-prefetching:
	case <-time.After(5 * time.Second):
		t.Fatal("no prefetch request")
	}
	prefetcher := false
	for id, stack := range driverGoroutines() {
		if _, ok := before[id]; !ok && strings.Contains(stack, "startPrefetch") {
			prefetcher = true
		}
	}
	if !prefetcher {
		t.Fatal("found no prefetch goroutine; driverGoroutines misses it")
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Session.Close waited for Rows the application never closed")
	}
	checkGoroutinesJoined(t, before, 2*time.Second)
}

// openConnections counts the connections in s's pools.
func openConnections(s *Session) int {
	p := implCluster.SessionData(s.Core()).InternalPool
	p.InternalMu.RLock()
	defer p.InternalMu.RUnlock()
	n := 0
	for _, hp := range p.InternalHostConnPools {
		n += hp.Size()
	}
	return n
}
