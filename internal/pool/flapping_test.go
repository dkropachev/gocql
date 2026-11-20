// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package pool_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/endpoint"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/session"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/tests"
)

// constantReconnect retries every delay, forever.
type constantReconnect time.Duration

func (d constantReconnect) Delay(int) (time.Duration, bool) { return time.Duration(d), true }

// dialFailure marks the error of one failed dial, so a conviction event can
// be traced back to the dial it reports.
type dialFailure struct {
	seq int
	err error
}

func (e *dialFailure) Error() string { return fmt.Sprintf("dial %d: %v", e.seq, e.err) }
func (e *dialFailure) Unwrap() error { return e.err }

// convictionLog records what a ConvictionPolicy is told and convicts like
// SimpleConvictionPolicy.
type convictionLog struct {
	mu       sync.Mutex
	failures []endpoint.ConvictionEvent
}

func (l *convictionLog) AddFailure(ev endpoint.ConvictionEvent) bool {
	l.mu.Lock()
	l.failures = append(l.failures, ev)
	l.mu.Unlock()
	return !ev.Shard.Set
}

func (l *convictionLog) Reset(endpoint.ConvictionEvent) {}

func (l *convictionLog) events() []endpoint.ConvictionEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]endpoint.ConvictionEvent(nil), l.failures...)
}

func hostUp(s *session.Session) bool {
	hosts := implCluster.SessionData(s.Core()).InternalHostSource.InternalGetHostsList()
	return len(hosts) == 1 && hosts[0].IsUp()
}

// goroutineSummary counts goroutines by the function each was created by.
func goroutineSummary(stacks map[string]string) string {
	counts := map[string]int{}
	for _, stack := range stacks {
		creator := "?"
		if i := strings.Index(stack, "created by "); i >= 0 {
			creator, _, _ = strings.Cut(stack[i+len("created by "):], "\n")
		}
		counts[creator]++
	}
	var lines []string
	for creator, n := range counts {
		lines = append(lines, fmt.Sprintf("%3d %s", n, creator))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// TestNodeFlapping takes a node down and up again many times under an open
// session. Every time the session must notice, drop the node, and when it is
// back reconnect to exactly one connection per shard, keeping no connection
// or goroutine of an earlier round; the conviction policy must hear of each
// failure once, and never of the cancellation of a pool the session closed.
func TestNodeFlapping(t *testing.T) {
	t.Run("SameAddress", func(t *testing.T) { testNodeFlapping(t, nil) })
	// The node tells other nodes another address than clients, as with
	// broadcast_rpc_address set: marking it down must not depend on finding
	// it by the address the session dials.
	t.Run("BroadcastAddressDiffers", func(t *testing.T) { testNodeFlapping(t, net.IPv4(127, 0, 0, 3)) })
}

func testNodeFlapping(t *testing.T, broadcast net.IP) {
	const rounds = 20
	before := tests.DriverGoroutines()
	n := newFakeNode(t, testShards, nil, shardAwarePort)
	n.broadcast = broadcast
	policy := &convictionLog{}
	var mu sync.Mutex
	seq := 0
	s := connect(t, n, func(c *settings.Config) {
		c.Reconnect = constantReconnect(5 * time.Millisecond)
		c.Conviction = policy
		c.ConnectTimeout = time.Second
		c.DialHost = func(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
			conn, err := endpoint.DefaultDialer{}.DialContext(ctx, target)
			if err != nil {
				mu.Lock()
				seq++
				err = &dialFailure{seq: seq, err: err}
				mu.Unlock()
			}
			return conn, err
		}
	})
	// waitUp waits until the session is back to one connection per shard
	// and the host is up; withControl also waits for the control connection,
	// which reconnects on its own, slower schedule.
	waitUp := func(round int, withControl bool) {
		t.Helper()
		waitFull(t, s, n)
		waitFor(t, 10*time.Second, func() string {
			if open := n.openConns(); withControl && open != testShards+1 || !hostUp(s) {
				return fmt.Sprintf("round %d: node has %d open connections, want %d for the pool and the control connection; host up: %v", round, open, testShards+1, hostUp(s))
			}
			return ""
		})
	}
	waitUp(0, true)
	settled := tests.DriverGoroutines()

	for round := 1; round <= rounds; round++ {
		failuresBefore := len(policy.events())
		n.down()
		waitFor(t, 10*time.Second, func() string {
			if poolSize(s) != 0 || hostUp(s) {
				return fmt.Sprintf("round %d: node down, but the pool holds %d connections and host up is %v", round, poolSize(s), hostUp(s))
			}
			return ""
		})
		if len(policy.events()) == failuresBefore {
			t.Fatalf("round %d: the host went down without a failure reported to the conviction policy", round)
		}
		n.up()
		waitUp(round, false)
	}
	waitUp(rounds, true)

	// Every round ends where the first began: as many goroutines as then.
	waitFor(t, 5*time.Second, func() string {
		if now := tests.DriverGoroutines(); len(now) != len(settled) {
			return fmt.Sprintf("after %d rounds the session runs %d goroutines, %d after it first connected\nthen:\n%s\nnow:\n%s",
				rounds, len(now), len(settled), goroutineSummary(settled), goroutineSummary(now))
		}
		return ""
	})

	// Each failed dial is one failure: one event from the control
	// connection, or from the pool one event for its shard and, when it
	// leaves the pool empty, one more for the host. A failure the session
	// caused by closing a pool is not the host's and is not reported.
	type key struct {
		source endpoint.ConvictionSource
		shard  bool
		seq    int
	}
	seen := map[key]bool{}
	sources := map[int]endpoint.ConvictionSource{}
	for _, ev := range policy.events() {
		if errors.Is(ev.Err, context.Canceled) || errors.Is(ev.Err, cqlerr.ErrClosed) {
			t.Errorf("policy told of the session's own cancellation: %+v", ev)
			continue
		}
		var failure *dialFailure
		if !errors.As(ev.Err, &failure) {
			// Not a dial: the node closed a connection during its setup.
			continue
		}
		k := key{ev.Source, ev.Shard.Set, failure.seq}
		if seen[k] {
			t.Errorf("policy told twice of %v (source %v, shard event %v)", failure, ev.Source, ev.Shard.Set)
		}
		seen[k] = true
		if src, ok := sources[failure.seq]; ok && src != ev.Source {
			t.Errorf("policy told of %v from both %v and %v", failure, src, ev.Source)
		}
		sources[failure.seq] = ev.Source
	}

	if len(seen) == 0 {
		t.Fatal("no conviction event traced back to a failed dial; the check above checked nothing")
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	n.stop()
	tests.CheckGoroutinesJoined(t, before, 2*time.Second)
}
