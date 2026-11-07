// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package ccm

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestParseResources(t *testing.T) {
	cases := map[string]Resources{
		"":                      {},
		"cpus=8,memory=16G":     {CPUs: 8, MemoryMB: 16384},
		"memory=512":            {MemoryMB: 512},
		" cpus = 4 , mem=2048M": {CPUs: 4, MemoryMB: 2048},
	}
	for in, want := range cases {
		if got, err := ParseResources(in); err != nil || got != want {
			t.Errorf("ParseResources(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"cpus", "cpus=x", "disk=1", "memory=1T"} {
		if _, err := ParseResources(bad); err == nil {
			t.Errorf("ParseResources(%q) accepted", bad)
		}
	}
	if got := (Resources{CPUs: 7}).budgetString(); got != "cpus=7,memory=unlimited" {
		t.Fatal(got)
	}
	if got := (Resources{}).budgetString(); got != "cpus=unlimited,memory=unlimited" {
		t.Fatal(got)
	}
	if b := HostBudget(); b.CPUs <= 0 {
		t.Fatal(b)
	}
}

func TestSpecCost(t *testing.T) {
	s := scyllaSpec(t) // two nodes
	s.SMP = 2
	if got := s.Cost(); got != (Resources{CPUs: 4, MemoryMB: 2048}) {
		t.Fatal(got)
	}
	v, _ := ParseVersion(Cassandra, "5.0.9")
	if got := (ClusterSpec{Version: v, Nodes: []int{3}}).Cost(); got != (Resources{CPUs: 3, MemoryMB: 3072}) {
		t.Fatal(got)
	}
}

// budgetPool has room for two one-CPU nodes.
func budgetPool(t *testing.T) (*Pool, *fakeRunner) {
	p, r := testPool(t)
	p.cfg.Budget = Resources{CPUs: 2}
	return p, r
}

func spec(t *testing.T, nodes int, tag string) ClusterSpec {
	s := scyllaSpec(t)
	s.Nodes = []int{nodes}
	s.Tag = tag
	return s
}

func acquireAsync(p *Pool, t *testing.T, req Request) chan *Lease {
	got := make(chan *Lease, 1)
	go func() {
		l, err := p.Acquire(context.Background(), t, req)
		if err != nil {
			t.Error(err)
		}
		got <- l
	}()
	return got
}

func blocked(t *testing.T, got chan *Lease) {
	t.Helper()
	select {
	case l := <-got:
		t.Fatal("granted over budget:", l.Cluster().Name())
	case <-time.After(100 * time.Millisecond):
	}
}

func TestPoolBudgetEvictsIdleCluster(t *testing.T) {
	p, r := budgetPool(t)
	ctx := context.Background()
	a, err := p.Acquire(ctx, t, Request{Spec: spec(t, 2, "a")})
	if err != nil {
		t.Fatal(err)
	}
	a.Release(false)
	if got := p.Used(); got.CPUs != 2 {
		t.Fatal("idle cluster not counted", got)
	}
	b, err := p.Acquire(ctx, t, Request{Spec: spec(t, 1, "b")})
	if err != nil {
		t.Fatal(err)
	}
	if n := r.count(func(a []string) bool { return a[0] == "remove" }); n != 1 {
		t.Fatal("idle cluster not evicted", n)
	}
	if got := p.Used(); got.CPUs != 1 {
		t.Fatal("used after eviction", got)
	}
	b.Release(false)
	p.Close(ctx)
	if got := p.Used(); got.CPUs != 0 {
		t.Fatal("used after close", got)
	}
}

func TestPoolBudgetWaitsForLeaseToEnd(t *testing.T) {
	p, _ := budgetPool(t)
	ctx := context.Background()
	a, err := p.Acquire(ctx, t, Request{Spec: spec(t, 2, "a")})
	if err != nil {
		t.Fatal(err)
	}
	// Reusing the held cluster takes no budget.
	again, err := p.Acquire(ctx, t, Request{Spec: spec(t, 2, "a")})
	if err != nil {
		t.Fatal(err)
	}
	got := acquireAsync(p, t, Request{Spec: spec(t, 1, "b")})
	blocked(t, got)
	a.Release(false)
	blocked(t, got) // still leased by again
	again.Release(false)
	b := <-got
	if b == nil || !strings.Contains(b.Cluster().Name(), "budgetwaits") {
		t.Fatal(b)
	}
	b.Release(false)
	p.Close(ctx)
}

func TestPoolBudgetIsFirstComeFirstServed(t *testing.T) {
	p, _ := budgetPool(t)
	ctx := context.Background()
	a, err := p.Acquire(ctx, t, Request{Spec: spec(t, 2, "a")})
	if err != nil {
		t.Fatal(err)
	}
	big := acquireAsync(p, t, Request{Spec: spec(t, 2, "big")})
	time.Sleep(50 * time.Millisecond) // big queues first
	small := acquireAsync(p, t, Request{Spec: spec(t, 1, "small")})
	blocked(t, big)
	a.Release(false)
	b := <-big
	blocked(t, small) // would fit next to nothing, but big came first and holds all
	b.Release(false)
	s := <-small
	s.Release(false)
	p.Close(ctx)
}

func TestPoolBudgetRejectsOversizedRequest(t *testing.T) {
	p, _ := budgetPool(t)
	_, err := p.Acquire(context.Background(), t, Request{Spec: spec(t, 3, "")})
	if err == nil || !strings.Contains(err.Error(), "more than the whole budget") {
		t.Fatal(err)
	}
}

func TestPoolBudgetWaitHonoursContext(t *testing.T) {
	p, _ := budgetPool(t)
	ctx := context.Background()
	a, err := p.Acquire(ctx, t, Request{Spec: spec(t, 2, "a")})
	if err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := p.Acquire(short, t, Request{Spec: spec(t, 1, "b")}); err == nil || !strings.Contains(err.Error(), "waiting for budget") {
		t.Fatal(err)
	}
	// The timed-out waiter left the queue: the next one is not stuck behind it.
	a.Release(false)
	b, err := p.Acquire(ctx, t, Request{Spec: spec(t, 1, "c")})
	if err != nil {
		t.Fatal(err)
	}
	b.Release(false)
	p.Close(ctx)
}

func TestPoolBudgetPinnedLeaseFailsWaitersFast(t *testing.T) {
	p, _ := budgetPool(t)
	ctx := context.Background()
	suite, err := p.Acquire(ctx, t, Request{Spec: spec(t, 1, "suite"), Pinned: true})
	if err != nil {
		t.Fatal(err)
	}
	// One CPU is left: a one-node cluster fits, a two-node one never will.
	one, err := p.Acquire(ctx, t, Request{Spec: spec(t, 1, "one")})
	if err != nil {
		t.Fatal(err)
	}
	one.Release(false)
	if _, err := p.Acquire(ctx, t, Request{Spec: spec(t, 2, "two")}); err == nil || !strings.Contains(err.Error(), "held until the pool closes") {
		t.Fatal(err)
	}
	// A waiter queued before the pin fails once the pin lands.
	p2, _ := budgetPool(t)
	held, err := p2.Acquire(ctx, t, Request{Spec: spec(t, 1, "held")})
	if err != nil {
		t.Fatal(err)
	}
	other, err := p2.Acquire(ctx, t, Request{Spec: spec(t, 1, "other")})
	if err != nil {
		t.Fatal(err)
	}
	failed := make(chan error, 1)
	go func() {
		_, err := p2.Acquire(ctx, t, Request{Spec: spec(t, 2, "late")})
		failed <- err
	}()
	time.Sleep(50 * time.Millisecond)
	pin, err := p2.Acquire(ctx, t, Request{Spec: spec(t, 1, "held"), Pinned: true})
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release(false)
	select {
	case err := <-failed:
		if err == nil || !strings.Contains(err.Error(), "held until the pool closes") {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter not failed after pin")
	}
	held.Release(false)
	other.Release(false)
	pin.Release(false)
	suite.Release(false)
	p.Close(ctx)
	p2.Close(ctx)
	if p.Used() != (Resources{}) || p2.Used() != (Resources{}) {
		t.Fatal("budget not returned", p.Used(), p2.Used())
	}
}
