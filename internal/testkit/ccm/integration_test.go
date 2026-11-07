// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

//go:build integration

package ccm

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// GOCQL_CCM_TEST_DISTRIBUTION (scylla or cassandra) and
// GOCQL_CCM_TEST_VERSION pick what the live tests install; the version
// defaults to the shared cluster's, else ScyllaDB release:2026.3.4 or
// Cassandra 5.0.9.
func liveSpec(t *testing.T, nodes int) ClusterSpec {
	t.Helper()
	if err := Available(); err != nil {
		t.Skip("ccm not installed:", err)
	}
	dist := Scylla
	if d := os.Getenv("GOCQL_CCM_TEST_DISTRIBUTION"); d != "" {
		var err error
		if dist, err = ParseDistribution(d); err != nil {
			t.Fatal(err)
		}
	}
	raw := os.Getenv("GOCQL_CCM_TEST_VERSION")
	if raw == "" {
		if shared, err := AttachShared(dist); err == nil {
			raw = shared.Spec().Version.CCMArg
			shared.Close()
		} else if dist == Cassandra {
			raw = "5.0.9"
		} else {
			raw = "release:2026.3.4"
		}
	}
	v, err := ParseVersion(dist, raw)
	if err != nil {
		t.Fatal(err)
	}
	return ClusterSpec{Version: v, Nodes: []int{nodes}}
}

// otherConfig is a setting that makes a second spec differ from liveSpec's.
func otherConfig(s ClusterSpec) []string {
	if s.Distribution() == Cassandra {
		return []string{"concurrent_reads: 4"}
	}
	return []string{"enable_tablets: false"}
}

func TestCCMNodeStopStart(t *testing.T) {
	spec := liveSpec(t, 1)
	c := New(t, spec)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	v, err := c.Version(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := strings.TrimPrefix(spec.Version.CCMArg, "release:"); !strings.HasPrefix(v.Raw, want) {
		t.Fatalf("server reports %s, asked for %s", v.Raw, spec.Version.CCMArg)
	}
	t.Log("server version", v)
	n := c.Node(1)
	if err := n.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if st, err := n.Status(ctx); err != nil || st.State != NodeDown {
		t.Fatal("after stop", st, err)
	}
	if n.acceptsNative() {
		t.Fatal("stopped node accepts connections")
	}
	if err := n.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if !n.acceptsNative() {
		t.Fatal("started node refuses connections")
	}
	if len(c.StoppedNodes()) != 0 {
		t.Fatal("node still recorded as stopped")
	}
}

func TestCCMPoolRunsClustersSideBySide(t *testing.T) {
	spec := liveSpec(t, 1)
	two := spec
	two.Config = otherConfig(spec)
	p := NewPool(PoolConfig{})
	// Registered first, so it runs after the lease releases below.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := p.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	var wg sync.WaitGroup
	clusters := make([]*Cluster, 2)
	for i, s := range []ClusterSpec{spec, two} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := p.Acquire(context.Background(), t, Request{Spec: s, Exclusive: true})
			if err != nil {
				t.Error(err)
				return
			}
			clusters[i] = l.Cluster()
			t.Cleanup(func() { l.Release(t.Failed()) })
		}()
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	if clusters[0].IPPrefix() == clusters[1].IPPrefix() {
		t.Fatal("clusters share an address prefix")
	}
	for _, c := range clusters {
		if !c.Node(1).acceptsNative() {
			t.Fatal(c.Name(), "not up")
		}
	}
}

func TestCCMSharedIsReadOnly(t *testing.T) {
	if err := Available(); err != nil {
		t.Skip(err)
	}
	c := Shared(t, liveSpec(t, 1).Distribution())
	if err := c.Node(1).Stop(context.Background()); !errors.Is(err, ErrNotOwned) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	v, err := c.Version(ctx)
	if err != nil {
		t.Skip("shared cluster not running:", err)
	}
	t.Log("shared cluster", c.Name(), "runs", v.Raw, "hosts", c.Hosts())
}

func TestCCMPoolBudgetEvicts(t *testing.T) {
	first := liveSpec(t, 1)
	second := first
	second.Config = otherConfig(first)
	p := NewPool(PoolConfig{Budget: Resources{CPUs: 1}})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := p.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	a, err := p.Acquire(ctx, t, Request{Spec: first, Exclusive: true})
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan *Lease, 1)
	go func() {
		l, err := p.Acquire(ctx, t, Request{Spec: second, Exclusive: true})
		if err != nil {
			t.Error(err)
		}
		got <- l
	}()
	select {
	case <-got:
		t.Fatal("second cluster created over budget")
	case <-time.After(2 * time.Second):
	}
	firstNode := a.Cluster().Node(1)
	a.Release(false)
	b := <-got
	if b == nil {
		return
	}
	defer b.Release(false)
	// The second cluster may reuse the freed address prefix, so check the
	// first one's files rather than its port.
	if _, err := os.Stat(firstNode.Dir()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("first cluster not removed by eviction:", err)
	}
	if !b.Cluster().Node(1).acceptsNative() {
		t.Fatal("second cluster not up")
	}
	if used := p.Used(); used.CPUs != 1 {
		t.Fatal("used", used)
	}
}
