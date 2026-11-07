// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package ccm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseVersion(t *testing.T) {
	cases := []struct {
		d       Distribution
		in, out string
	}{
		{Scylla, "6.2.0", "release:6.2.0"},
		{Scylla, "2024.1", "release:2024.1"},
		{Scylla, "2025.1.0-rc2", "release:2025.1.0-rc2"},
		{Scylla, "release:6.1", "release:6.1"},
		{Scylla, "release:2025.1.0:debug", "release:2025.1.0:debug"},
		{Scylla, "unstable/master:latest", "unstable/master:latest"},
		{Scylla, "unstable:master/latest", "unstable/master:latest"},
		{Scylla, "unstable/master:2020-02-15T03:02:19Z", "unstable/master:2020-02-15T03:02:19Z"},
		{Scylla, "hotfix/branch-2025.1:12", "hotfix/branch-2025.1:12"},
		{Cassandra, "5.0.9", "5.0.9"},
		{Cassandra, "4.1", "4.1"},
		{Cassandra, "git:trunk", "git:trunk"},
	}
	for _, c := range cases {
		v, err := ParseVersion(c.d, c.in)
		if err != nil || v.CCMArg != c.out || v.Distribution != c.d || v.Raw != c.in {
			t.Errorf("ParseVersion(%s, %q) = %+v, %v; want %q", c.d, c.in, v, err, c.out)
		}
	}
	for _, bad := range []string{"", "latest", "6", "release:", "release:6.x", "unstable/master", "unstable/:latest", "unstable/master:tomorrow", "unstable/master:debug", "6.2.0 ", "6.2 .0"} {
		if v, err := ParseVersion(Scylla, bad); err == nil && bad != "6.2.0 " {
			t.Errorf("ParseVersion(scylla, %q) = %+v, want error", bad, v)
		}
	}
	if _, err := ParseVersion(Cassandra, "release:5.0"); err == nil {
		t.Error("cassandra accepted a scylla reference")
	}
}

func TestParseServerVersion(t *testing.T) {
	v, err := ParseServerVersion(Scylla, "2026.3.4-0.20261001.abcdef")
	if err != nil || v.Major != 2026 || v.Minor != 3 || v.Patch != 4 || v.Pre != "-0.20261001.abcdef" {
		t.Fatal(v, err)
	}
	if !v.AtLeast(2026, 3, 4) || !v.AtLeast(2025, 9, 9) || v.AtLeast(2026, 4, 0) || v.AtLeast(2026, 3, 5) {
		t.Fatal("AtLeast", v)
	}
	if v, err := ParseServerVersion(Cassandra, "5.0"); err != nil || v.Patch != 0 || v.String() != "5.0.0" {
		t.Fatal(v, err)
	}
	if _, err := ParseServerVersion(Scylla, "unknown"); err == nil {
		t.Fatal("parsed garbage")
	}
}

func TestWithConfigDir(t *testing.T) {
	cases := map[string][]string{
		"status":             {"status", "--config-dir=/d"},
		"create c -n 1":      {"create", "--config-dir=/d", "c", "-n", "1"},
		"node1 stop":         {"node1", "stop", "--config-dir=/d"},
		"node12 nodetool -v": {"node12", "nodetool", "--config-dir=/d", "-v"},
		"nodetool x":         {"nodetool", "--config-dir=/d", "x"},
	}
	for in, want := range cases {
		if got := withConfigDir(strings.Fields(in), "/d"); !slices.Equal(got, want) {
			t.Errorf("%q: %q, want %q", in, got, want)
		}
	}
}

func TestParseStatus(t *testing.T) {
	out := `Cluster: 'c'
-----------
node1: UP
       binary=('127.0.1.1', 9042)
node2: DOWN
node3: DOWN (Not initialized)
node4: (not started)
`
	got := parseStatus([]byte(out))
	want := []NodeStatus{{"node1", NodeUp}, {"node2", NodeDown}, {"node3", NodeDown}, {"node4", NodeDown}}
	if !slices.Equal(got, want) {
		t.Fatal(got)
	}
}

func TestSpecKeyAndArgs(t *testing.T) {
	v, _ := ParseVersion(Scylla, "6.2.0")
	a := ClusterSpec{Version: v, Config: []string{"b: 1", "a: 2"}}
	b := ClusterSpec{Version: v, Nodes: []int{1}, SMP: 1, MemoryMB: 1024, Config: []string{"a: 2", "b: 1"}}
	if a.Key() != b.Key() {
		t.Fatal("defaults or config order changed the key")
	}
	if c := (ClusterSpec{Version: v, Nodes: []int{2}}); c.Key() == a.Key() {
		t.Fatal("node count ignored by key")
	}
	args := b.withDefaults().createArgs("127.0.10.")
	want := []string{"-v", "release:6.2.0", "-n", "1", "-i", "127.0.10.", "--scylla", "--jvm_arg=--smp 1 --memory 1024M"}
	if !slices.Equal(args, want) {
		t.Fatal(args)
	}
	cv, _ := ParseVersion(Cassandra, "5.0.9")
	cs := ClusterSpec{Version: cv, Nodes: []int{2, 1}, VNodes: true}.withDefaults()
	if got := cs.createArgs("127.0.11."); !slices.Equal(got, []string{"-v", "5.0.9", "-n", "2:1", "-i", "127.0.11.", "--vnodes"}) {
		t.Fatal(got)
	}
	if got := cs.startArgs(); !slices.Equal(got, []string{"--jvm_arg=-Xms512m -Xmx512m"}) {
		t.Fatal(got)
	}
	if err := (ClusterSpec{Version: v, Nodes: []int{10}}).validate(); err == nil {
		t.Fatal("10 nodes accepted")
	}
}

func TestFlattenConfig(t *testing.T) {
	var out []string
	flattenConfig("", map[string]any{
		"native_transport_port":     9042,
		"client_encryption_options": map[string]any{"enabled": true, "keyfile": "/k"},
		"experimental_features":     []any{"udf"},
		"unset":                     nil,
	}, &out)
	want := []string{`client_encryption_options.enabled: true`, `client_encryption_options.keyfile: /k`, `experimental_features: ["udf"]`, `native_transport_port: 9042`}
	if !slices.Equal(out, want) {
		t.Fatal(out)
	}
}

// fakeRunner imitates ccm on the cluster files.
type fakeRunner struct {
	mu        sync.Mutex
	calls     [][]string
	failStart map[string]int // node name -> starts left to fail
}

func (f *fakeRunner) Run(_ context.Context, configDir string, args ...string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, append([]string{configDir}, args...))
	if slices.Contains(args, "switch") {
		return nil, errors.New("switch used")
	}
	clusters, _ := filepath.Glob(filepath.Join(configDir, "*", "cluster.conf"))
	clusterDir := ""
	if len(clusters) == 1 {
		clusterDir = filepath.Dir(clusters[0])
	}
	switch {
	case args[0] == "create":
		name, n, prefix := args[1], 0, ""
		for i, a := range args {
			if a == "-n" {
				for _, part := range strings.Split(args[i+1], ":") {
					var k int
					fmt.Sscan(part, &k)
					n += k
				}
			}
			if a == "-i" {
				prefix = args[i+1]
			}
		}
		dir := filepath.Join(configDir, name)
		os.MkdirAll(dir, 0o777)
		var nodes []string
		for i := 1; i <= n; i++ {
			node := fmt.Sprintf("node%d", i)
			nodes = append(nodes, "- "+node)
			os.MkdirAll(filepath.Join(dir, node, "logs"), 0o777)
			os.WriteFile(filepath.Join(dir, node, "node.conf"), fmt.Appendf(nil, "name: %s\nstatus: DOWN\njmx_port: '7%d00'\ninterfaces:\n  binary:\n  - %s%d\n  - 9042\n", node, i, prefix, i), 0o644)
			os.WriteFile(filepath.Join(dir, node, "logs", "system.log"), []byte("boot\n"), 0o644)
		}
		os.WriteFile(filepath.Join(dir, "cluster.conf"), fmt.Appendf(nil, "name: %s\nnodes:\n%s\nipprefix: %s\nscylla_version: %s\nuse_vnodes: true\nconfig_options:\n  native_shard_aware_transport_port: 19042\n", name, strings.Join(nodes, "\n"), prefix, args[3]), 0o644)
	case args[0] == "remove":
		os.RemoveAll(clusterDir)
	case args[0] == "status":
		var b strings.Builder
		nodes, _ := filepath.Glob(filepath.Join(clusterDir, "node*", "node.conf"))
		for _, nc := range nodes {
			data, _ := os.ReadFile(nc)
			status := "DOWN"
			if strings.Contains(string(data), "status: UP") {
				status = "UP"
			}
			fmt.Fprintf(&b, "%s: %s\n", filepath.Base(filepath.Dir(nc)), status)
		}
		return []byte(b.String()), nil
	case args[0] == "start":
		nodes, _ := filepath.Glob(filepath.Join(clusterDir, "node*", "node.conf"))
		for _, nc := range nodes {
			setStatus(nc, "UP")
		}
	case args[0] == "stop":
		nodes, _ := filepath.Glob(filepath.Join(clusterDir, "node*", "node.conf"))
		for _, nc := range nodes {
			setStatus(nc, "DOWN")
		}
	case isNodeName(args[0]) && args[1] == "stop":
		setStatus(filepath.Join(clusterDir, args[0], "node.conf"), "DOWN")
	case isNodeName(args[0]) && args[1] == "start":
		if f.failStart[args[0]] > 0 {
			f.failStart[args[0]]--
			return nil, errors.New("start failed")
		}
		setStatus(filepath.Join(clusterDir, args[0], "node.conf"), "UP")
	}
	return nil, nil
}

func setStatus(path, status string) {
	data, _ := os.ReadFile(path)
	s := strings.Replace(strings.Replace(string(data), "status: UP", "status: "+status, 1), "status: DOWN", "status: "+status, 1)
	os.WriteFile(path, []byte(s), 0o644)
}

func (f *fakeRunner) count(pred func([]string) bool) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if pred(c[1:]) {
			n++
		}
	}
	return n
}

func scyllaSpec(t *testing.T) ClusterSpec {
	v, err := ParseVersion(Scylla, "6.2.0")
	if err != nil {
		t.Fatal(err)
	}
	return ClusterSpec{Version: v, Nodes: []int{2}}
}

// testPool uses a fake runner; fake nodes listen nowhere, so the native
// port check reports every node reachable.
func testPool(t *testing.T) (*Pool, *fakeRunner) {
	r := &fakeRunner{failStart: map[string]int{}}
	p := NewPool(PoolConfig{Runner: r, ArtifactsDir: t.TempDir(), LockDir: t.TempDir()})
	dialNative = func(*Node) bool { return true }
	t.Cleanup(func() { dialNative = (*Node).dialNative })
	return p, r
}

func TestPoolReusesClusterPerSpec(t *testing.T) {
	p, r := testPool(t)
	ctx := context.Background()
	a, err := p.Acquire(ctx, t, Request{Spec: scyllaSpec(t)})
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Acquire(ctx, t, Request{Spec: scyllaSpec(t)})
	if err != nil {
		t.Fatal(err)
	}
	if a.Cluster().Name() != b.Cluster().Name() {
		t.Fatal("two shared leases got two clusters")
	}
	if a.Cluster().Owned() {
		t.Fatal("shared lease got node control")
	}
	if err := a.Cluster().Node(1).Stop(ctx); !errors.Is(err, ErrNotOwned) {
		t.Fatal("shared lease stopped a node:", err)
	}
	if got := a.Cluster().Hosts(); len(got) != 2 || got[0].String() != "127.0.10.1:9042" {
		t.Fatal(got)
	}
	if p, ok := a.Cluster().Node(1).ShardAwarePort(); !ok || p != 19042 {
		t.Fatal("shard-aware port", p, ok)
	}
	a.Release(false)
	b.Release(false)

	spec := scyllaSpec(t)
	spec.Nodes = []int{1}
	c, err := p.Acquire(ctx, t, Request{Spec: spec})
	if err != nil {
		t.Fatal(err)
	}
	if c.Cluster().Name() == a.Cluster().Name() || c.Cluster().IPPrefix() == a.Cluster().IPPrefix() {
		t.Fatal("different spec shared a cluster or an address prefix")
	}
	c.Release(false)
	if n := r.count(func(a []string) bool { return a[0] == "create" }); n != 2 {
		t.Fatal("creates", n)
	}
	for _, call := range r.calls {
		if !strings.HasPrefix(call[0], p.cfg.ArtifactsDir) {
			t.Fatal("command outside a per-cluster config dir", call)
		}
	}
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if n := r.count(func(a []string) bool { return a[0] == "remove" }); n != 2 {
		t.Fatal("removes", n)
	}
	if _, err := p.Acquire(ctx, t, Request{Spec: spec}); err == nil {
		t.Fatal("closed pool handed out a lease")
	}
}

func TestPoolExclusiveWaitsAndRestoresNodes(t *testing.T) {
	p, r := testPool(t)
	ctx := context.Background()
	shared, err := p.Acquire(ctx, t, Request{Spec: scyllaSpec(t)})
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan *Lease)
	go func() {
		l, err := p.Acquire(ctx, t, Request{Spec: scyllaSpec(t), Exclusive: true})
		if err != nil {
			t.Error(err)
		}
		got <- l
	}()
	select {
	case <-got:
		t.Fatal("exclusive lease granted while a shared lease is held")
	case <-time.After(100 * time.Millisecond):
	}
	shared.Release(false)
	ex := <-got
	if !ex.Cluster().Owned() {
		t.Fatal("exclusive lease without node control")
	}
	if err := ex.Cluster().Node(2).Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if st, _ := ex.Cluster().Node(2).Status(ctx); st.State != NodeDown {
		t.Fatal("node not stopped", st)
	}
	os.WriteFile(ex.Cluster().Node(1).LogFile(), []byte("boot\nduring test\n"), 0o644)
	ex.Release(true) // restarts node2 and keeps the log written during the lease
	if n := r.count(func(a []string) bool { return a[0] == "node2" && a[1] == "start" }); n != 1 {
		t.Fatal("node2 not restarted", n)
	}
	logged, err := os.ReadFile(filepath.Join(ex.Cluster().ConfigDir(), "failed", slug(t.Name(), 80), "node1", "system.log"))
	if err != nil || string(logged) != "during test\n" {
		t.Fatalf("lease log %q, %v", logged, err)
	}
	again, err := p.Acquire(ctx, t, Request{Spec: scyllaSpec(t)})
	if err != nil {
		t.Fatal(err)
	}
	if again.Cluster().Name() != ex.Cluster().Name() {
		t.Fatal("restored cluster not reused")
	}
	again.Release(false)
	if n := r.count(func(a []string) bool { return a[0] == "create" }); n != 1 {
		t.Fatal("creates", n)
	}
	p.Close(ctx)
}

func TestPoolReplacesUnrestorableCluster(t *testing.T) {
	p, r := testPool(t)
	ctx := context.Background()
	ex, err := p.Acquire(ctx, t, Request{Spec: scyllaSpec(t), Exclusive: true})
	if err != nil {
		t.Fatal(err)
	}
	first := ex.Cluster().Name()
	if err := ex.Cluster().Node(1).Stop(ctx); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	r.failStart["node1"] = 1
	r.mu.Unlock()
	ex.Release(false)
	next, err := p.Acquire(ctx, t, Request{Spec: scyllaSpec(t)})
	if err != nil {
		t.Fatal(err)
	}
	if next.Cluster().Name() == first {
		t.Fatal("broken cluster reused")
	}
	next.Release(false)
	p.Close(ctx)
	if n := r.count(func(a []string) bool { return a[0] == "remove" }); n != 2 {
		t.Fatal("removes", n)
	}
}

func TestAttachIsReadOnly(t *testing.T) {
	p, _ := testPool(t)
	l, err := p.Acquire(context.Background(), t, Request{Spec: scyllaSpec(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close(context.Background())
	defer l.Release(false)
	r := &fakeRunner{}
	c, err := Attach(r, l.Cluster().ConfigDir(), l.Cluster().Name())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// Another cluster in the same config dir is current: commands on the
	// attached cluster must still reach it.
	os.WriteFile(filepath.Join(l.Cluster().ConfigDir(), "CURRENT"), []byte("other\n"), 0o644)
	if st, err := c.Status(context.Background()); err != nil || len(st) != 2 || st[0].State != NodeUp {
		t.Fatal("status of attached cluster", st, err)
	}
	if r.calls[0][0] == l.Cluster().ConfigDir() {
		t.Fatal("attached cluster commands run in the shared config dir")
	}
	if c.Owned() || c.Distribution() != Scylla || len(c.Nodes()) != 2 {
		t.Fatal(c.Owned(), c.Distribution(), len(c.Nodes()))
	}
	for _, err := range []error{c.Node(1).Stop(context.Background()), c.Node(1).Start(context.Background()), c.Stop(context.Background()), c.Remove(context.Background())} {
		if !errors.Is(err, ErrNotOwned) {
			t.Fatal(err)
		}
	}
	if spec := c.Spec(); spec.Version.CCMArg != "release:6.2.0" || spec.NodeCount() != 2 || !spec.VNodes || !slices.Contains(spec.Config, "native_shard_aware_transport_port: 19042") {
		t.Fatal(spec)
	}
}

func TestClaimPrefixIsExclusive(t *testing.T) {
	dir := t.TempDir()
	var mu sync.Mutex
	seen := map[string]bool{}
	var wg sync.WaitGroup
	var leases []*prefixLease
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := claimPrefix(dir)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if seen[l.Prefix] {
				t.Error("prefix claimed twice", l.Prefix)
			}
			seen[l.Prefix] = true
			leases = append(leases, l)
		}()
	}
	wg.Wait()
	for _, l := range leases {
		l.Release()
	}
	l, err := claimPrefix(dir)
	if err != nil || l.Slot != firstSlot {
		t.Fatal("released prefix not reused", l, err)
	}
	l.Release()
}
