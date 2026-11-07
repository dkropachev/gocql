// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package ccm

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"
)

// Request asks a Provider for a cluster.
type Request struct {
	Spec ClusterSpec
	// Exclusive leases hold the cluster alone and may stop and start its
	// nodes. Shared leases run alongside each other and get a read-only
	// cluster: node control returns ErrNotOwned.
	Exclusive bool
	// Pinned marks a lease held until the pool closes, such as the cluster
	// a whole test run uses. Its cost never comes back to waiting requests,
	// so a request that cannot fit next to pinned clusters fails at once
	// instead of waiting forever.
	Pinned bool
}

// Provider hands out clusters. Pool is the implementation.
type Provider interface {
	Acquire(ctx context.Context, t testing.TB, req Request) (*Lease, error)
}

// PoolConfig configures a Pool.
type PoolConfig struct {
	Runner Runner
	// ArtifactsDir holds one directory per cluster (its ccm config dir,
	// ccm.log and logs of failed leases). Empty means DefaultArtifactsDir().
	ArtifactsDir string
	// LockDir holds address prefix locks. Empty means DefaultLockDir().
	LockDir string
	// Budget caps the resources (ClusterSpec.Cost) of the clusters the pool
	// keeps at once. A request for a new cluster that does not fit waits,
	// first come first served, while the pool removes idle clusters, oldest
	// first, to make room. A request larger than the whole budget fails.
	// Zero means $GOCQL_CCM_BUDGET ("cpus=8,memory=16G"), else unlimited.
	Budget Resources
	// KeepOnFailure leaves a cluster running at Close if any of its leases
	// failed, for debugging. Default from GOCQL_CCM_KEEP.
	KeepOnFailure bool
}

// DefaultArtifactsDir is $GOCQL_CCM_ARTIFACTS or <tmp>/gocql-ccm.
func DefaultArtifactsDir() string {
	if d := os.Getenv("GOCQL_CCM_ARTIFACTS"); d != "" {
		return d
	}
	return filepath.Join(os.TempDir(), "gocql-ccm")
}

// Pool creates clusters on demand and reuses one cluster for every request
// with the same spec, within a resource budget. Create it once per test
// process and Close it after the tests, typically in TestMain.
type Pool struct {
	entries map[string]*poolEntry
	// changed is closed and replaced whenever a lease ends, a cluster goes
	// away or a waiter leaves, waking every request waiting for budget.
	changed chan struct{}
	// queue holds requests waiting to create a cluster, oldest first; only
	// the head may take budget or evict.
	queue []*budgetWaiter
	cfg   PoolConfig
	used  Resources
	// pinned is the cost of clusters with a pinned lease.
	pinned Resources
	mu     sync.Mutex
	closed bool
}

type budgetWaiter struct{ spec ClusterSpec }

const exclusiveWeight = 1 << 30

type poolEntry struct {
	lastUsed time.Time
	err      error
	ready    chan struct{}
	c        *Cluster
	sem      *semaphore.Weighted
	key      string
	cost     Resources
	// leases counts held and pending leases; guarded by Pool.mu.
	leases int
	mu     sync.Mutex
	broken bool
	failed bool
	// pinned: a pinned lease holds the entry; guarded by Pool.mu.
	pinned bool
}

// NewPool returns an empty pool.
func NewPool(cfg PoolConfig) *Pool {
	if cfg.Runner == nil {
		cfg.Runner = ExecRunner{}
	}
	if cfg.ArtifactsDir == "" {
		cfg.ArtifactsDir = DefaultArtifactsDir()
	}
	if os.Getenv("GOCQL_CCM_KEEP") != "" {
		cfg.KeepOnFailure = true
	}
	if cfg.Budget.unlimited() {
		if b, err := ParseResources(os.Getenv("GOCQL_CCM_BUDGET")); err == nil {
			cfg.Budget = b
		} else {
			fmt.Fprintln(os.Stderr, "ccm: ignoring GOCQL_CCM_BUDGET:", err)
		}
	}
	if cfg.LockDir == "" {
		cfg.LockDir = DefaultLockDir()
	}
	if err := os.MkdirAll(cfg.LockDir, 0o777); err == nil {
		for _, name := range sweepStale(cfg.Runner, cfg.ArtifactsDir, cfg.LockDir) {
			fmt.Fprintf(os.Stderr, "ccm: stopped cluster %s, left running by a test process that exited\n", name)
		}
	}
	return &Pool{cfg: cfg, entries: map[string]*poolEntry{}, changed: make(chan struct{})}
}

// markKept records that a cluster is left running on purpose, so later
// sweeps leave it alone.
func markKept(c *Cluster) {
	_ = os.WriteFile(filepath.Join(c.ConfigDir(), keepMarker), []byte("kept after a failed test (GOCQL_CCM_KEEP)\n"), 0o644)
	fmt.Fprintf(os.Stderr, "ccm: keeping cluster %s (config dir %s) after a failed test\n", c.Name(), c.ConfigDir())
}

// Budget is the pool's budget; zero fields are unlimited.
func (p *Pool) Budget() Resources { return p.cfg.Budget }

// Used is the cost of the clusters the pool keeps now.
func (p *Pool) Used() Resources {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.used
}

func (p *Pool) broadcastLocked() {
	close(p.changed)
	p.changed = make(chan struct{})
}

// Lease is a cluster held by one test.
type Lease struct {
	pool      *Pool
	entry     *poolEntry
	cluster   *Cluster
	logf      func(string, ...any)
	mark      LogMark
	test      string
	once      sync.Once
	exclusive bool
}

// Cluster is the leased cluster.
func (l *Lease) Cluster() *Cluster { return l.cluster }

// Acquire returns a lease on a cluster built from req.Spec, creating and
// starting the cluster on first use. It blocks while an exclusive lease
// holds the cluster, and an exclusive request waits for every shared lease
// to end.
func (p *Pool) Acquire(ctx context.Context, t testing.TB, req Request) (*Lease, error) {
	spec := req.Spec.withDefaults()
	if err := spec.validate(); err != nil {
		return nil, err
	}
	key := spec.Key()
	weight := int64(1)
	if req.Exclusive {
		weight = exclusiveWeight
	}
	for {
		e, create, err := p.entry(ctx, key, spec)
		if err != nil {
			return nil, err
		}
		if create {
			e.c, e.err = p.create(ctx, t, spec)
			if e.err != nil {
				// Requests for this spec fail fast from now on; the budget
				// goes back to the others.
				p.mu.Lock()
				p.used = p.used.sub(e.cost)
				e.cost = Resources{}
				p.broadcastLocked()
				p.mu.Unlock()
			}
			close(e.ready)
		}
		select {
		case <-e.ready:
		case <-ctx.Done():
			p.dropPending(e)
			return nil, ctx.Err()
		}
		if e.err != nil {
			p.dropPending(e)
			return nil, e.err
		}
		if err := e.sem.Acquire(ctx, weight); err != nil {
			p.dropPending(e)
			return nil, err
		}
		e.mu.Lock()
		broken := e.broken
		e.mu.Unlock()
		if broken {
			e.sem.Release(weight)
			p.dropPending(e)
			continue // replaced by a fresh cluster on the next round
		}
		c := e.c
		if !req.Exclusive {
			c = c.readOnlyView()
		}
		name := "<no test>"
		logf := func(string, ...any) {}
		if t != nil {
			name = t.Name()
			logf = t.Logf
		}
		if req.Pinned {
			p.mu.Lock()
			if !e.pinned {
				e.pinned = true
				p.pinned = p.pinned.add(e.cost)
				p.broadcastLocked() // waiters that can no longer fit fail now
			}
			p.mu.Unlock()
		}
		kind := map[bool]string{true: "exclusive", false: "shared"}[req.Exclusive]
		e.c.Note("%s lease acquired by %s", kind, name)
		return &Lease{pool: p, entry: e, cluster: c, exclusive: req.Exclusive, test: name, logf: logf, mark: e.c.MarkLogs()}, nil
	}
}

// entry returns the pool entry for key with a pending lease counted on it.
// When there is none it reserves budget for a new one, waiting in line and
// evicting idle clusters as needed, and reports create: the caller then
// creates the cluster.
func (p *Pool) entry(ctx context.Context, key string, spec ClusterSpec) (_ *poolEntry, create bool, _ error) {
	cost := spec.Cost()
	w := &budgetWaiter{spec: spec}
	queued := false
	p.mu.Lock()
	defer func() {
		if queued {
			for i, q := range p.queue {
				if q == w {
					p.queue = append(p.queue[:i], p.queue[i+1:]...)
					break
				}
			}
			p.broadcastLocked()
		}
		p.mu.Unlock()
	}()
	for {
		if p.closed {
			return nil, false, errors.New("ccm: pool closed")
		}
		if e := p.entries[key]; e != nil {
			e.leases++
			return e, false, nil
		}
		budget := p.cfg.Budget
		if !cost.within(budget) {
			return nil, false, fmt.Errorf("ccm: cluster needs %v, more than the whole budget %v", cost, budget.budgetString())
		}
		if !cost.add(p.pinned).within(budget) {
			return nil, false, fmt.Errorf("ccm: cluster needs %v; budget %v minus %v held until the pool closes leaves too little", cost, budget.budgetString(), p.pinned)
		}
		if !queued {
			p.queue = append(p.queue, w)
			queued = true
		}
		if p.queue[0] == w {
			if p.used.add(cost).within(budget) {
				p.used = p.used.add(cost)
				e := &poolEntry{key: key, cost: cost, ready: make(chan struct{}), sem: semaphore.NewWeighted(exclusiveWeight), leases: 1}
				p.entries[key] = e
				return e, true, nil
			}
			if victim := p.idleLocked(); victim != nil {
				p.evictLocked(victim)
				continue
			}
		}
		changed := p.changed
		p.mu.Unlock()
		select {
		case <-changed:
			p.mu.Lock()
		case <-ctx.Done():
			p.mu.Lock()
			return nil, false, fmt.Errorf("ccm: waiting for budget (%v used of %v, need %v): %w", p.used, budget.budgetString(), cost, ctx.Err())
		}
	}
}

// idleLocked is the least recently used cluster no lease holds or waits
// for, claimed exclusively; nil when there is none.
func (p *Pool) idleLocked() *poolEntry {
	var victim *poolEntry
	for _, e := range p.entries {
		select {
		case <-e.ready:
		default:
			continue // being created
		}
		if e.err != nil || e.leases > 0 {
			continue
		}
		if victim == nil || e.lastUsed.Before(victim.lastUsed) {
			victim = e
		}
	}
	if victim == nil || !victim.sem.TryAcquire(exclusiveWeight) {
		return nil
	}
	return victim
}

// evictLocked removes an idle cluster the caller claimed with idleLocked.
// It drops p.mu while ccm removes the cluster and frees its budget after.
func (p *Pool) evictLocked(e *poolEntry) {
	e.mu.Lock()
	e.broken = true
	e.mu.Unlock()
	delete(p.entries, e.key)
	p.mu.Unlock()
	e.c.Note("evicted to free budget")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	err := e.c.Remove(ctx)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ccm: evict cluster %s: %v\n", e.c.Name(), err)
	}
	e.sem.Release(exclusiveWeight)
	p.mu.Lock()
	p.forgetLocked(e)
	p.broadcastLocked()
}

// forgetLocked takes a removed entry's cost off the books.
func (p *Pool) forgetLocked(e *poolEntry) {
	p.used = p.used.sub(e.cost)
	if e.pinned {
		p.pinned = p.pinned.sub(e.cost)
		e.pinned = false
	}
	e.cost = Resources{}
}

// dropPending undoes the pending lease entry counted.
func (p *Pool) dropPending(e *poolEntry) {
	p.mu.Lock()
	e.leases--
	e.lastUsed = time.Now()
	p.broadcastLocked()
	p.mu.Unlock()
}

// MustAcquire acquires a lease for t, fails t on error, and releases the
// lease in t.Cleanup, reporting whether t failed.
func (p *Pool) MustAcquire(t testing.TB, req Request) *Cluster {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	l, err := p.Acquire(ctx, t, req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Release(t.Failed()) })
	return l.Cluster()
}

// Release returns the cluster to the pool. When failed, the part of every
// node log written during the lease is copied to the artifacts directory.
// An exclusive lease restarts every node the test left stopped; if that
// fails the cluster is removed and the next request creates a new one.
func (l *Lease) Release(failed bool) {
	l.once.Do(func() { l.release(failed) })
}

func (l *Lease) release(failed bool) {
	e, c := l.entry, l.entry.c
	weight := int64(1)
	if l.exclusive {
		weight = exclusiveWeight
	}
	if failed {
		dst := filepath.Join(c.ConfigDir(), "failed", slug(l.test, 80))
		if err := c.CollectLogsSince(l.mark, dst); err != nil {
			l.logf("ccm: collect logs: %v", err)
		}
		l.logf("ccm: cluster %s logs for this test: %s\n%s", c.Name(), dst, c.LogTail(30))
		e.mu.Lock()
		e.failed = true
		e.mu.Unlock()
	}
	c.Note("lease released by %s (failed=%v)", l.test, failed)
	if l.exclusive {
		ctx, cancel := context.WithTimeout(context.Background(), 2*c.spec.StartTimeout)
		err := c.RestoreNodes(ctx)
		if err == nil {
			err = c.WaitUp(ctx)
		}
		cancel()
		if err != nil {
			l.logf("ccm: cluster %s could not be restored, removing it: %v", c.Name(), err)
			l.pool.discard(e)
		}
	}
	e.sem.Release(weight)
	l.pool.dropPending(e)
}

// discard removes a broken cluster; the caller holds it exclusively.
func (p *Pool) discard(e *poolEntry) {
	e.mu.Lock()
	e.broken = true
	keep := e.failed && p.cfg.KeepOnFailure
	e.mu.Unlock()
	p.mu.Lock()
	if p.entries[e.key] == e {
		delete(p.entries, e.key)
	}
	p.mu.Unlock()
	if keep {
		markKept(e.c)
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		_ = e.c.Remove(ctx)
		cancel()
	}
	// A kept cluster still runs, but it is no longer the pool's to count.
	p.mu.Lock()
	p.forgetLocked(e)
	p.broadcastLocked()
	p.mu.Unlock()
}

// Close waits for every lease to end and removes every cluster, or leaves a
// cluster running when KeepOnFailure is set and one of its leases failed.
func (p *Pool) Close(ctx context.Context) error {
	p.mu.Lock()
	p.closed = true
	entries := make([]*poolEntry, 0, len(p.entries))
	for _, e := range p.entries {
		entries = append(entries, e)
	}
	clear(p.entries)
	p.broadcastLocked()
	p.mu.Unlock()
	var errs []error
	for _, e := range entries {
		select {
		case <-e.ready:
		case <-ctx.Done():
			return ctx.Err()
		}
		if e.err != nil {
			continue
		}
		if err := e.sem.Acquire(ctx, exclusiveWeight); err != nil {
			// A lease outlived the tests. Remove the cluster anyway rather
			// than leak its processes.
			errs = append(errs, fmt.Errorf("ccm: cluster %s still leased at close: %w", e.c.Name(), err))
		}
		e.mu.Lock()
		keep := e.failed && p.cfg.KeepOnFailure
		e.mu.Unlock()
		if keep {
			markKept(e.c)
			continue
		}
		rmCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Minute)
		if err := e.c.Remove(rmCtx); err != nil {
			errs = append(errs, err)
		}
		cancel()
		p.mu.Lock()
		p.forgetLocked(e)
		p.mu.Unlock()
	}
	return errors.Join(errs...)
}

func (p *Pool) create(ctx context.Context, t testing.TB, spec ClusterSpec) (*Cluster, error) {
	owner := "pool"
	if t != nil {
		owner = t.Name()
	}
	suffix := make([]byte, 3)
	_, _ = rand.Read(suffix)
	name := "gocql_" + slug(owner, 32) + "_" + hex.EncodeToString(suffix)
	dir := filepath.Join(p.cfg.ArtifactsDir, name)
	c, err := Create(ctx, CreateConfig{Runner: p.cfg.Runner, ConfigDir: dir, Name: name, LockDir: p.cfg.LockDir}, spec)
	if err != nil {
		return nil, err
	}
	c.Note("created for %s, spec key %s", owner, spec.Key())
	if err := c.Start(ctx); err != nil {
		logs := filepath.Join(dir, "failed-start")
		_ = c.CollectLogs(logs)
		err = fmt.Errorf("%w\nnode logs: %s\n%s", err, logs, c.LogTail(30))
		rmCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Minute)
		defer cancel()
		_ = c.Remove(rmCtx)
		return nil, err
	}
	if err := c.WaitUp(ctx); err != nil {
		rmCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Minute)
		defer cancel()
		_ = c.Remove(rmCtx)
		return nil, err
	}
	return c, nil
}

// readOnlyView is the cluster without node control, for shared leases.
func (c *Cluster) readOnlyView() *Cluster {
	v := &Cluster{name: c.name, configDir: c.configDir, runner: c.runner, spec: c.spec, dist: c.dist, ccmArg: c.ccmArg, ipPrefix: c.ipPrefix, config: c.config, log: c.log, stopped: map[string]bool{}}
	v.nodes = make([]*Node, len(c.nodes))
	for i, n := range c.nodes {
		cp := *n
		cp.cluster = v
		v.nodes[i] = &cp
	}
	return v
}

// slug makes s safe for file and ccm cluster names.
func slug(s string, limit int) string {
	var b strings.Builder
	under := false
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			under = false
		} else if !under {
			b.WriteByte('_')
			under = true
		}
	}
	out := strings.Trim(b.String(), "_")
	if len(out) > limit {
		out = out[:limit]
	}
	if out == "" {
		out = "x"
	}
	return out
}
