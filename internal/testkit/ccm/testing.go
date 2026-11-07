// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package ccm

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Cluster names make scylla-start and make cassandra-start create.
const (
	SharedScyllaName    = "gocql_scylla_integration_test"
	SharedCassandraName = "gocql_cassandra_integration_test"
)

// Available reports whether the ccm binary is installed.
func Available() error { return ExecRunner{}.Available() }

// New creates and starts a cluster owned by t alone, and removes it when t
// ends. It skips t when ccm is not installed. Prefer a shared Pool when
// several tests can use the same cluster.
func New(t testing.TB, spec ClusterSpec) *Cluster {
	t.Helper()
	if err := Available(); err != nil {
		t.Skip("ccm not installed:", err)
	}
	p := NewPool(PoolConfig{})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := p.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return p.MustAcquire(t, Request{Spec: spec, Exclusive: true})
}

// DefaultConfigDir is $CCM_CONFIG_DIR or ~/.ccm, where make scylla-start
// and make cassandra-start keep their clusters.
func DefaultConfigDir() string {
	if d := os.Getenv("CCM_CONFIG_DIR"); d != "" {
		return d
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".ccm")
}

// AttachShared attaches to the cluster make scylla-start or make
// cassandra-start created for d: $GOCQL_CCM_CLUSTER in DefaultConfigDir(),
// or the Makefile's name for d. Close the cluster when done.
func AttachShared(d Distribution) (*Cluster, error) {
	name := os.Getenv("GOCQL_CCM_CLUSTER")
	if name == "" {
		name = SharedScyllaName
		if d == Cassandra {
			name = SharedCassandraName
		}
	}
	dir := DefaultConfigDir()
	if _, err := os.Stat(filepath.Join(dir, name, "cluster.conf")); err != nil {
		return nil, fmt.Errorf("ccm: no cluster %s in %s", name, dir)
	}
	return Attach(nil, dir, name)
}

// RunningShared attaches to the shared cluster for d (see AttachShared)
// and returns it with the version its nodes report, or an error when it is
// missing or not every node is up within wait. Close the cluster when done.
func RunningShared(d Distribution, wait time.Duration) (*Cluster, ServerVersion, error) {
	c, err := AttachShared(d)
	if err != nil {
		return nil, ServerVersion{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	if err := c.WaitUp(ctx); err != nil {
		c.Close()
		return nil, ServerVersion{}, err
	}
	vctx, vcancel := context.WithTimeout(context.Background(), time.Minute)
	defer vcancel()
	v, err := c.Version(vctx)
	if err != nil {
		c.Close()
		return nil, ServerVersion{}, err
	}
	return c, v, nil
}

// Shared attaches t to the shared cluster for d, skipping t when there is
// none. The cluster is read-only: node control returns ErrNotOwned.
func Shared(t testing.TB, d Distribution) *Cluster {
	t.Helper()
	c, err := AttachShared(d)
	if err != nil {
		t.Skip(err)
	}
	t.Cleanup(c.Close)
	return c
}
