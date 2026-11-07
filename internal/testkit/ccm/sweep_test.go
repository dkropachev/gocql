// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

//go:build unix

package ccm

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
)

// killRunner stops nodes by killing the pid each node's cassandra.pid names.
type killRunner struct{ stops []string }

func (k *killRunner) Run(_ context.Context, configDir string, args ...string) ([]byte, error) {
	if args[0] == "stop" {
		k.stops = append(k.stops, filepath.Base(configDir))
		pids, _ := filepath.Glob(filepath.Join(configDir, "*", "node*", "cassandra.pid"))
		for _, f := range pids {
			data, _ := os.ReadFile(f)
			var pid int
			fmt.Sscan(strings.TrimSpace(string(data)), &pid)
			syscall.Kill(pid, syscall.SIGKILL)
		}
	}
	return nil, nil
}

// leftover writes the files of a cluster on slot whose node1 is a running
// process, as a crashed test process would leave them.
func leftover(t *testing.T, artifacts string, slot int) string {
	name := fmt.Sprintf("gocql_leftover_%d", slot)
	node := filepath.Join(artifacts, name, name, "node1")
	if err := os.MkdirAll(node, 0o777); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(artifacts, name, name, "cluster.conf"), fmt.Appendf(nil, "name: %s\nnodes:\n- node1\nipprefix: 127.0.%d.\nscylla_version: release:6.2.0\n", name, slot), 0o644)
	os.WriteFile(filepath.Join(node, "node.conf"), fmt.Appendf(nil, "name: node1\ninterfaces:\n  binary:\n  - 127.0.%d.1\n  - 9042\n", slot), 0o644)
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go cmd.Wait() // reap, so the pid disappears once killed
	t.Cleanup(func() { cmd.Process.Kill() })
	os.WriteFile(filepath.Join(node, "cassandra.pid"), fmt.Append(nil, cmd.Process.Pid), 0o644)
	return name
}

func TestSweepStopsOnlyOrphanedClusters(t *testing.T) {
	artifacts, locks := t.TempDir(), t.TempDir()
	orphan := leftover(t, artifacts, 20)
	owned := leftover(t, artifacts, 21)
	kept := leftover(t, artifacts, 22)
	os.WriteFile(filepath.Join(artifacts, kept, keepMarker), nil, 0o644)
	held, err := lockSlot(locks, 21) // a live pool owns slot 21
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()

	r := &killRunner{}
	stopped := sweepStale(r, artifacts, locks)
	if !slices.Equal(stopped, []string{orphan}) || !slices.Equal(r.stops, []string{orphan}) {
		t.Fatalf("stopped %v (ccm stop in %v), want only %s", stopped, r.stops, orphan)
	}
	if _, err := os.Stat(filepath.Join(artifacts, orphan, orphan, "node1", "node.conf")); err != nil {
		t.Fatal("sweep deleted the stopped cluster's files:", err)
	}
	if l, err := lockSlot(locks, 20); err != nil {
		t.Fatal("sweep kept the orphan's address lock:", err)
	} else {
		l.Release()
	}
	// Nothing left to stop on a second sweep.
	if again := sweepStale(r, artifacts, locks); len(again) != 0 {
		t.Fatal(again)
	}
	_ = owned
}
