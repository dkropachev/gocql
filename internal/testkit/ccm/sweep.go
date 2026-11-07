// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package ccm

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// keepMarker in a cluster's artifacts directory marks a cluster left
// running on purpose (KeepOnFailure); sweeps skip it.
const keepMarker = "KEEP"

// sweepStale stops clusters in artifactsDir that a crashed test process
// left running: their address lock is free, so no live Pool owns them, yet
// their nodes still run. They are stopped, not removed, so their logs stay
// for a look. It returns the names of the clusters it stopped.
func sweepStale(r Runner, artifactsDir, lockDir string) []string {
	dirs, _ := filepath.Glob(filepath.Join(artifactsDir, "gocql_*"))
	var stopped []string
	for _, dir := range dirs {
		name := filepath.Base(dir)
		if _, err := os.Stat(filepath.Join(dir, keepMarker)); err == nil {
			continue
		}
		var conf clusterConf
		if readYAML(filepath.Join(dir, name, "cluster.conf"), &conf) != nil {
			continue // removed, or not ours
		}
		slot, ok := prefixSlot(conf.IPPrefix)
		if !ok {
			continue
		}
		lease, err := lockSlot(lockDir, slot)
		if err != nil {
			continue // a live process owns it
		}
		c := &Cluster{name: name, configDir: dir, runner: r, owned: true, log: &commandLog{}, stopped: map[string]bool{}}
		if c.loadTopology() == nil && len(c.pids()) > 0 && anyAlive(c.pids()) {
			if log, err := openCommandLog(dir, nil); err == nil {
				c.log = log
			}
			c.Note("stopping: left running by a test process that exited")
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			if err := c.Stop(ctx); err == nil {
				stopped = append(stopped, name)
			} else {
				fmt.Fprintf(os.Stderr, "ccm: stop stale cluster %s: %v\n", name, err)
			}
			cancel()
			c.log.Close()
		}
		lease.Release()
	}
	return stopped
}

func anyAlive(pids []int) bool {
	for _, pid := range pids {
		if processAlive(pid) {
			return true
		}
	}
	return false
}

func prefixSlot(prefix string) (int, bool) {
	parts := strings.Split(strings.TrimSuffix(prefix, "."), ".")
	if len(parts) != 3 {
		return 0, false
	}
	slot, err := strconv.Atoi(parts[2])
	return slot, err == nil && slot >= firstSlot && slot <= lastSlot
}
