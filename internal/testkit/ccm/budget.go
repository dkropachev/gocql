// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package ccm

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// Resources is an amount of host CPU and memory. A zero field is unlimited
// when the value is a budget.
type Resources struct {
	CPUs     int
	MemoryMB int
}

func (r Resources) String() string {
	return fmt.Sprintf("cpus=%d,memory=%dM", r.CPUs, r.MemoryMB)
}

// budgetString prints r as a budget, where zero means unlimited.
func (r Resources) budgetString() string {
	cpus, mem := "unlimited", "unlimited"
	if r.CPUs > 0 {
		cpus = fmt.Sprint(r.CPUs)
	}
	if r.MemoryMB > 0 {
		mem = fmt.Sprintf("%dM", r.MemoryMB)
	}
	return "cpus=" + cpus + ",memory=" + mem
}

func (r Resources) unlimited() bool { return r.CPUs == 0 && r.MemoryMB == 0 }

func (r Resources) add(o Resources) Resources {
	return Resources{CPUs: r.CPUs + o.CPUs, MemoryMB: r.MemoryMB + o.MemoryMB}
}

func (r Resources) sub(o Resources) Resources {
	return Resources{CPUs: r.CPUs - o.CPUs, MemoryMB: r.MemoryMB - o.MemoryMB}
}

// within reports whether r fits budget b.
func (r Resources) within(b Resources) bool {
	return (b.CPUs == 0 || r.CPUs <= b.CPUs) && (b.MemoryMB == 0 || r.MemoryMB <= b.MemoryMB)
}

// Cost is what a running cluster of the spec takes from the host: SMP CPUs
// and MemoryMB per ScyllaDB node; one CPU and twice the heap (heap plus JVM
// overhead) per Cassandra node.
func (s ClusterSpec) Cost() Resources {
	s = s.withDefaults()
	n := s.NodeCount()
	if s.Distribution() == Cassandra {
		return Resources{CPUs: n, MemoryMB: 2 * s.MemoryMB * n}
	}
	return Resources{CPUs: s.SMP * n, MemoryMB: s.MemoryMB * n}
}

// ParseResources reads "cpus=8,memory=16G"; memory takes an M or G suffix
// (M when none). Either part may be left out, leaving it unlimited.
func ParseResources(s string) (Resources, error) {
	var r Resources
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			return r, fmt.Errorf("ccm: budget %q: want key=value", part)
		}
		switch strings.TrimSpace(k) {
		case "cpus", "cpu":
			n, err := strconv.Atoi(strings.TrimSpace(v))
			if err != nil || n < 0 {
				return r, fmt.Errorf("ccm: budget cpus %q", v)
			}
			r.CPUs = n
		case "memory", "mem":
			v = strings.ToUpper(strings.TrimSpace(v))
			mult := 1
			if g, ok := strings.CutSuffix(v, "G"); ok {
				v, mult = g, 1024
			} else {
				v = strings.TrimSuffix(v, "M")
			}
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				return r, fmt.Errorf("ccm: budget memory %q", v)
			}
			r.MemoryMB = n * mult
		default:
			return r, fmt.Errorf("ccm: budget key %q, want cpus or memory", k)
		}
	}
	return r, nil
}

// HostBudget is all CPUs and three quarters of the memory of this host, the
// share test clusters may take when nothing else is configured. Memory is
// unlimited where /proc/meminfo is unavailable.
func HostBudget() Resources {
	r := Resources{CPUs: runtime.NumCPU()}
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return r
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 2 && fields[0] == "MemTotal:" {
			if kb, err := strconv.Atoi(fields[1]); err == nil {
				r.MemoryMB = kb / 1024 * 3 / 4
			}
		}
	}
	return r
}
