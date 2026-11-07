// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package ccm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

// ClusterSpec describes a cluster to create. Two requests with equal specs
// (equal Key) can share one cluster.
type ClusterSpec struct {
	Version      VersionSpec
	Tag          string
	Nodes        []int
	Config       []string
	Args         []string
	SMP          int
	MemoryMB     int
	StartTimeout time.Duration
	VNodes       bool
}

// Distribution is the database the spec runs.
func (s ClusterSpec) Distribution() Distribution { return s.Version.Distribution }

func (s ClusterSpec) withDefaults() ClusterSpec {
	if len(s.Nodes) == 0 {
		s.Nodes = []int{1}
	}
	if s.SMP == 0 {
		s.SMP = 1
	}
	if s.MemoryMB == 0 {
		s.MemoryMB = 1024
		if s.Distribution() == Cassandra {
			s.MemoryMB = 512
		}
	}
	if s.StartTimeout == 0 {
		s.StartTimeout = 5 * time.Minute
	}
	return s
}

func (s ClusterSpec) validate() error {
	if s.Version.CCMArg == "" {
		return fmt.Errorf("ccm: spec has no version")
	}
	if _, err := ParseDistribution(string(s.Distribution())); err != nil {
		return err
	}
	total := 0
	for _, n := range s.Nodes {
		if n <= 0 {
			return fmt.Errorf("ccm: data center with %d nodes", n)
		}
		total += n
	}
	if total > 9 {
		return fmt.Errorf("ccm: %d nodes; at most 9 fit one address prefix slot", total)
	}
	for _, c := range s.Config {
		if !strings.Contains(c, ":") {
			return fmt.Errorf("ccm: config %q is not key: value", c)
		}
	}
	return nil
}

// NodeCount is the total number of nodes.
func (s ClusterSpec) NodeCount() int {
	total := 0
	for _, n := range s.withDefaults().Nodes {
		total += n
	}
	return total
}

// Key identifies the spec for cluster reuse: specs with equal keys build
// identical clusters.
func (s ClusterSpec) Key() string {
	s = s.withDefaults()
	s.Config = slices.Clone(s.Config)
	slices.Sort(s.Config)
	b, _ := json.Marshal(struct {
		D      Distribution
		V      string
		Tag    string
		Nodes  []int
		Config []string
		Args   []string
		SMP    int
		Mem    int
		VNodes bool
	}{D: s.Distribution(), V: s.Version.CCMArg, Tag: s.Tag, Nodes: s.Nodes, Config: s.Config, Args: s.Args, SMP: s.SMP, Mem: s.MemoryMB, VNodes: s.VNodes})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

// createArgs is the ccm create command line after the cluster name.
func (s ClusterSpec) createArgs(ipPrefix string) []string {
	nodes := make([]string, len(s.Nodes))
	for i, n := range s.Nodes {
		nodes[i] = fmt.Sprint(n)
	}
	args := []string{"-v", s.Version.CCMArg, "-n", strings.Join(nodes, ":"), "-i", ipPrefix}
	if s.Distribution() == Scylla {
		args = append(args, "--scylla")
		jvm := append([]string{fmt.Sprintf("--smp %d --memory %dM", s.SMP, s.MemoryMB)}, s.Args...)
		args = append(args, "--jvm_arg="+strings.Join(jvm, " "))
	}
	if s.VNodes {
		args = append(args, "--vnodes")
	}
	return args
}

// startArgs are the arguments for ccm start and ccm nodeN start. Cassandra
// takes its JVM options at start, not at create.
func (s ClusterSpec) startArgs() []string {
	if s.Distribution() != Cassandra {
		return nil
	}
	jvm := append([]string{fmt.Sprintf("-Xms%dm -Xmx%dm", s.MemoryMB, s.MemoryMB)}, s.Args...)
	return []string{"--jvm_arg=" + strings.Join(jvm, " ")}
}

// flattenConfig turns a nested config_options map into updateconf lines.
//
// Strings are written bare: Cassandra's ccm keeps any value that is not
// true, false or an integer as the raw text, quotes included, while
// ScyllaDB's ccm parses values as YAML. Lists and maps are written as YAML
// flow collections, which only ScyllaDB's ccm reads back as such.
func flattenConfig(prefix string, m map[string]any, out *[]string) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		switch v := m[k].(type) {
		case nil:
		case map[string]any:
			flattenConfig(key, v, out)
		case string:
			*out = append(*out, key+": "+v)
		default:
			b, err := json.Marshal(v)
			if err != nil {
				b = []byte(fmt.Sprint(v))
			}
			*out = append(*out, key+": "+string(b))
		}
	}
}
