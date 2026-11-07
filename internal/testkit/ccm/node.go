// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package ccm

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Node is one node of a Cluster.
type Node struct {
	cluster *Cluster
	addr    netip.AddrPort
	name    string
	index   int
}

// Name is the ccm node name, "node1".
func (n *Node) Name() string { return n.name }

// Index is the 1-based node number.
func (n *Node) Index() int { return n.index }

// Addr is the native transport address.
func (n *Node) Addr() netip.AddrPort { return n.addr }

// IP is the node's address.
func (n *Node) IP() string { return n.addr.Addr().String() }

// Dir is the node's directory.
func (n *Node) Dir() string { return filepath.Join(n.cluster.Dir(), n.name) }

// LogFile is the node's main log.
func (n *Node) LogFile() string { return filepath.Join(n.Dir(), "logs", "system.log") }

// LogFiles are every log file of the node.
func (n *Node) LogFiles() []string {
	files, _ := filepath.Glob(filepath.Join(n.Dir(), "logs", "*.log"))
	return files
}

// ShardAwarePort is the configured native_shard_aware_transport_port.
func (n *Node) ShardAwarePort() (uint16, bool) {
	v, ok := n.cluster.config["native_shard_aware_transport_port"]
	if !ok {
		return 0, false
	}
	p, err := strconv.ParseUint(fmt.Sprint(v), 10, 16)
	return uint16(p), err == nil
}

// Status runs ccm status and returns this node's line.
func (n *Node) Status(ctx context.Context) (NodeStatus, error) {
	st, err := n.cluster.Status(ctx)
	if err != nil {
		return NodeStatus{}, err
	}
	for _, s := range st {
		if s.Name == n.name {
			return s, nil
		}
	}
	return NodeStatus{Name: n.name, State: NodeUnknown}, nil
}

// Stop shuts the node down gracefully and returns once its process exited.
// The cluster remembers the node as stopped until Start; a Pool restarts it
// when the lease ends.
func (n *Node) Stop(ctx context.Context) error { return n.stop(ctx, "--gently") }

// Kill stops the node with SIGKILL.
func (n *Node) Kill(ctx context.Context) error { return n.stop(ctx, "--not-gently") }

func (n *Node) stop(ctx context.Context, how string) error {
	if !n.cluster.owned {
		return ErrNotOwned
	}
	pid := n.pid()
	n.cluster.markStopped(n, true)
	if _, err := n.cluster.run(ctx, n.name, "stop", how); err != nil {
		return err
	}
	return waitExit(ctx, []int{pid})
}

// Start starts the node and waits until it accepts native connections.
func (n *Node) Start(ctx context.Context) error {
	if !n.cluster.owned {
		return ErrNotOwned
	}
	ctx, cancel := context.WithTimeout(ctx, n.cluster.spec.StartTimeout)
	defer cancel()
	args := append([]string{n.name, "start", "--wait-for-binary-proto"}, n.cluster.spec.startArgs()...)
	if _, err := n.cluster.run(ctx, args...); err != nil {
		return err
	}
	n.cluster.markStopped(n, false)
	return nil
}

// Nodetool runs nodetool on the node.
func (n *Node) Nodetool(ctx context.Context, args ...string) ([]byte, error) {
	return n.cluster.run(ctx, append([]string{n.name, "nodetool"}, args...)...)
}

func (n *Node) releaseVersion(ctx context.Context) (string, error) {
	if n.cluster.dist == Scylla {
		return scyllaAPIVersion(ctx, n.IP())
	}
	out, err := n.Nodetool(ctx, "version")
	if err != nil {
		return "", err
	}
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		if v, ok := strings.CutPrefix(strings.TrimSpace(sc.Text()), "ReleaseVersion:"); ok {
			return strings.TrimSpace(v), nil
		}
	}
	return "", fmt.Errorf("ccm: no ReleaseVersion in nodetool version output %q", out)
}

// dialNative checks a node's native port; tests replace it.
var dialNative = (*Node).dialNative

func (n *Node) acceptsNative() bool { return dialNative(n) }

func (n *Node) dialNative() bool {
	c, err := net.DialTimeout("tcp", n.addr.String(), time.Second)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

// pid is the node's process id, from cassandra.pid (both distributions
// write it) or node.conf.
func (n *Node) pid() int {
	if data, err := os.ReadFile(filepath.Join(n.Dir(), "cassandra.pid")); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil {
			return pid
		}
	}
	var nc nodeConf
	if readYAML(filepath.Join(n.Dir(), "node.conf"), &nc) == nil {
		return nc.PID
	}
	return 0
}

// NodeState is a node state as ccm status prints it.
type NodeState string

const (
	NodeUp      NodeState = "UP"
	NodeDown    NodeState = "DOWN"
	NodeUnknown NodeState = "UNKNOWN"
)

// NodeStatus is one line of ccm status.
type NodeStatus struct {
	Name  string
	State NodeState
}

// parseStatus reads "nodeN: STATE" lines of ccm status; the header and
// verbose detail lines are skipped.
func parseStatus(out []byte) []NodeStatus {
	var st []NodeStatus
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t") {
			continue
		}
		name, state, ok := strings.Cut(line, ":")
		if !ok || !isNodeName(strings.TrimSpace(name)) {
			continue
		}
		s := NodeState(strings.ToUpper(strings.TrimSpace(state)))
		switch {
		case s == NodeUp:
		case strings.HasPrefix(string(s), "DOWN"), s == "(NOT STARTED)", s == "DECOMMISSIONED":
			s = NodeDown
		default:
			s = NodeUnknown
		}
		st = append(st, NodeStatus{Name: strings.TrimSpace(name), State: s})
	}
	return st
}
