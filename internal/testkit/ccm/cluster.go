// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package ccm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.yaml.in/yaml/v3"
)

// ErrNotOwned is returned by node and cluster control on a cluster this
// package did not create.
var ErrNotOwned = errors.New("ccm: cluster is attached, not owned; node control is refused")

// Cluster is one ccm cluster. Its methods are safe for concurrent use.
type Cluster struct {
	runner     Runner
	log        *commandLog
	config     map[string]any
	version    *ServerVersion
	stopped    map[string]bool
	addr       *prefixLease
	ipPrefix   string
	ccmArg     string
	dist       Distribution
	name       string
	attachView string
	configDir  string
	nodes      []*Node
	spec       ClusterSpec
	mu         sync.Mutex
	vnodes     bool
	owned      bool
}

// CreateConfig places a cluster to create.
type CreateConfig struct {
	Runner    Runner
	Log       io.Writer
	ConfigDir string
	Name      string
	IPPrefix  string
	LockDir   string
}

// Create creates an owned cluster, applies its configuration and leaves it
// stopped. On error nothing is left behind.
func Create(ctx context.Context, cfg CreateConfig, spec ClusterSpec) (_ *Cluster, err error) {
	spec = spec.withDefaults()
	if err := spec.validate(); err != nil {
		return nil, err
	}
	if cfg.Runner == nil {
		cfg.Runner = ExecRunner{}
	}
	if cfg.Name == "" || cfg.ConfigDir == "" {
		return nil, fmt.Errorf("ccm: create needs a name and a config dir")
	}
	if err := os.MkdirAll(cfg.ConfigDir, 0o777); err != nil {
		return nil, fmt.Errorf("ccm: config dir: %w", err)
	}
	log, err := openCommandLog(cfg.ConfigDir, cfg.Log)
	if err != nil {
		return nil, err
	}
	c := &Cluster{name: cfg.Name, configDir: cfg.ConfigDir, runner: cfg.Runner, owned: true, spec: spec, dist: spec.Distribution(), ccmArg: spec.Version.CCMArg, log: log, stopped: map[string]bool{}}
	if cfg.IPPrefix == "" {
		lockDir := cfg.LockDir
		if lockDir == "" {
			lockDir = DefaultLockDir()
		}
		if c.addr, err = claimPrefix(lockDir); err != nil {
			log.Close()
			return nil, err
		}
		cfg.IPPrefix = c.addr.Prefix
	}
	c.ipPrefix = cfg.IPPrefix
	defer func() {
		if err != nil {
			rmCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
			defer cancel()
			_, _ = c.run(rmCtx, "remove")
			c.close()
		}
	}()
	if _, err := c.run(ctx, append([]string{"create", c.name}, spec.createArgs(c.ipPrefix)...)...); err != nil {
		return nil, err
	}
	if err := c.loadTopology(); err != nil {
		return nil, err
	}
	if err := c.relocatePorts(); err != nil {
		return nil, err
	}
	if _, err := c.run(ctx, append([]string{"updateconf"}, spec.Config...)...); err != nil {
		return nil, err
	}
	if c.dist == Cassandra {
		if err := c.setCassandraHeap(); err != nil {
			return nil, err
		}
	}
	if err := c.loadTopology(); err != nil {
		return nil, err
	}
	return c, nil
}

// Attach opens an existing cluster without taking ownership: node and
// cluster control return ErrNotOwned. Call Close when done.
//
// ccm commands act on the current cluster of their config dir, which other
// processes switch at will. Attach therefore runs every command in a
// private config dir holding only a link to the cluster and a CURRENT file
// naming it.
func Attach(r Runner, configDir, name string) (*Cluster, error) {
	if r == nil {
		r = ExecRunner{}
	}
	target := filepath.Join(configDir, name)
	if _, err := os.Stat(filepath.Join(target, "cluster.conf")); err != nil {
		return nil, fmt.Errorf("ccm: attach %s: %w", name, err)
	}
	view, err := os.MkdirTemp("", "gocql-ccm-attach-")
	if err != nil {
		return nil, fmt.Errorf("ccm: attach: %w", err)
	}
	if err := os.Symlink(target, filepath.Join(view, name)); err != nil {
		os.RemoveAll(view)
		return nil, fmt.Errorf("ccm: attach: %w", err)
	}
	if err := os.WriteFile(filepath.Join(view, "CURRENT"), []byte(name+"\n"), 0o644); err != nil {
		os.RemoveAll(view)
		return nil, fmt.Errorf("ccm: attach: %w", err)
	}
	c := &Cluster{name: name, configDir: view, attachView: view, runner: r, log: &commandLog{}, stopped: map[string]bool{}}
	if err := c.loadTopology(); err != nil {
		os.RemoveAll(view)
		return nil, err
	}
	return c, nil
}

// Close releases what Attach set up. It does not stop the cluster.
func (c *Cluster) Close() {
	if c.attachView != "" {
		os.RemoveAll(c.attachView)
	}
}

// Name is the ccm cluster name.
func (c *Cluster) Name() string { return c.name }

// ConfigDir is the ccm --config-dir holding the cluster.
func (c *Cluster) ConfigDir() string { return c.configDir }

// Dir is the cluster's own directory, holding one directory per node.
func (c *Cluster) Dir() string { return filepath.Join(c.configDir, c.name) }

// Owned reports whether this package created the cluster.
func (c *Cluster) Owned() bool { return c.owned }

// Distribution is the database the cluster runs.
func (c *Cluster) Distribution() Distribution { return c.dist }

// IPPrefix such as "127.0.10.".
func (c *Cluster) IPPrefix() string { return c.ipPrefix }

// Spec is the spec an owned cluster was created with. For an attached
// cluster it is rebuilt from the cluster files: version, node count, vnodes
// and configuration; sizing and node arguments are not recorded there.
func (c *Cluster) Spec() ClusterSpec {
	if c.owned {
		return c.spec
	}
	spec := ClusterSpec{Version: VersionSpec{Distribution: c.dist, Raw: c.ccmArg, CCMArg: c.ccmArg}, Nodes: []int{len(c.nodes)}, VNodes: c.vnodes}
	flattenConfig("", c.config, &spec.Config)
	return spec.withDefaults()
}

// Nodes are the cluster's nodes in ccm order.
func (c *Cluster) Nodes() []*Node { return c.nodes }

// Node is node i, 1-based like ccm's nodeN; nil when there is none.
func (c *Cluster) Node(i int) *Node {
	if i < 1 || i > len(c.nodes) {
		return nil
	}
	return c.nodes[i-1]
}

// Hosts are the nodes' native transport addresses.
func (c *Cluster) Hosts() []netip.AddrPort {
	out := make([]netip.AddrPort, len(c.nodes))
	for i, n := range c.nodes {
		out[i] = n.addr
	}
	return out
}

// IPs are the nodes' native transport IP addresses.
func (c *Cluster) IPs() []string {
	out := make([]string, len(c.nodes))
	for i, n := range c.nodes {
		out[i] = n.addr.Addr().String()
	}
	return out
}

// Start starts every stopped node and waits for their native transport.
func (c *Cluster) Start(ctx context.Context) error {
	if !c.owned {
		return ErrNotOwned
	}
	ctx, cancel := context.WithTimeout(ctx, c.spec.StartTimeout)
	defer cancel()
	args := append([]string{"start", "--wait-for-binary-proto", "--wait-other-notice"}, c.spec.startArgs()...)
	if _, err := c.run(ctx, args...); err != nil {
		return err
	}
	c.mu.Lock()
	clear(c.stopped)
	c.mu.Unlock()
	return nil
}

// Stop stops every node and returns once all their processes exited.
func (c *Cluster) Stop(ctx context.Context) error {
	if !c.owned {
		return ErrNotOwned
	}
	pids := c.pids()
	if _, err := c.run(ctx, "stop"); err != nil {
		return err
	}
	return waitExit(ctx, pids)
}

// Remove stops the cluster, deletes its files and frees its address prefix.
// Node logs are deleted with it; copy them first with CollectLogs.
func (c *Cluster) Remove(ctx context.Context) error {
	if !c.owned {
		return ErrNotOwned
	}
	pids := c.pids()
	_, err := c.run(ctx, "remove")
	if err == nil {
		err = waitExit(ctx, pids)
	}
	c.close()
	return err
}

func (c *Cluster) close() {
	c.addr.Release()
	c.addr = nil
	c.log.Close()
}

// Status runs ccm status.
func (c *Cluster) Status(ctx context.Context) ([]NodeStatus, error) {
	out, err := c.run(ctx, "status")
	if err != nil {
		return nil, err
	}
	return parseStatus(out), nil
}

// WaitUp waits until every node reports UP and accepts native connections.
func (c *Cluster) WaitUp(ctx context.Context) error {
	for {
		st, err := c.Status(ctx)
		up := err == nil && len(st) == len(c.nodes)
		for _, s := range st {
			up = up && s.State == NodeUp
		}
		if up {
			for _, n := range c.nodes {
				up = up && n.acceptsNative()
			}
		}
		if up {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("ccm: cluster %s not up: %w (last status %v, %v)", c.name, ctx.Err(), st, err)
		case <-time.After(time.Second):
		}
	}
}

// Version is the version the first running node reports. It is read once
// and cached.
func (c *Cluster) Version(ctx context.Context) (ServerVersion, error) {
	c.mu.Lock()
	if c.version != nil {
		defer c.mu.Unlock()
		return *c.version, nil
	}
	c.mu.Unlock()
	var lastErr error
	for _, n := range c.nodes {
		raw, err := n.releaseVersion(ctx)
		if err != nil {
			lastErr = err
			continue
		}
		v, err := ParseServerVersion(c.dist, raw)
		if err != nil {
			return v, err
		}
		c.mu.Lock()
		c.version = &v
		c.mu.Unlock()
		return v, nil
	}
	return ServerVersion{}, fmt.Errorf("ccm: no node reported a version: %w", lastErr)
}

// LogMark records the current size of every node log, so a later
// CollectLogsSince copies only what was written after it.
type LogMark map[string]int64

// MarkLogs returns the current end of every node log.
func (c *Cluster) MarkLogs() LogMark {
	mark := LogMark{}
	for _, n := range c.nodes {
		for _, f := range n.LogFiles() {
			if st, err := os.Stat(f); err == nil {
				mark[f] = st.Size()
			}
		}
	}
	return mark
}

// CollectLogs copies every node log into dst/<node>/.
func (c *Cluster) CollectLogs(dst string) error { return c.CollectLogsSince(nil, dst) }

// CollectLogsSince copies the part of every node log written after mark
// into dst/<node>/.
func (c *Cluster) CollectLogsSince(mark LogMark, dst string) error {
	var errs []error
	for _, n := range c.nodes {
		for _, f := range n.LogFiles() {
			if err := copyFrom(f, mark[f], filepath.Join(dst, n.name, filepath.Base(f))); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// LogTail returns the last lines of every node's main log, for test output.
func (c *Cluster) LogTail(lines int) string {
	var b strings.Builder
	for _, n := range c.nodes {
		data, err := os.ReadFile(n.LogFile())
		if err != nil {
			continue
		}
		all := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
		if len(all) > lines {
			all = all[len(all)-lines:]
		}
		fmt.Fprintf(&b, "==> %s <==\n%s\n", n.LogFile(), strings.Join(all, "\n"))
	}
	return b.String()
}

func (c *Cluster) run(ctx context.Context, args ...string) ([]byte, error) {
	start := time.Now()
	out, err := c.runner.Run(ctx, c.configDir, args...)
	c.log.record(args, time.Since(start), out, err)
	return out, err
}

func (c *Cluster) pids() []int {
	var pids []int
	for _, n := range c.nodes {
		if pid := n.pid(); pid > 0 {
			pids = append(pids, pid)
		}
	}
	return pids
}

func (c *Cluster) markStopped(n *Node, stopped bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if stopped {
		c.stopped[n.name] = true
	} else {
		delete(c.stopped, n.name)
	}
}

// StoppedNodes are the nodes stopped through Node.Stop or Node.Kill and not
// started again.
func (c *Cluster) StoppedNodes() []*Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*Node
	for _, n := range c.nodes {
		if c.stopped[n.name] {
			out = append(out, n)
		}
	}
	return out
}

// RestoreNodes starts every node in StoppedNodes.
func (c *Cluster) RestoreNodes(ctx context.Context) error {
	var errs []error
	for _, n := range c.StoppedNodes() {
		if err := n.Start(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type clusterConf struct {
	ConfigOptions    map[string]any `yaml:"config_options"`
	Name             string         `yaml:"name"`
	IPPrefix         string         `yaml:"ipprefix"`
	ScyllaVersion    string         `yaml:"scylla_version"`
	CassandraVersion string         `yaml:"cassandra_version"`
	InstallDir       string         `yaml:"install_dir"`
	Nodes            []string       `yaml:"nodes"`
	UseVNodes        bool           `yaml:"use_vnodes"`
}

type nodeConf struct {
	Name       string `yaml:"name"`
	Status     string `yaml:"status"`
	Interfaces struct {
		Binary []any `yaml:"binary"`
	} `yaml:"interfaces"`
	PID int `yaml:"pid"`
}

func (c *Cluster) loadTopology() error {
	var conf clusterConf
	if err := readYAML(filepath.Join(c.Dir(), "cluster.conf"), &conf); err != nil {
		return err
	}
	if conf.ScyllaVersion != "" || strings.Contains(conf.InstallDir, "scylla") {
		c.dist = Scylla
		c.ccmArg = conf.ScyllaVersion
	} else {
		c.dist = Cassandra
		c.ccmArg = conf.CassandraVersion
	}
	if c.ipPrefix == "" {
		c.ipPrefix = conf.IPPrefix
	}
	c.config = conf.ConfigOptions
	c.vnodes = conf.UseVNodes
	nodes := make([]*Node, 0, len(conf.Nodes))
	for i, name := range conf.Nodes {
		var nc nodeConf
		if err := readYAML(filepath.Join(c.Dir(), name, "node.conf"), &nc); err != nil {
			return err
		}
		addr, err := binaryAddr(nc.Interfaces.Binary)
		if err != nil {
			return fmt.Errorf("ccm: %s/%s: %w", c.name, name, err)
		}
		nodes = append(nodes, &Node{cluster: c, index: i + 1, name: name, addr: addr})
	}
	c.nodes = nodes
	return nil
}

// relocatePorts moves each node's JMX and remote debug ports, which ccm
// numbers from the node index alone, into a range of the cluster's address
// slot, so Cassandra clusters on different prefixes do not collide on them.
func (c *Cluster) relocatePorts() error {
	slot := 0
	if parts := strings.Split(strings.TrimSuffix(c.ipPrefix, "."), "."); len(parts) == 3 {
		slot, _ = strconv.Atoi(parts[2])
	}
	if slot < firstSlot {
		return nil
	}
	for _, n := range c.nodes {
		path := filepath.Join(n.Dir(), "node.conf")
		var m map[string]any
		if err := readYAML(path, &m); err != nil {
			return err
		}
		m["jmx_port"] = strconv.Itoa(20000 + slot*10 + n.index)
		m["remote_debug_port"] = strconv.Itoa(40000 + slot*10 + n.index)
		data, err := yaml.Marshal(m)
		if err != nil {
			return err
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return err
		}
	}
	return nil
}

var maxHeapRe = regexp.MustCompile(`(?m)^#?MAX_HEAP_SIZE=.*$`)

func (c *Cluster) setCassandraHeap() error {
	for _, n := range c.nodes {
		path := filepath.Join(n.Dir(), "conf", "cassandra-env.sh")
		data, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		data = maxHeapRe.ReplaceAll(data, []byte(fmt.Sprintf(`MAX_HEAP_SIZE="%dM"`, c.spec.MemoryMB)))
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func binaryAddr(itf []any) (netip.AddrPort, error) {
	if len(itf) != 2 {
		return netip.AddrPort{}, fmt.Errorf("binary interface %v", itf)
	}
	ip, err := netip.ParseAddr(fmt.Sprint(itf[0]))
	if err != nil {
		return netip.AddrPort{}, err
	}
	port, err := strconv.ParseUint(fmt.Sprint(itf[1]), 10, 16)
	if err != nil {
		return netip.AddrPort{}, err
	}
	return netip.AddrPortFrom(ip, uint16(port)), nil
}

func readYAML(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("ccm: %w", err)
	}
	if err := yaml.Unmarshal(data, v); err != nil {
		return fmt.Errorf("ccm: %s: %w", path, err)
	}
	return nil
}

func waitExit(ctx context.Context, pids []int) error {
	for _, pid := range pids {
		for processAlive(pid) {
			select {
			case <-ctx.Done():
				return fmt.Errorf("ccm: process %d did not exit: %w", pid, ctx.Err())
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	return nil
}

func copyFrom(src string, offset int64, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer in.Close()
	if st, err := in.Stat(); err == nil && st.Size() < offset {
		offset = 0 // rotated or truncated
	}
	if _, err := in.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o777); err != nil {
		return err
	}
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// scyllaAPIVersion asks a ScyllaDB node's REST API for its release.
func scyllaAPIVersion(ctx context.Context, ip string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(ip, "10000")+"/storage_service/scylla_release_version", nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("scylla API: %s: %s", resp.Status, body)
	}
	var v string
	if err := json.Unmarshal(body, &v); err != nil {
		return "", fmt.Errorf("scylla API: %q: %w", body, err)
	}
	return v, nil
}

type commandLog struct {
	w  io.Writer
	f  *os.File
	mu sync.Mutex
}

func openCommandLog(dir string, w io.Writer) (*commandLog, error) {
	if w != nil {
		return &commandLog{w: w}, nil
	}
	f, err := os.OpenFile(filepath.Join(dir, "ccm.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("ccm: command log: %w", err)
	}
	return &commandLog{w: f, f: f}, nil
}

func (l *commandLog) record(args []string, took time.Duration, out []byte, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.w == nil {
		return
	}
	status := "ok"
	if err != nil {
		status = err.Error()
	}
	fmt.Fprintf(l.w, "%s ccm %s (%s): %s\n", time.Now().Format(time.RFC3339Nano), strings.Join(args, " "), took.Round(time.Millisecond), status)
	if len(out) > 0 {
		fmt.Fprintf(l.w, "%s\n", strings.TrimRight(string(out), "\n"))
	}
}

// Note writes a line into the cluster's command log, e.g. which test holds
// the cluster.
func (c *Cluster) Note(format string, args ...any) {
	c.log.mu.Lock()
	defer c.log.mu.Unlock()
	if c.log.w != nil {
		fmt.Fprintf(c.log.w, "%s # %s\n", time.Now().Format(time.RFC3339Nano), fmt.Sprintf(format, args...))
	}
}

func (l *commandLog) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f != nil {
		l.f.Close()
	}
	l.w, l.f = nil, nil
}
