//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package integration

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/testkit/ccm"
)

// The suite runs every test against one cluster: the one given with
// -cluster, else the cluster make scylla-start or make cassandra-start
// created (found through ccm), else, with -ccm.provision, one the suite
// creates through ccm and removes at the end. Tests that stop nodes cannot
// use that shared cluster; nodeControlCluster gives them a second one,
// created once and reused by each of them in turn.
var (
	flagCCMProvision = flag.Bool("ccm.provision", false, "create the suite's cluster through ccm when -cluster is not given and no make-created cluster is running")
	flagCCMVersion   = flag.String("ccm.version", "", "version for clusters the suite creates through ccm; default: the make-created cluster's version")
	flagCCMBudget    = flag.String("ccm.budget", "", "resources the clusters the suite creates may take at once, e.g. cpus=8,memory=16G; default: $GOCQL_CCM_BUDGET, else all CPUs and 3/4 of memory")
)

var (
	ccmPool *ccm.Pool
	// ccmTemplate is the spec clusters the suite creates start from: the
	// shared cluster's version and configuration.
	ccmTemplate    ccm.ClusterSpec
	ccmTemplateErr error
)

// setupCCM points the suite at its cluster and returns the teardown that
// removes every cluster the suite created.
func setupCCM() (teardown func()) {
	teardown = func() {}
	if err := ccm.Available(); err != nil {
		ccmTemplateErr = fmt.Errorf("ccm not installed: %w", err)
		return teardown
	}
	dist, err := ccm.ParseDistribution(*flagDistribution)
	if err != nil {
		ccmTemplateErr = err
		return teardown
	}
	var budget ccm.Resources
	switch {
	case *flagCCMBudget != "":
		if budget, err = ccm.ParseResources(*flagCCMBudget); err != nil {
			fmt.Fprintln(os.Stderr, "integration:", err)
			os.Exit(2)
		}
	case os.Getenv("GOCQL_CCM_BUDGET") == "":
		budget = ccm.HostBudget()
	}
	ccmPool = ccm.NewPool(ccm.PoolConfig{Budget: budget})
	teardown = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		if err := ccmPool.Close(ctx); err != nil {
			fmt.Fprintln(os.Stderr, "ccm teardown:", err)
		}
	}
	clusterGiven := flagSet("cluster")

	if shared, err := ccm.AttachShared(dist); err == nil {
		closePool := teardown
		teardown = func() { closePool(); shared.Close() }
		ccmTemplate = suiteSpec(shared.Spec())
		if *flagCCMVersion != "" {
			ccmTemplate.Version, ccmTemplateErr = ccm.ParseVersion(dist, *flagCCMVersion)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if !clusterGiven && shared.WaitUp(ctx) == nil {
			useSuiteCluster(shared)
			fmt.Fprintf(os.Stderr, "integration: using ccm cluster %s (%s, version %v)\n", shared.Name(), *flagCluster, flagCassVersion)
			return teardown
		}
		if clusterGiven {
			return teardown
		}
	} else if *flagCCMVersion != "" {
		ccmTemplate = suiteSpec(defaultSuiteSpec(dist))
		ccmTemplate.Version, ccmTemplateErr = ccm.ParseVersion(dist, *flagCCMVersion)
	} else {
		ccmTemplateErr = fmt.Errorf("no ccm cluster to copy and no -ccm.version: %w", err)
	}

	if clusterGiven {
		return teardown
	}
	if !*flagCCMProvision {
		if name := os.Getenv("GOCQL_CCM_CLUSTER"); name != "" {
			// make names the cluster it started; running every test against
			// the 127.0.0.1 default instead would only bury this.
			fmt.Fprintf(os.Stderr, "integration: ccm cluster %s is missing or not up in %s\n", name, ccm.DefaultConfigDir())
			os.Exit(2)
		}
		return teardown
	}
	if ccmTemplateErr != nil {
		fmt.Fprintln(os.Stderr, "integration: cannot provision a cluster:", ccmTemplateErr)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	// A shared lease held for the whole run; the pool removes the cluster
	// at teardown.
	lease, err := ccmPool.Acquire(ctx, nil, ccm.Request{Spec: ccmTemplate, Pinned: true})
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration: provision cluster:", err)
		os.Exit(2)
	}
	useSuiteCluster(lease.Cluster())
	fmt.Fprintf(os.Stderr, "integration: created ccm cluster %s (%s, version %v)\n", lease.Cluster().Name(), *flagCluster, flagCassVersion)
	closePool := teardown
	return func() {
		lease.Release(false)
		closePool()
	}
}

func useSuiteCluster(c *ccm.Cluster) {
	*flagCluster = strings.Join(c.IPs(), ",")
	if !flagSet("clusterSize") {
		*clusterSize = len(c.Nodes())
	}
	if !flagSet("gocql.cversion") {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		v, err := c.Version(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, "integration: read server version:", err)
			return
		}
		if err := flagCassVersion.Set(fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)); err != nil {
			fmt.Fprintln(os.Stderr, "integration: server version:", err)
		}
	}
}

func flagSet(name string) bool {
	set := false
	flag.Visit(func(f *flag.Flag) { set = set || f.Name == name })
	return set
}

// suiteSpec adapts a cluster's spec for clusters the suite creates: no
// password authentication, which only Cassandra's superuser setup makes
// usable and the driver does not need when the server does not ask.
func suiteSpec(spec ccm.ClusterSpec) ccm.ClusterSpec {
	config := spec.Config[:0:0]
	for _, c := range spec.Config {
		if !strings.HasPrefix(c, "authenticator:") {
			config = append(config, c)
		}
	}
	spec.Config = config
	return spec
}

// defaultSuiteSpec mirrors the cluster make scylla-start and make
// cassandra-start create, for -ccm.provision without one to copy.
func defaultSuiteSpec(d ccm.Distribution) ccm.ClusterSpec {
	pki, _ := filepath.Abs("../../testdata/pki")
	if d == ccm.Cassandra {
		return ccm.ClusterSpec{Version: ccm.VersionSpec{Distribution: d}, Nodes: []int{3}, VNodes: true, Args: []string{"-XX:NewSize=100m"}, Config: []string{
			"client_encryption_options.enabled: true",
			"client_encryption_options.keystore: " + pki + "/.keystore",
			"client_encryption_options.keystore_password: cassandra",
			"client_encryption_options.require_client_auth: true",
			"client_encryption_options.truststore: " + pki + "/.truststore",
			"client_encryption_options.truststore_password: cassandra",
			"concurrent_reads: 2",
			"concurrent_writes: 2",
			"write_request_timeout_in_ms: 5000",
			"read_request_timeout_in_ms: 5000",
			"user_defined_functions_enabled: true",
			"materialized_views_enabled: true",
		}}
	}
	return ccm.ClusterSpec{Version: ccm.VersionSpec{Distribution: d}, Nodes: []int{3}, SMP: 2, Args: []string{"--experimental-features udf --enable-user-defined-functions true"}, Config: []string{
		"native_transport_port_ssl: 9142",
		"native_transport_port: 9042",
		"native_shard_aware_transport_port: 19042",
		"native_shard_aware_transport_port_ssl: 19142",
		"client_encryption_options.enabled: true",
		"client_encryption_options.certificate: " + pki + "/cassandra.crt",
		"client_encryption_options.keyfile: " + pki + "/cassandra.key",
		"client_encryption_options.truststore: " + pki + "/ca.crt",
		"client_encryption_options.require_client_auth: true",
		"maintenance_socket: workdir",
		"enable_tablets: true",
		"enable_user_defined_functions: true",
		"experimental_features: [udf]",
	}}
}

// nodeControlCluster returns a cluster t holds alone and may stop and start
// nodes of. It has the suite cluster's version and configuration and the
// given node count. Every test asking for the same node count reuses one
// cluster: the pool restarts any node a test left stopped before handing
// the cluster to the next test. Tests are skipped when ccm is unavailable.
func nodeControlCluster(t *testing.T, nodes int) *ccm.Cluster {
	t.Helper()
	if ccmPool == nil || ccmTemplateErr != nil {
		t.Skip("node control needs ccm:", ccmTemplateErr)
	}
	spec := ccmTemplate
	spec.Nodes = []int{nodes}
	// Keep node-control clusters apart from a provisioned suite cluster
	// with the same shape, whose shared lease lasts the whole run.
	spec.Tag = "node-control"
	return ccmPool.MustAcquire(t, ccm.Request{Spec: spec, Exclusive: true})
}
