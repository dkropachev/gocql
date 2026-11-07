// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package ccm provisions and controls ScyllaDB and Cassandra test clusters
// through the ccm command line tool, which must already be installed.
//
// Every ccm command runs with its own --config-dir, so the package never
// touches ccm's "current cluster" state and several clusters can live at
// once, in one test process or across the processes go test starts per
// package. Clusters the package creates get their own 127.0.N. address
// prefix, claimed through a lock file so concurrent processes never share
// one.
//
// A Cluster is either owned (created by this package) or attached (an
// existing cluster such as the one make scylla-start creates). Only owned
// clusters accept node control: stopping a node of an attached cluster
// returns ErrNotOwned, so a test can never take down a node other tests use.
//
// Tests normally get clusters from a Pool. A Pool reuses one cluster for
// every request with the same ClusterSpec: shared leases run concurrently,
// exclusive leases (for tests that stop nodes) run one at a time, and on
// release the pool restarts any node the test left stopped before handing
// the cluster to the next test. A Pool can be given a CPU and memory budget:
// a request for a new cluster that does not fit waits in line while the
// pool removes idle clusters to make room. New provisions a cluster for one
// test only.
//
// Logs stay traceable to tests: each cluster has an artifacts directory with
// ccm.log (every ccm command, its duration and output), and a failed lease
// copies the part of every node log written during that lease into
// <artifacts>/<cluster>/failed/<test>/.
package ccm
