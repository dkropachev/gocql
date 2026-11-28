//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/execopt"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implSession "github.com/scylladb/gocql/v2/internal/session"
)

// TestRunStreamCoordinatorStop stops the coordinator of an incremental
// stream after one delivered row and before the next page request: the
// stream must fail terminally without replaying rows, and once the node is
// back, queries pinned to it must succeed again on the same session.
func TestRunStreamCoordinatorStop(t *testing.T) {
	cluster := nodeControlCluster(t, 1)
	node := cluster.Node(1)
	cfg := newFixtureConfig(cluster.IPs(), fixtureCompression, func(c *config.ConfigBuilder) {
		c.WithPool(config.PerHost{Connections: 1}).WithHeartbeat(0, 0).WithMetadataRefresh(0).
			// Streaming rejects frame compression before protocol v5, and the
			// suite's default compressor would skip this test on every v4 lane.
			// Compression is not what it tests.
			WithCompression(cql.NoCompression)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s, err := gocql.Connect(ctx, mustBuild(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	runDDL := func(stmt string) {
		t.Helper()
		if _, err := s.UnpreparedExec(ctx, cql.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: stmt, Params: cql.NoParams}, cql.EmptyParams{}); err != nil {
			t.Fatal(stmt, err)
		}
	}
	keyspace := testKeyspaceName(t, fmt.Sprintf("%x", time.Now().UnixNano()))
	runDDL("CREATE KEYSPACE " + keyspace + " WITH replication={'class':'NetworkTopologyStrategy','replication_factor':1}")
	// The cluster is reused by the next node-control test.
	defer runDDL("DROP KEYSPACE " + keyspace)
	runDDL("CREATE TABLE " + keyspace + ".records(id int PRIMARY KEY,value blob)")
	for i := range 5 {
		runDDL(fmt.Sprintf("INSERT INTO %s.records(id,value)VALUES(%d,0x%02x)", keyspace, i, i))
	}

	var target cql.UUID
	for _, host := range (*implSession.Session)(s).Data().InternalHostSource.InternalGetHostsList() {
		if host.ConnectAddress().String() == node.IP() {
			target = implEndpoint.InternalHostInfoHostUUID(host)
		}
	}
	if target.IsZero() {
		t.Fatal("no host for", node.IP())
	}
	pinned := execopt.Options{HostID: cql.Option[cql.UUID]{Set: true, Value: target}}

	statement := cql.Statement[cql.EmptyParams, cql.DynamicRow]{CQL: "SELECT id,value FROM " + keyspace + ".records", Params: cql.NoParams, Rows: cql.DynamicRows}
	prepared := codecTestMust(s.Prepare(ctx, statement, execopt.PrepareOptions{}))
	type output struct {
		ID    int32
		Value []byte
	}
	decoder := cql.StreamRowInto(func(*output) {}, cql.StreamColumn("id", cql.StreamInt32, func(r *output) *int32 { return &r.ID }), cql.StreamColumn("value", cql.StreamByteSlice, func(r *output) *[]byte { return &r.Value }))
	streamOptions := pinned
	streamOptions.PageSize = 1
	streamPrepared := codecTestMust(s.PrepareStream(ctx, cql.StreamStatement[cql.EmptyParams, output]{CQL: statement.CQL, Params: cql.NoParams, Rows: decoder}))
	interrupted, err := pagingRunStream(ctx, t, s, streamPrepared, cql.EmptyParams{}, true, streamOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer interrupted.Close()
	dst := output{Value: make([]byte, 0, 16)}
	if !interrupted.NextInto(&dst) {
		t.Fatal("first row", interrupted.Err())
	}

	if err := node.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	// The node control pool restarts the node if an assertion below fails.
	if interrupted.NextInto(&dst) || interrupted.Err() == nil {
		t.Fatal("coordinator stop was not terminal", interrupted.Err())
	}
	terminal := interrupted.Err()
	if result, progress := interrupted.Result(), interrupted.Progress(); result.FetchedRows != 1 || !progress.OutputStarted {
		t.Fatal("interruption replayed output", result, progress)
	}
	if interrupted.NextInto(&dst) || !errors.Is(interrupted.Err(), terminal) {
		t.Fatal("terminal interruption changed", interrupted.Err())
	}
	if err := interrupted.Close(); !errors.Is(err, terminal) {
		t.Fatal("close lost interruption", err)
	}

	if err := node.Start(ctx); err != nil {
		t.Fatal(err)
	}
	probeOptions := pinned
	probeOptions.Prefetch = cql.Some(false)
	deadline := time.Now().Add(40 * time.Second)
	for {
		probe, probeErr := prepared.Run(ctx, cql.EmptyParams{}, bindOptions(s, probeOptions))
		if probeErr == nil {
			n := 0
			var row cql.DynamicRow
			for probe.NextInto(&row) {
				n++
			}
			probeErr = probe.Close()
			if probeErr == nil && n == 5 {
				break
			}
			if probeErr == nil {
				probeErr = fmt.Errorf("%d rows", n)
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("coordinator pool did not recover", probeErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The deferred DROP KEYSPACE refreshes metadata on the control
	// connection, which returns on its own schedule, after the pool may have.
	for deadline := time.Now().Add(40 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		_, err := s.RefreshMetadata(ctx)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("control connection did not recover", err)
		}
	}
	t.Logf("coordinator stop after one delivered row: %v; restart and pinned probe succeeded", terminal)
}
