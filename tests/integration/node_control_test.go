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

	implSession "github.com/scylladb/gocql/v2/internal/session"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
)

// TestRunStreamCoordinatorStop stops the coordinator of an incremental
// stream after one delivered row and before the next page request: the
// stream must fail terminally without replaying rows, and once the node is
// back, queries pinned to it must succeed again on the same session.
func TestRunStreamCoordinatorStop(t *testing.T) {
	cluster := nodeControlCluster(t, 1)
	node := cluster.Node(1)
	config := newFixtureConfig(cluster.IPs(), fixtureCompression, func(c *gocql.ConfigBuilder) {
		c.WithPoolSize(gocql.PerHost(1)).NoHeartbeat().NoMetadataRefresh().
			// Streaming rejects frame compression before protocol v5, and the
			// suite's default compressor would skip this test on every v4 lane.
			// Compression is not what it tests.
			WithCompression(gocql.NoCompression)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s, err := gocql.Connect[testDialect](ctx, mustBuild(t, config))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	exec := func(stmt string) {
		t.Helper()
		b := codecTestMust(BindUnpreparedOwned(cql.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: stmt, Kind: cql.OrdinaryStatement, Params: cql.EmptyParamsEncoder, Rows: cql.EmptyRowDecoder}, cql.NewColumns(), cql.EmptyParams{}))
		defer b.Close()
		if _, err := Exec(ctx, s, b, gocql.CompiledOptions{}); err != nil {
			t.Fatal(stmt, err)
		}
	}
	keyspace := testKeyspaceName(t, fmt.Sprintf("%x", time.Now().UnixNano()))
	exec("CREATE KEYSPACE " + keyspace + " WITH replication={'class':'NetworkTopologyStrategy','replication_factor':1}")
	// The cluster is reused by the next node-control test.
	defer exec("DROP KEYSPACE " + keyspace)
	exec("CREATE TABLE " + keyspace + ".records(id int PRIMARY KEY,value blob)")
	for i := range 5 {
		exec(fmt.Sprintf("INSERT INTO %s.records(id,value)VALUES(%d,0x%02x)", keyspace, i, i))
	}

	var target cql.UUID
	for _, host := range (*implSession.Session)(s).Data().InternalHostSource.InternalGetHostsList() {
		if host.ConnectAddress().String() == node.IP() {
			target = implEndpoint.InternalHostInfoHostUUID(host)
		}
	}
	if target.IsEmpty() {
		t.Fatal("no host for", node.IP())
	}
	pinned := gocql.Options{HostID: cql.Option[cql.UUID]{Set: true, Value: target}}

	statement := cql.Statement[cql.EmptyParams, cql.DynamicRow]{CQL: "SELECT id,value FROM " + keyspace + ".records", Kind: cql.OrdinaryStatement, Params: cql.EmptyParamsEncoder, Rows: cql.DynamicRows}
	prepared := codecTestMust(s.Prepare(ctx, statement, gocql.PrepareOptions{}))
	bound := codecTestMust(BindOwned(prepared, cql.EmptyParams{}))
	defer bound.Close()
	type output struct {
		ID    int32
		Value []byte
	}
	decoder := cql.StreamRow(func(*output) {}, cql.StreamColumn("id", cql.StreamInt32, func(r *output) *int32 { return &r.ID }), cql.StreamColumn("value", cql.StreamBlob, func(r *output) *[]byte { return &r.Value }))
	streamOptions := pinned
	streamOptions.PageSize = cql.Option[int32]{Set: true, Value: 1}
	interrupted, err := RunStream(ctx, s, bound, decoder, gocql.StreamOptions{Incremental: true, Options: streamOptions})
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
	if info := interrupted.Info(); info.DeliveredRows != 1 || !info.OutputStarted {
		t.Fatal("interruption replayed output", info)
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
	probeOptions.Prefetch = cql.Option[int]{Set: true, Value: 0}
	deadline := time.Now().Add(40 * time.Second)
	for {
		probe, probeErr := Run(ctx, s, bound, bindOptions(s, probeOptions))
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
	t.Logf("coordinator stop after one delivered row: %v; restart and pinned probe succeeded", terminal)
}
