//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/exec"
	implSession "github.com/scylladb/gocql/v2/internal/session"
)

// TestStrongConsistency checks leader-aware routing for ScyllaDB strongly
// consistent tables with the session's default routing policy: writes and
// strongly consistent reads of a keyspace created with consistency =
// 'global' go to the tablet leader that TABLETS_ROUTING_V2 feedback
// reports, while reads at ONE and requests to an eventually consistent
// keyspace keep spreading over the replicas.
func TestStrongConsistency(t *testing.T) {
	cluster := strongConsistencyCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s, err := gocql.Connect(ctx, mustBuild(t, newFixtureConfig(cluster.IPs(), 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !(*implSession.Session)(s).Core().GetConn().InternalFramers.InternalDefaults.InternalTabletsRoutingV2 {
		t.Fatal("server with strongly-consistent-tables did not negotiate TABLETS_ROUTING_V2_EXPERIMENTAL")
	}
	suffix := fmt.Sprintf("%x", time.Now().UnixNano())
	global := testKeyspaceName(t, "global_"+suffix)
	eventual := testKeyspaceName(t, "eventual_"+suffix)
	for ks, option := range map[string]string{global: " AND consistency = 'global'", eventual: ""} {
		if err := v2CreateTable(ctx, s, "CREATE KEYSPACE "+ks+" WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'enabled': true}"+option); err != nil {
			t.Fatal(err)
		}
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := v2CreateTable(cleanup, s, "DROP KEYSPACE "+ks); err != nil {
				t.Error(err)
			}
		}()
		if err := v2CreateTable(ctx, s, "CREATE TABLE "+ks+".records(pk int PRIMARY KEY, v int)"); err != nil {
			t.Fatal(err)
		}
	}
	for ks, want := range map[string]string{global: "global", eventual: ""} {
		meta, ok := s.Metadata().Keyspace(ks)
		if !ok || !meta.TabletBased || meta.ConsistencyMode != want {
			t.Fatalf("keyspace %s: %+v; want a tablet keyspace with ConsistencyMode %q", ks, meta, want)
		}
	}

	type statement struct {
		write func(ks string, c cql.Consistency) (cql.Result, error)
		read  func(ks string, c cql.Consistency) (cql.Result, error)
	}
	consistency := func(c cql.Consistency) exec.CompiledOptions {
		return bindOptions(s, exec.Options{Consistency: cql.Option[cql.Consistency]{Value: c, Set: true}})
	}
	prepared := map[string]statement{}
	for _, ks := range []string{global, eventual} {
		insert := codecTestMust(s.Prepare(ctx, cql.Statement[int32, cql.EmptyRow]{CQL: "INSERT INTO " + ks + ".records(pk, v) VALUES(?, 7)", Params: cql.OneParam(cql.Int32Codec), Rows: cql.EmptyRowDecoder}, exec.PrepareOptions{}))
		selectV := codecTestMust(s.Prepare(ctx, cql.Statement[int32, cql.DynamicRow]{CQL: "SELECT v FROM " + ks + ".records WHERE pk = ?", Params: cql.OneParam(cql.Int32Codec), Rows: cql.DynamicRows}, exec.PrepareOptions{}))
		prepared[ks] = statement{
			write: func(_ string, c cql.Consistency) (cql.Result, error) { return insert.Exec(ctx, 1, consistency(c)) },
			read: func(_ string, c cql.Consistency) (cql.Result, error) {
				rows, err := selectV.Run(ctx, 1, consistency(c))
				if err != nil {
					return cql.Result{}, err
				}
				var row cql.DynamicRow
				if !rows.NextInto(&row) {
					return cql.Result{}, fmt.Errorf("no row: %w", rows.Err())
				}
				if v2SchemaRoutingInt(t, row, "v") != 7 {
					t.Fatal("bad row", row)
				}
				for rows.NextInto(&row) {
				}
				result := rows.Info()
				return result, rows.Close()
			},
		}
	}

	// feedback returns the leader a response's TABLETS_ROUTING_V2 feedback
	// reports, its first replica, and waits until the session routes by it.
	feedback := func(ks string, r cql.Result) (cql.UUID, bool) {
		for _, entry := range r.CustomPayload() {
			if entry.Key == "tablets-routing-v1" {
				t.Fatal("V1 feedback on a V2 connection")
			}
			if entry.Key != "tablets-routing-v2" || !entry.Value.Valid {
				continue
			}
			hint, err := parseTypedTabletPayload(entry.Value.Value, ks, "records", true)
			if err != nil {
				t.Fatal("tablet feedback", err)
			}
			if _, ok := hint.Version(); !ok || len(hint.Replicas()) != 3 {
				t.Fatalf("tablet feedback %v without a version or three replicas", hint)
			}
			leader := cql.UUID(hint.Replicas()[0].HostUUIDValue())
			// Feedback is published in the background: wait for it, so that
			// a later request with a coincidentally matching random version
			// block is not mistaken for one routed by it.
			for deadline := time.Now().Add(10 * time.Second); ; {
				replicas, err := s.Metadata().Replicas(ks, "records", hint.LastToken())
				if err == nil && len(replicas) == 3 && replicas[0].HostID == leader {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("feedback for leader %s not published: %v, %v", leader, replicas, err)
				}
				time.Sleep(10 * time.Millisecond)
			}
			return leader, true
		}
		return cql.UUID{}, false
	}

	// leaders holds the leader the last feedback for each keyspace reported.
	leaders := map[string]cql.UUID{}
	// toLeader runs op until 16 responses in a row carry no feedback, each of
	// which must come from the leader the last feedback reported.
	toLeader := func(name, ks string, op func(string, cql.Consistency) (cql.Result, error), c cql.Consistency) {
		t.Helper()
		stable, learned := 0, 0
		for attempt := 0; attempt < 256 && stable < 16; attempt++ {
			r, err := op(ks, c)
			if err != nil {
				t.Fatal(name, c, err)
			}
			if l, ok := feedback(ks, r); ok {
				leaders[ks], stable = l, 0
				learned++
				continue
			}
			leader := leaders[ks]
			if leader.IsEmpty() {
				continue // the random first version block matched
			}
			if r.HostID != leader {
				t.Fatalf("%s at %v: coordinator %s, want leader %s", name, c, r.HostID, leader)
			}
			stable++
		}
		if stable < 16 {
			t.Fatalf("%s at %v: leader routing did not settle: %d feedback responses", name, c, learned)
		}
	}
	// spread runs op 64 times after the tablet is known and checks that more
	// than one replica coordinated it.
	spread := func(name, ks string, op func(string, cql.Consistency) (cql.Result, error), c cql.Consistency) {
		t.Helper()
		hosts := map[cql.UUID]bool{}
		for range 64 {
			r, err := op(ks, c)
			if err != nil {
				t.Fatal(name, c, err)
			}
			if _, ok := feedback(ks, r); ok {
				continue
			}
			hosts[r.HostID] = true
		}
		if len(hosts) < 2 {
			t.Fatalf("%s at %v: every request went to %v", name, c, hosts)
		}
	}

	// The first write also creates the row the reads return.
	toLeader("global write", global, prepared[global].write, cql.Quorum)
	toLeader("global read", global, prepared[global].read, cql.LocalQuorum)
	toLeader("global read", global, prepared[global].read, cql.Quorum)
	spread("global read", global, prepared[global].read, cql.One)
	spread("global read", global, prepared[global].read, cql.LocalOne)
	if _, err := prepared[eventual].write(eventual, cql.Quorum); err != nil {
		t.Fatal(err)
	}
	spread("eventual write", eventual, prepared[eventual].write, cql.Quorum)
	spread("eventual read", eventual, prepared[eventual].read, cql.Quorum)
}
