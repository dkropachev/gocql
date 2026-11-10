//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package integration

import (
	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	routing "github.com/scylladb/gocql/v2/routing"

	"context"
	"math"
	"slices"
	"testing"
)

// tabletAuthority brackets routed queries with independent server reads.
// During a transition or disagreement it supplies no correctness credit.
type tabletObservation struct {
	last      int64
	replicas  []routing.Target
	readNodes []cql.UUID
	stable    bool
}

func tabletAuthority(t *testing.T, ctx context.Context, s *gocql.Session, table cql.UUID, token int64) tabletObservation {
	t.Helper()
	type parameters struct {
		Table cql.UUID
		Token int64
	}
	statement := cql.Statement[parameters, cql.DynamicRow]{
		CQL:    "SELECT last_token,replicas,new_replicas,stage FROM system.tablets WHERE table_id=? AND last_token>=? LIMIT 1",
		Params: cql.Params(cql.Param("id", cql.UUIDCodec, func(p parameters) cql.UUID { return p.Table }), cql.Param("token", cql.Int64Codec, func(p parameters) int64 { return p.Token })),
		Rows:   cql.DynamicRows,
	}
	columns := cql.NewColumns(cql.ColumnSpec{Name: "id", Type: cql.UUIDType}, cql.ColumnSpec{Name: "token", Type: cql.BigIntType})
	bound := codecTestMust(BindUnpreparedOwned(statement, columns, parameters{table, token}))
	defer bound.Close()
	observation := tabletObservation{stable: true}
	for index, host := range s.Metadata().Hosts() {
		page := codecTestMust(RunUnpaged(ctx, s, bound, bindOptions(s, gocql.Options{HostID: cql.Option[cql.UUID]{Set: true, Value: host.ID}})))
		if page.Info().HostID != host.ID {
			t.Fatal("authoritative read lost exact host pin", page.Info().HostID, host.ID)
		}
		observation.readNodes = append(observation.readNodes, page.Info().HostID)
		var row cql.DynamicRow
		if !page.NextInto(&row) {
			t.Fatal("authoritative tablet absent", page.Err())
		}
		last := codecTestMust(codecTestMust(row.Get("last_token")).Signed())
		iterator := codecTestMust(codecTestMust(row.Get("replicas")).Elements())
		var replicas []routing.Target
		for iterator.Next() {
			tuple := codecTestMust(iterator.Value().Elements())
			if !tuple.Next() {
				t.Fatal("authoritative replica missing host", tuple.Err())
			}
			id := codecTestMust(tuple.Value().UUID())
			if !tuple.Next() {
				t.Fatal("authoritative replica missing shard", tuple.Err())
			}
			shard := codecTestMust(tuple.Value().Signed())
			if shard < 0 || shard > math.MaxUint32 {
				t.Fatal("invalid authoritative shard", shard)
			}
			if tuple.Next() || tuple.Err() != nil {
				t.Fatal("authoritative replica extra member", tuple.Err())
			}
			replicas = append(replicas, routing.Target{HostID: id, ShardID: cql.Option[uint32]{Set: true, Value: uint32(shard)}})
		}
		if iterator.Err() != nil {
			t.Fatal(iterator.Err())
		}
		if codecTestMust(row.Get("new_replicas")).State() != cql.Null || codecTestMust(row.Get("stage")).State() != cql.Null {
			observation.stable = false
		}
		if page.NextInto(&row) || page.Err() != nil {
			t.Fatal("authoritative query count", page.Err())
		}
		if err := page.Close(); err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			observation.last, observation.replicas = last, replicas
		} else if last != observation.last || !sameReplicaSet(replicas, observation.replicas) {
			observation.stable = false
		}
	}
	if len(observation.readNodes) == 0 {
		t.Fatal("no authoritative read nodes")
	}
	bound.Close()
	if err := bound.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	return observation
}

// Placement is a set of distinct host/shard destinations. Duplicate entries
// cannot satisfy an exact authoritative-set assertion in either direction.
func sameReplicaSet(a, b []routing.Target) bool {
	if len(a) != len(b) {
		return false
	}
	for i, target := range a {
		if slices.Contains(a[:i], target) || !slices.Contains(b, target) {
			return false
		}
	}
	for i, target := range b {
		if slices.Contains(b[:i], target) {
			return false
		}
	}
	return true
}
