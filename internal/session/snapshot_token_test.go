// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/routing"
	metadata2 "github.com/scylladb/gocql/v2/metadata"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

type snapshotTokenKey struct {
	A int32
	B string
}

// TestSnapshotTokenMatchesPreparedRouting checks that Snapshot.Token agrees
// with the token a prepared statement computes from the same values.
func TestSnapshotTokenMatchesPreparedRouting(t *testing.T) {
	s := testSession(t)
	const partitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
	p := s.Peripheral()
	p.InternalMu.Lock()
	config := p.InternalConfig
	p.InternalMu.Unlock()
	config.Partitioners = map[string]routing2.TokenPartitioner{partitioner: routing2.Murmur3TokenPartitioner}
	s.ConfigureV2Peripheral(config)

	intType := codecTestMust(cql.ScalarType(cql.KindInt))
	textType := codecTestMust(cql.ScalarType(cql.KindText))
	next := routing.InternalClonePlacementSnapshot(s.Metadata())
	ks := metadata.InternalCloneV2Keyspace(routing.SnapshotData(s.Metadata()).InternalKeyspaces["ks"])
	column := func(name, typ string) *metadata2.Column {
		return &metadata2.Column{Keyspace: "ks", Name: name, Type: typ, Kind: metadata2.ColumnKindPartitionKey}
	}
	ks.Tables["single"] = &metadata2.Table{Keyspace: "ks", Name: "single", PartitionKey: []*metadata2.Column{column("a", "int")}}
	ks.Tables["composite"] = &metadata2.Table{Keyspace: "ks", Name: "composite", PartitionKey: []*metadata2.Column{column("a", "int"), column("b", "text")}}
	routing.SnapshotData(next).InternalKeyspaces = map[string]*metadata2.Keyspace{"ks": ks}
	routing.SnapshotData(next).InternalPartitioner = partitioner
	routing.SnapshotData(next).InternalPartitioners = config.Partitioners
	p.InternalSnapshot.Store(next)

	prepare := func(table string, columns cql.Columns, indexes []int) *Prepared[snapshotTokenKey, cql.EmptyRow] {
		// Explicit parameters, not ParamsOf: reflection would keep every
		// method alive and grow the test binary the benchmarks run in.
		encoder := cql.Params(
			cql.ParamAt(0, cql.Int32Codec, func(k snapshotTokenKey) int32 { return k.A }),
			cql.ParamAt(1, cql.StringCodec, func(k snapshotTokenKey) string { return k.B }),
		)
		params := codecTestMust(encoder.Compile(columns))
		state := &InternalPreparedState{InternalSession: s, InternalProtocol: 4, InternalParameters: columns, partitioner: partitioner, InternalInfo: PreparedInfo{Keyspace: "ks", Table: table, PartitionKeyIndexes: indexes}}
		prepared := new(Prepared[snapshotTokenKey, cql.EmptyRow])
		PreparedData(prepared).InternalState = state
		PreparedData(prepared).params = params
		PreparedData(prepared).rows = cql.Row[cql.EmptyRow]()
		PreparedData(prepared).plans = &InternalResultPlanCache[cql.EmptyRow]{}
		return prepared
	}
	columns := cql.NewColumns(
		cql.ColumnSpec{Name: "a", Type: intType, Keyspace: "ks"},
		cql.ColumnSpec{Name: "b", Type: textType, Keyspace: "ks"},
	)
	key := snapshotTokenKey{A: 42, B: "partition"}
	text := cql.TextValue(key.B)
	for _, tc := range []struct {
		table   string
		indexes []int
		values  []cql.Value
	}{
		{"single", []int{0}, []cql.Value{cql.IntValue(key.A)}},
		{"composite", []int{0, 1}, []cql.Value{cql.IntValue(key.A), text}},
	} {
		t.Run(tc.table, func(t *testing.T) {
			b := codecTestMust(BindBorrowed(prepare(tc.table, columns, tc.indexes), key))
			defer b.Close()
			want := b.Routing().Token
			if !want.Set {
				t.Fatal("prepared routing has no token")
			}
			got, err := s.Metadata().Token("ks", tc.table, tc.values...)
			if err != nil {
				t.Fatal(err)
			}
			if got != want.Value {
				t.Fatalf("Snapshot.Token = %d, prepared routing token = %d", got, want.Value)
			}
		})
	}
}
