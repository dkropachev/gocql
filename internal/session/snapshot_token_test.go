// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/routing"
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
	config.Partitioners = map[string]routing.TokenPartitioner{partitioner: routing.Murmur3TokenPartitioner}
	s.ConfigureV2Peripheral(config)

	intType := codecTestMust(cqltype.ScalarType(cqltype.TypeInt))
	textType := codecTestMust(cqltype.ScalarType(cqltype.TypeText))
	next := routing.InternalClonePlacementSnapshot(s.Metadata())
	ks := metadata.InternalCloneV2Keyspace(routing.SnapshotData(s.Metadata()).InternalKeyspaces["ks"])
	column := func(name, typ string) *metadata.ColumnMetadata {
		return &metadata.ColumnMetadata{Keyspace: "ks", Name: name, Type: typ, Kind: metadata.ColumnPartitionKey}
	}
	ks.Tables["single"] = &metadata.TableMetadata{Keyspace: "ks", Name: "single", PartitionKey: []*metadata.ColumnMetadata{column("a", "int")}}
	ks.Tables["composite"] = &metadata.TableMetadata{Keyspace: "ks", Name: "composite", PartitionKey: []*metadata.ColumnMetadata{column("a", "int"), column("b", "text")}}
	routing.SnapshotData(next).InternalKeyspaces = map[string]*metadata.KeyspaceMetadata{"ks": ks}
	routing.SnapshotData(next).InternalPartitioner = partitioner
	routing.SnapshotData(next).InternalPartitioners = config.Partitioners
	p.InternalSnapshot.Store(next)

	prepare := func(table string, columns cqltype.Columns, indexes []int) *Prepared[snapshotTokenKey, codec.EmptyRecord] {
		// Explicit parameters, not ParamsOf: reflection would keep every
		// method alive and grow the test binary the benchmarks run in.
		encoder := codec.Params(
			codec.ParamAt(0, codec.Int32, func(k snapshotTokenKey) int32 { return k.A }),
			codec.ParamAt(1, codec.StringCodec, func(k snapshotTokenKey) string { return k.B }),
		)
		params := codecTestMust(encoder.Compile(columns))
		state := &InternalPreparedState{InternalSession: s, InternalProtocol: 4, InternalParameters: columns, partitioner: partitioner, InternalInfo: PreparedInfo{Keyspace: "ks", Table: table, PartitionKeyIndexes: indexes}}
		prepared := new(Prepared[snapshotTokenKey, codec.EmptyRecord])
		PreparedData(prepared).InternalState = state
		PreparedData(prepared).params = params
		PreparedData(prepared).rows = codec.EmptyRowDecoder
		PreparedData(prepared).plans = &InternalResultPlanCache[codec.EmptyRecord]{}
		return prepared
	}
	columns := cqltype.NewColumns(
		cqltype.ColumnSpec{Name: "a", Type: intType, Keyspace: "ks"},
		cqltype.ColumnSpec{Name: "b", Type: textType, Keyspace: "ks"},
	)
	key := snapshotTokenKey{A: 42, B: "partition"}
	text := codecTestMust(codec.TextValue(key.B))
	for _, tc := range []struct {
		table   string
		indexes []int
		values  []codec.Value
	}{
		{"single", []int{0}, []codec.Value{codec.IntValue(key.A)}},
		{"composite", []int{0, 1}, []codec.Value{codec.IntValue(key.A), text}},
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
