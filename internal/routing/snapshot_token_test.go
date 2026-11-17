// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	model "github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/murmur"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
)

func tokenTestSnapshot() *MetadataSnapshot {
	column := func(name, typ string) *metadata.Column {
		return &metadata.Column{Keyspace: "ks", Name: name, Type: typ, Kind: metadata.ColumnPartitionKey}
	}
	ks := &metadata.Keyspace{
		Name: "ks",
		Tables: map[string]*metadata.Table{
			"single":    {Keyspace: "ks", Name: "single", PartitionKey: []*metadata.Column{column("id", "int")}},
			"composite": {Keyspace: "ks", Name: "composite", PartitionKey: []*metadata.Column{column("a", "int"), column("b", "text")}},
			"tupled":    {Keyspace: "ks", Name: "tupled", PartitionKey: []*metadata.Column{column("t", "frozen<tuple<int, text>>")}},
			"custom":    {Keyspace: "ks", Name: "custom", PartitionKey: []*metadata.Column{column("id", "int")}, Options: metadata.TableOptions{Partitioner: "unknown"}},
			"nokey":     {Keyspace: "ks", Name: "nokey"},
		},
	}
	return NewMetadataSnapshot(SnapshotState{
		InternalKeyspaces:    map[string]*metadata.Keyspace{"ks": ks},
		InternalPartitioner:  "org.apache.cassandra.dht.Murmur3Partitioner",
		InternalPartitioners: map[string]routing.TokenPartitioner{"org.apache.cassandra.dht.Murmur3Partitioner": routing.Murmur3TokenPartitioner},
	})
}

// routingCells encodes values with typed codecs, as a prepared statement's
// parameters are encoded, for comparison with Token.
func routingCell[T any](t *testing.T, typ cql.DataType, encoder cql.Encoder[T], value T) model.EncodedCell {
	t.Helper()
	encoded, err := cql.EncodeValue(typ, encoder, value)
	if err != nil {
		t.Fatal(err)
	}
	return model.EncodedCell{Typ: typ, Body: cqltype.BytesChain(encoded.Bytes()), State: encoded.State()}
}

func TestSnapshotToken(t *testing.T) {
	m := tokenTestSnapshot()
	intType, _ := cql.ScalarType(cql.TypeInt)
	textType, _ := cql.ScalarType(cql.TypeText)

	t.Run("SingleKeyMurmur3Vector", func(t *testing.T) {
		for _, n := range []int32{0, 1, 42, -1, math.MaxInt32, math.MinInt32} {
			got, err := m.Token("ks", "single", cql.IntValue(n))
			if err != nil {
				t.Fatal(err)
			}
			var key [4]byte
			binary.BigEndian.PutUint32(key[:], uint32(n))
			want := murmur.Murmur3H1(key[:])
			if want == math.MinInt64 {
				want = math.MaxInt64
			}
			if got != want {
				t.Fatalf("Token(%d) = %d, want %d", n, got, want)
			}
		}
	})
	t.Run("MatchesPreparedRouting", func(t *testing.T) {
		text, err := cql.TextValue("key")
		if err != nil {
			t.Fatal(err)
		}
		got, err := m.Token("ks", "composite", cql.IntValue(7), text)
		if err != nil {
			t.Fatal(err)
		}
		cells := []model.EncodedCell{routingCell(t, intType, cql.Int32Codec, int32(7)), routingCell(t, textType, cql.StringCodec, "key")}
		want, err := InternalComputeRoutingToken(cells, []int{0, 1}, routing.Murmur3TokenPartitioner)
		if err != nil || !want.Set {
			t.Fatal(want, err)
		}
		if got != want.Value {
			t.Fatalf("composite Token = %d, prepared routing = %d", got, want.Value)
		}
	})
	t.Run("FrozenTuple", func(t *testing.T) {
		tupleType, err := cql.TupleType(intType, textType)
		if err != nil {
			t.Fatal(err)
		}
		text, _ := cql.TextValue("x")
		tuple, err := cql.TupleValue(tupleType.WithFrozen(true), []cql.Value{cql.IntValue(1), text})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := m.Token("ks", "tupled", tuple); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("Errors", func(t *testing.T) {
		text, _ := cql.TextValue("x")
		for name, call := range map[string]func() error{
			"arity":       func() error { _, err := m.Token("ks", "composite", cql.IntValue(1)); return err },
			"type":        func() error { _, err := m.Token("ks", "single", text); return err },
			"null":        func() error { _, err := m.Token("ks", "single", cql.NullValue(intType)); return err },
			"partitioner": func() error { _, err := m.Token("ks", "custom", cql.IntValue(1)); return err },
			"no key":      func() error { _, err := m.Token("ks", "nokey"); return err },
		} {
			if err := call(); err == nil {
				t.Errorf("%s: no error", name)
			}
		}
		if _, err := m.Token("ks", "missing", cql.IntValue(1)); !errors.Is(err, cqlerr.ErrNoTable) {
			t.Errorf("missing table: %v", err)
		}
		if _, err := m.Token("nope", "single", cql.IntValue(1)); !errors.Is(err, cqlerr.ErrNoKeyspace) {
			t.Errorf("missing keyspace: %v", err)
		}
	})
}
