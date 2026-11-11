// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"

	codec "github.com/scylladb/gocql/v2/internal/codec"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	model "github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/murmur"
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

func tokenTestSnapshot() *MetadataSnapshot {
	column := func(name, typ string) *implMetadata.ColumnMetadata {
		return &implMetadata.ColumnMetadata{Keyspace: "ks", Name: name, Type: typ, Kind: implMetadata.ColumnPartitionKey}
	}
	ks := &implMetadata.KeyspaceMetadata{
		Name: "ks",
		Tables: map[string]*implMetadata.TableMetadata{
			"single":    {Keyspace: "ks", Name: "single", PartitionKey: []*implMetadata.ColumnMetadata{column("id", "int")}},
			"composite": {Keyspace: "ks", Name: "composite", PartitionKey: []*implMetadata.ColumnMetadata{column("a", "int"), column("b", "text")}},
			"tupled":    {Keyspace: "ks", Name: "tupled", PartitionKey: []*implMetadata.ColumnMetadata{column("t", "frozen<tuple<int, text>>")}},
			"custom":    {Keyspace: "ks", Name: "custom", PartitionKey: []*implMetadata.ColumnMetadata{column("id", "int")}, Options: implMetadata.TableMetadataOptions{Partitioner: "unknown"}},
			"nokey":     {Keyspace: "ks", Name: "nokey"},
		},
	}
	return NewMetadataSnapshot(SnapshotState{
		InternalKeyspaces:    map[string]*implMetadata.KeyspaceMetadata{"ks": ks},
		InternalPartitioner:  "org.apache.cassandra.dht.Murmur3Partitioner",
		InternalPartitioners: map[string]TokenPartitioner{"org.apache.cassandra.dht.Murmur3Partitioner": Murmur3TokenPartitioner},
	})
}

// routingCells encodes values with typed codecs, as a prepared statement's
// parameters are encoded, for comparison with Token.
func routingCell[T any](t *testing.T, typ cqltype.DataType, encoder codec.Encoder[T], value T) model.EncodedCell {
	t.Helper()
	encoded, err := codec.EncodeValue(typ, encoder, value)
	if err != nil {
		t.Fatal(err)
	}
	return model.EncodedCell{Typ: typ, Body: cqltype.BytesChain(encoded.Bytes()), State: encoded.State()}
}

func TestSnapshotToken(t *testing.T) {
	m := tokenTestSnapshot()
	intType, _ := cqltype.ScalarType(cqltype.TypeInt)
	textType, _ := cqltype.ScalarType(cqltype.TypeText)

	t.Run("SingleKeyMurmur3Vector", func(t *testing.T) {
		for _, n := range []int32{0, 1, 42, -1, math.MaxInt32, math.MinInt32} {
			got, err := m.Token("ks", "single", codec.IntValue(n))
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
		text, err := codec.TextValue("key")
		if err != nil {
			t.Fatal(err)
		}
		got, err := m.Token("ks", "composite", codec.IntValue(7), text)
		if err != nil {
			t.Fatal(err)
		}
		cells := []model.EncodedCell{routingCell(t, intType, codec.Int32, int32(7)), routingCell(t, textType, codec.StringCodec, "key")}
		want, err := InternalComputeRoutingToken(cells, []int{0, 1}, Murmur3TokenPartitioner)
		if err != nil || !want.Set {
			t.Fatal(want, err)
		}
		if got != want.Value {
			t.Fatalf("composite Token = %d, prepared routing = %d", got, want.Value)
		}
	})
	t.Run("FrozenTuple", func(t *testing.T) {
		tupleType, err := cqltype.TupleType(intType, textType)
		if err != nil {
			t.Fatal(err)
		}
		text, _ := codec.TextValue("x")
		tuple, err := codec.TupleValue(tupleType.WithFrozen(true), []codec.Value{codec.IntValue(1), text})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := m.Token("ks", "tupled", tuple); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("Errors", func(t *testing.T) {
		text, _ := codec.TextValue("x")
		for name, call := range map[string]func() error{
			"arity":       func() error { _, err := m.Token("ks", "composite", codec.IntValue(1)); return err },
			"type":        func() error { _, err := m.Token("ks", "single", text); return err },
			"null":        func() error { _, err := m.Token("ks", "single", codec.NullValue(intType)); return err },
			"partitioner": func() error { _, err := m.Token("ks", "custom", codec.IntValue(1)); return err },
			"no key":      func() error { _, err := m.Token("ks", "nokey"); return err },
		} {
			if err := call(); err == nil {
				t.Errorf("%s: no error", name)
			}
		}
		if _, err := m.Token("ks", "missing", codec.IntValue(1)); !errors.Is(err, runtimeerrors.ErrNoTable) {
			t.Errorf("missing table: %v", err)
		}
		if _, err := m.Token("nope", "single", codec.IntValue(1)); !errors.Is(err, runtimeerrors.ErrNoKeyspace) {
			t.Errorf("missing keyspace: %v", err)
		}
	})
}
