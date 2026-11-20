// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing_test

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/murmur"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
)

// murmur3MinKey is a partition key whose Murmur3 hash is math.MinInt64,
// which no partition's token is: Murmur3Partitioner maps it to MaxInt64.
var murmur3MinKey = []byte{0xee, 0x96, 0x16, 0x29, 0xb0, 0xb5, 0xad, 0x1d, 0x31, 0x9e, 0x18, 0xe8, 0x38, 0x92, 0xdb, 0xed}

// wrappedPartitioner hides the built-in partitioner it wraps, so the engine
// computes its tokens through Token rather than its own Murmur3 fast path.
type wrappedPartitioner struct{ routing.TokenPartitioner }

type constPartitioner int64

func (p constPartitioner) Token(routing.PartitionKey) (int64, error) { return int64(p), nil }

func blobCell(b []byte) model.EncodedCell {
	typ, _ := cql.ScalarType(cql.TypeBlob)
	return model.EncodedCell{Typ: typ, Body: cqltype.BytesChain(cql.BorrowBytes(b)), State: cql.Present}
}

// partitionerSnapshot has keyspace ks with a table for each partitioner
// test, each keyed by one blob, and the session's default partitioners plus
// my.Partitioner, which gives every key token 42.
func partitionerSnapshot(cluster string) *implRouting.MetadataSnapshot {
	registry := settings.DefaultConfig().Partitioners
	registry["my.Partitioner"] = constPartitioner(42)
	key := []*metadata.Column{{Keyspace: "ks", Name: "k", Type: "blob", Kind: metadata.ColumnPartitionKey}}
	table := func(partitioner string) *metadata.Table {
		return &metadata.Table{Keyspace: "ks", PartitionKey: key, Options: metadata.TableOptions{Partitioner: partitioner}}
	}
	return implRouting.NewMetadataSnapshot(implRouting.SnapshotState{
		InternalKeyspaces: map[string]*metadata.Keyspace{"ks": {Name: "ks", Tables: map[string]*metadata.Table{
			"default": table(""),
			"murmur3": table("org.apache.cassandra.dht.Murmur3Partitioner"),
			"cdc":     table("com.scylladb.dht.CDCPartitioner"),
			"random":  table("org.apache.cassandra.dht.RandomPartitioner"),
			"ordered": table("org.apache.cassandra.dht.ByteOrderedPartitioner"),
			"custom":  table("my.Partitioner"),
		}}},
		InternalPartitioner:  cluster,
		InternalPartitioners: registry,
	})
}

// TestPartitioners pins the token of a partition key under each partitioner
// a table can name: the session's built-in Murmur3 and CDC partitioners, a
// registered one, and partitioners it does not know, which give no token,
// so requests of their tables are not routed by token.
func TestPartitioners(t *testing.T) {
	cdcKey := binary.BigEndian.AppendUint64(nil, 0x0102030405060708)
	cdcKey = binary.BigEndian.AppendUint64(cdcKey, 0xffffffffffffffff)
	murmur3 := murmur.Murmur3H1([]byte("key"))

	t.Run("Murmur3MinToken", func(t *testing.T) {
		if h := murmur.Murmur3H1(murmur3MinKey); h != math.MinInt64 {
			t.Fatalf("fixture hash %d, want MinInt64", h)
		}
		cells := []model.EncodedCell{blobCell(murmur3MinKey)}
		for _, p := range []routing.TokenPartitioner{routing.Murmur3TokenPartitioner, wrappedPartitioner{routing.Murmur3TokenPartitioner}} {
			if token, err := implRouting.InternalComputeRoutingToken(cells, []int{0}, p); err != nil || token != cql.Some[int64](math.MaxInt64) {
				t.Fatalf("%T: token %v, %v; want MaxInt64", p, token, err)
			}
		}
		if token, err := partitionerSnapshot("Murmur3Partitioner").Token("ks", "default", cql.BlobValue(murmur3MinKey)); err != nil || token != math.MaxInt64 {
			t.Fatalf("snapshot token %d, %v; want MaxInt64", token, err)
		}
	})

	t.Run("Snapshot", func(t *testing.T) {
		for _, test := range []struct {
			name, cluster, table string
			key                  []byte
			token                int64
			valid                bool
		}{
			{"cluster murmur3", "org.apache.cassandra.dht.Murmur3Partitioner", "default", []byte("key"), murmur3, true},
			{"cluster murmur3 short name", "Murmur3Partitioner", "default", []byte("key"), murmur3, true},
			{"table murmur3", "org.apache.cassandra.dht.RandomPartitioner", "murmur3", []byte("key"), murmur3, true},
			{"cdc", "Murmur3Partitioner", "cdc", cdcKey, 0x0102030405060708, true},
			{"cdc short key", "Murmur3Partitioner", "cdc", []byte{1, 2, 3}, math.MinInt64, true},
			{"cdc min token", "Murmur3Partitioner", "cdc", binary.BigEndian.AppendUint64(nil, 1<<63), math.MaxInt64, true},
			{"registered", "Murmur3Partitioner", "custom", []byte("key"), 42, true},
			{"table random", "Murmur3Partitioner", "random", []byte("key"), 0, false},
			{"table byte ordered", "Murmur3Partitioner", "ordered", []byte("key"), 0, false},
			{"cluster random", "org.apache.cassandra.dht.RandomPartitioner", "default", []byte("key"), 0, false},
		} {
			t.Run(test.name, func(t *testing.T) {
				token, err := partitionerSnapshot(test.cluster).Token("ks", test.table, cql.BlobValue(test.key))
				if (err == nil) != test.valid || test.valid && token != test.token {
					t.Fatalf("token %d, %v; want %d, valid %v", token, err, test.token, test.valid)
				}
			})
		}
	})

	t.Run("RequestToken", func(t *testing.T) {
		// A request's partitioner is the one its table names; the session
		// finds none for a partitioner it does not know, and the request
		// then has no token.
		registry := settings.DefaultConfig().Partitioners
		cells := []model.EncodedCell{blobCell(cdcKey)}
		for _, test := range []struct {
			partitioner string
			token       cql.Option[int64]
		}{
			{"org.apache.cassandra.dht.Murmur3Partitioner", cql.Some(murmur.Murmur3H1(cdcKey))},
			{"Murmur3Partitioner", cql.Some(murmur.Murmur3H1(cdcKey))},
			{"com.scylladb.dht.CDCPartitioner", cql.Some[int64](0x0102030405060708)},
			{"CDCPartitioner", cql.Some[int64](0x0102030405060708)},
			{"org.apache.cassandra.dht.RandomPartitioner", cql.Option[int64]{}},
			{"org.apache.cassandra.dht.ByteOrderedPartitioner", cql.Option[int64]{}},
		} {
			p := registry[test.partitioner]
			if p == nil && test.token.Set {
				t.Fatalf("%s is not a default partitioner", test.partitioner)
			}
			if token, err := implRouting.InternalComputeRoutingToken(cells, []int{0}, p); err != nil || token != test.token {
				t.Fatalf("%s: token %v, %v; want %v", test.partitioner, token, err, test.token)
			}
		}
	})

	t.Run("VnodeRing", func(t *testing.T) {
		// Ring tokens -100 (host 1), 0 (host 2) and 100 (host 3): each host
		// owns the tokens after the previous one, up to and including its
		// own, and host 1 also those after 100, wrapping around.
		m := implRouting.NewMetadataSnapshot(implRouting.SnapshotState{
			InternalKeyspaces: map[string]*metadata.Keyspace{"ks": {Name: "ks", Tables: map[string]*metadata.Table{"t": {Name: "t"}}}},
			InternalRanges: map[string][]metadata.TokenRange{"ks": {
				{Start: 100, End: -100, Replicas: []metadata.Target{{HostID: cql.UUID{15: 1}}}},
				{Start: -100, End: 0, Replicas: []metadata.Target{{HostID: cql.UUID{15: 2}}}},
				{Start: 0, End: 100, Replicas: []metadata.Target{{HostID: cql.UUID{15: 3}}}},
			}},
		})
		for token, host := range map[int64]byte{math.MinInt64: 1, -101: 1, -100: 1, -99: 2, 0: 2, 1: 3, 100: 3, 101: 1, math.MaxInt64: 1} {
			replicas, err := m.Replicas("ks", "t", token)
			if err != nil || len(replicas) != 1 || replicas[0].HostID[15] != host {
				t.Fatalf("token %d: replicas %v, %v; want host %d", token, replicas, err, host)
			}
		}
	})
}
