// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"errors"
	"math"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/murmur"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// PartitionKey is a request's encoded partition key: the bytes a
// TokenPartitioner hashes. For a composite key it is the components, each
// with its length and a terminator, as the server encodes it.
type PartitionKey struct{ state partitionKeyState }

// partitionKeyState is the key's bytes. The engine builds it through the
// mirror in internal/routing.
type partitionKeyState struct {
	chain wire.Chain
}

// Len returns the key's length in bytes.
func (k PartitionKey) Len() int64 { return k.state.chain.Len() }

// ForEach calls fn with the key's bytes, in order, in one or more pieces.
func (k PartitionKey) ForEach(fn func(cql.Bytes) error) error {
	if fn == nil {
		return &cql.ValidationError{Field: "partition key", Err: errors.New("nil visitor")}
	}
	return k.state.chain.Each(func(s wire.Span) error { return fn(cql.BorrowBytes(s.Bytes())) })
}

// TokenPartitioner computes the token of a partition key.
type TokenPartitioner interface {
	Token(PartitionKey) (int64, error)
}

// Murmur3TokenPartitioner is the partitioner of Murmur3Partitioner tables,
// the default.
var Murmur3TokenPartitioner TokenPartitioner = murmur3Partitioner{}

// CDCTokenPartitioner is the partitioner of ScyllaDB's CDC log tables.
var CDCTokenPartitioner TokenPartitioner = cdcPartitioner{}

type murmur3Partitioner struct{}

func (murmur3Partitioner) Token(k PartitionKey) (int64, error) {
	var hash murmur.Stream
	err := k.state.chain.Each(func(s wire.Span) error { _, err := hash.Write(s.Bytes()); return err })
	token := hash.Sum64()
	if token == math.MinInt64 {
		token = math.MaxInt64
	}
	return token, err
}

type cdcPartitioner struct{}

func (cdcPartitioner) Token(k PartitionKey) (int64, error) {
	if k.Len() < 8 {
		return math.MinInt64, nil
	}
	var n uint64
	remaining := 8
	e := k.state.chain.Each(func(s wire.Span) error {
		for _, b := range s.Bytes() {
			if remaining == 0 {
				break
			}
			n = n<<8 | uint64(b)
			remaining--
		}
		return nil
	})
	v := int64(n)
	if v == math.MinInt64 {
		v = math.MaxInt64
	}
	return v, e
}
