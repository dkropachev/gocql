// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"errors"
	"io"
	"math"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/link"
	"github.com/scylladb/gocql/v2/internal/murmur"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// PartitionKey is a request's encoded partition key: the bytes a
// TokenPartitioner hashes. For a composite key it is the components, each
// with its length and a terminator, as the server encodes it.
//
// The zero PartitionKey has no components.
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

// WriteTo writes the key's bytes to w, as io.WriterTo does, so that a
// TokenPartitioner can hash the key with a hash.Hash in one call. It returns
// the bytes written and the first error of w.
func (k PartitionKey) WriteTo(w io.Writer) (int64, error) {
	if w == nil {
		return 0, &cql.ValidationError{Field: "partition key", Err: errors.New("nil writer")}
	}
	var n int64
	err := k.state.chain.Each(func(s wire.Span) error {
		m, err := w.Write(s.Bytes())
		n += int64(m)
		return err
	})
	return n, err
}

var _ io.WriterTo = PartitionKey{}

// TokenPartitioner computes the token of a partition key.
type TokenPartitioner interface {
	// Token returns the token of the key.
	Token(PartitionKey) (int64, error)
}

// TokenPartitionerFunc adapts a function to TokenPartitioner.
type TokenPartitionerFunc func(PartitionKey) (int64, error)

// Token calls f.
func (f TokenPartitionerFunc) Token(key PartitionKey) (int64, error) { return f(key) }

// Murmur3TokenPartitioner is the partitioner of Murmur3Partitioner tables,
// the default.
var Murmur3TokenPartitioner TokenPartitioner = murmur3Partitioner{}

// CDCTokenPartitioner is the partitioner of ScyllaDB's CDC log tables. The
// driver picks it for a table whose partitioner class names it, so on
// Cassandra, which has no such tables, it is never used.
var CDCTokenPartitioner TokenPartitioner = cdcPartitioner{}

type murmur3Partitioner struct{}

// The engine hashes keys of the built-in Murmur3 partitioner itself, without
// building a PartitionKey; it asks through internal/link whether a
// partitioner is that one.
func init() {
	link.Set(link.IsMurmur3Partitioner, func(p TokenPartitioner) bool {
		_, ok := p.(murmur3Partitioner)
		return ok
	})
}

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
