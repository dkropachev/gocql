// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"errors"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/link"
)

// Snapshot is an immutable view of cluster topology and schema. A session
// publishes a new one when either changes; Session.Metadata returns the
// current one. A Snapshot is safe for concurrent use. Every host and schema
// value it returns is the caller's private copy: changing it does not change
// the snapshot. A nil or zero Snapshot knows no cluster: it reports no hosts
// or schema, and its lookups fail with a validation error.
type Snapshot struct{ src snapshotSource }

// snapshotSource is the session's snapshot behind a Snapshot.
type snapshotSource interface {
	Generation() uint64
	Hosts() []HostSnapshot
	Host(id cql.UUID) (HostSnapshot, bool)
	Keyspaces() []string
	Keyspace(name string) (*Keyspace, bool)
	Table(keyspace, name string) (*Table, bool)
	Replicas(keyspace, table string, token int64) ([]Target, error)
	Token(keyspace, table string, key ...cql.Value) (int64, error)
	TokenRanges(keyspace, table string) ([]TokenRange, error)
	Diagnostics() []error
}

// The engine wraps its snapshots, and unwraps them again, through
// internal/link.
func init() {
	link.Set(link.NewSnapshot, func(src any) *Snapshot { return &Snapshot{src: src.(snapshotSource)} })
	link.Set(link.SnapshotSource, func(s *Snapshot) any {
		if s == nil {
			return nil
		}
		return s.src
	})
}

// Generation identifies the snapshot: a later snapshot of the same session
// has a greater generation.
func (s *Snapshot) Generation() uint64 {
	if s.empty() {
		return 0
	}
	return s.src.Generation()
}

// Hosts returns the hosts of the cluster the session knows of. Host looks
// up one host without copying the others.
func (s *Snapshot) Hosts() []HostSnapshot {
	if s.empty() {
		return nil
	}
	return s.src.Hosts()
}

// Host returns the host with id.
func (s *Snapshot) Host(id cql.UUID) (HostSnapshot, bool) {
	if s.empty() {
		return HostSnapshot{}, false
	}
	return s.src.Host(id)
}

// Keyspaces returns the names of the keyspaces whose schema the snapshot
// holds, sorted. Which keyspaces those are depends on Mode.
func (s *Snapshot) Keyspaces() []string {
	if s.empty() {
		return nil
	}
	return s.src.Keyspaces()
}

// Keyspace returns the schema of the named keyspace, as far as Mode loads it.
// It copies the whole keyspace; Table copies one table.
func (s *Snapshot) Keyspace(name string) (*Keyspace, bool) {
	if s.empty() {
		return nil, false
	}
	return s.src.Keyspace(name)
}

// Table returns the schema of keyspace.name, a table (not a materialized
// view), as far as Mode loads it.
func (s *Snapshot) Table(keyspace, name string) (*Table, bool) {
	if s.empty() {
		return nil, false
	}
	return s.src.Table(keyspace, name)
}

// Replicas returns the replicas of token in keyspace.table, from the token
// ring or, for a tablet table, from its tablets. A keyspace or table the
// snapshot does not hold, because it does not exist or is not loaded, is a
// cql.ValidationError that wraps cqlerr.ErrNoKeyspace or cqlerr.ErrNoTable.
func (s *Snapshot) Replicas(keyspace, table string, token int64) ([]Target, error) {
	if s.empty() {
		return nil, errNoSnapshot
	}
	return s.src.Replicas(keyspace, table, token)
}

// Token returns the token of the partition whose partition-key columns hold
// key, in column order, for keyspace.table (a table or a materialized view).
// Each value is encoded as the column's schema type, exactly as a prepared
// statement's routing key is, and hashed with the table's partitioner. It
// needs loaded schema metadata: a keyspace or table the snapshot does not
// hold is a validation error, as for Replicas, and so is a key of the wrong
// arity or type, or a null component. Tokens are int64: the built-in
// partitioners are Murmur3 and ScyllaDB's CDC partitioner, and
// config.ConfigBuilder.WithPartitioner adds others with int64 tokens.
func (s *Snapshot) Token(keyspace, table string, key ...cql.Value) (int64, error) {
	if s.empty() {
		return 0, errNoSnapshot
	}
	return s.src.Token(keyspace, table, key...)
}

// TokenRanges returns the token ranges of keyspace.table with their replicas.
// A keyspace or table the snapshot does not hold is an error, as for
// Replicas.
func (s *Snapshot) TokenRanges(keyspace, table string) ([]TokenRange, error) {
	if s.empty() {
		return nil, errNoSnapshot
	}
	return s.src.TokenRanges(keyspace, table)
}

// Diagnostics returns the errors that made the snapshot incomplete, such as a
// schema table that failed to load.
func (s *Snapshot) Diagnostics() []error {
	if s.empty() {
		return nil
	}
	return s.src.Diagnostics()
}

// errNoSnapshot is the error of a nil or zero Snapshot, which knows no
// cluster.
var errNoSnapshot = &cql.ValidationError{Field: "metadata", Err: errors.New("empty snapshot")}

// empty reports whether s is nil or the zero Snapshot, which a session never
// returns.
func (s *Snapshot) empty() bool { return s == nil || s.src == nil }
