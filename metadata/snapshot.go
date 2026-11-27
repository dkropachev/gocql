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
//
// Lookups take the server's internal names of keyspaces, tables, views and
// types, as system_schema stores them: an unquoted CQL identifier is folded
// to lower case, a quoted one keeps its case. Name converts a CQL identifier
// to its internal name.
type Snapshot struct{ src snapshotSource }

// snapshotSource is the session's snapshot behind a Snapshot.
type snapshotSource interface {
	Generation() uint64
	Hosts() []HostSnapshot
	Host(id cql.UUID) (HostSnapshot, bool)
	HostInfo(id cql.UUID) (HostSnapshot, bool)
	Keyspaces() []string
	Tables(keyspace string) []string
	Keyspace(name string) (*Keyspace, bool)
	Table(keyspace, name string) (*Table, bool)
	View(keyspace, name string) (*View, bool)
	Type(keyspace, name string) (*Type, bool)
	Replicas(keyspace, table string, token int64) ([]Target, error)
	Token(keyspace, table string, key ...cql.Value) (int64, error)
	TokenRanges(keyspace, table string) ([]TokenRange, bool, error)
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

// HostInfo returns the host with id, as Host does, but without its Tokens,
// which Host copies: a host has 256 vnode tokens by default, and a caller
// that wants only its address, location or state need not copy them.
func (s *Snapshot) HostInfo(id cql.UUID) (HostSnapshot, bool) {
	if s.empty() {
		return HostSnapshot{}, false
	}
	return s.src.HostInfo(id)
}

// Keyspaces returns the names of the keyspaces whose schema the snapshot
// holds, sorted. Which keyspaces those are depends on Mode.
func (s *Snapshot) Keyspaces() []string {
	if s.empty() {
		return nil
	}
	return s.src.Keyspaces()
}

// Tables returns the names of the tables of keyspace that the snapshot
// holds, sorted, without copying their schema. It lists no materialized
// views, and returns nil for a keyspace the snapshot does not hold.
func (s *Snapshot) Tables(keyspace string) []string {
	if s.empty() {
		return nil
	}
	return s.src.Tables(keyspace)
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

// View returns the schema of the materialized view keyspace.name, as far as
// Mode loads it. A view that backs a secondary index is not a View; see
// Keyspace.Indexes.
func (s *Snapshot) View(keyspace, name string) (*View, bool) {
	if s.empty() {
		return nil, false
	}
	return s.src.View(keyspace, name)
}

// Type returns the user-defined type keyspace.name, as far as Mode loads it.
func (s *Snapshot) Type(keyspace, name string) (*Type, bool) {
	if s.empty() {
		return nil, false
	}
	return s.src.Type(keyspace, name)
}

// Replicas returns the replicas of token in keyspace.table, from the
// snapshot's own token ring or, for a tablet table, its own tablets, as
// routing.ReplicaRoutes finds them. A tablet replica's ShardID is its
// tablet's, and a vnode replica's the shard that owns token on the host when
// HostSnapshot.Shards is known.
//
// A keyspace the snapshot does not hold, because it does not exist or is not
// loaded, is a cql.ValidationError that wraps cqlerr.ErrNoKeyspace. A table
// it does not hold is placed by its keyspace's token ring, as routing does
// under ModeKeyspaces, except in a tablet keyspace, whose tables have their
// own placement: there it is a cql.ValidationError that wraps
// cqlerr.ErrNoTable. A token of a tablet table that no tablet the snapshot
// holds covers is an error that wraps ErrPlacementUnknown.
func (s *Snapshot) Replicas(keyspace, table string, token int64) ([]Target, error) {
	if s.empty() {
		return nil, errNoSnapshot
	}
	return s.src.Replicas(keyspace, table, token)
}

// PartitionReplicas returns the hosts of the replicas of the partition whose
// partition-key columns hold key, in column order, in keyspace.table: Token
// and then Replicas, with each replica's host as HostInfo returns it. It fails
// as Token and Replicas do.
func (s *Snapshot) PartitionReplicas(keyspace, table string, key ...cql.Value) ([]HostSnapshot, error) {
	token, err := s.Token(keyspace, table, key...)
	if err != nil {
		return nil, err
	}
	targets, err := s.Replicas(keyspace, table, token)
	if err != nil {
		return nil, err
	}
	hosts := make([]HostSnapshot, 0, len(targets))
	for _, t := range targets {
		if h, ok := s.src.HostInfo(t.HostID); ok {
			hosts = append(hosts, h)
		}
	}
	return hosts, nil
}

// Token returns the token of the partition whose partition-key columns hold
// key, in column order, for keyspace.table (a table or a materialized view).
// Each value is encoded as the column's schema type, exactly as a prepared
// statement's routing key is, and hashed with the table's partitioner. It
// needs loaded schema metadata: a keyspace or table the snapshot does not
// hold is a validation error that wraps cqlerr.ErrNoKeyspace or
// cqlerr.ErrNoTable, and so is a key of the wrong arity or type, or a null
// component. Tokens are int64: the built-in partitioners are Murmur3 and
// ScyllaDB's CDC partitioner, and config.ConfigBuilder.WithPartitioner adds
// others with int64 tokens.
func (s *Snapshot) Token(keyspace, table string, key ...cql.Value) (int64, error) {
	if s.empty() {
		return 0, errNoSnapshot
	}
	return s.src.Token(keyspace, table, key...)
}

// TokenRanges returns the token ranges of keyspace.table with their
// replicas, in token order, from the snapshot's own token ring or tablets,
// and reports whether they cover the whole ring. A token ring does once the
// snapshot has one. A tablet table does only once the snapshot holds every
// one of its tablets, which the session learns from the responses to its
// requests, so a tablet table's ranges are often incomplete: a token outside
// them has no known replicas. A keyspace or table the snapshot does not hold
// is a cql.ValidationError that wraps cqlerr.ErrNoKeyspace or
// cqlerr.ErrNoTable.
func (s *Snapshot) TokenRanges(keyspace, table string) (ranges []TokenRange, complete bool, err error) {
	if s.empty() {
		return nil, false, errNoSnapshot
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

// ErrPlacementUnknown is the error Snapshot.Replicas and PartitionReplicas
// wrap for a token of a tablet table whose tablet the snapshot does not hold.
// The session learns a table's tablets from the responses to its requests,
// so a token no request has reached yet, or whose tablet the session forgot,
// has no known replicas; a later snapshot may have them.
var ErrPlacementUnknown = errors.New("metadata: tablet placement of the token is not known yet")

// errNoSnapshot is the error of a nil or zero Snapshot, which knows no
// cluster.
var errNoSnapshot = &cql.ValidationError{Field: "metadata", Err: errors.New("empty snapshot")}

// empty reports whether s is nil or the zero Snapshot, which a session never
// returns.
func (s *Snapshot) empty() bool { return s == nil || s.src == nil }
