// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"fmt"

	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/protocol"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

// Dialect is the database and native protocol version a Session speaks. The
// caller, who knows which cluster they connect to, passes it to Connect:
//
//	session, err := gocql.Connect(ctx, cfg, gocql.ScyllaV4)
//
// The driver negotiates no protocol version. Every connection speaks the
// dialect's version, and Connect fails when the cluster does not speak it or
// is not the database the dialect names (cqlerr.ServerMismatchError).
//
// The dialect decides what the driver can know before a request runs and how
// it reads results that differ between Cassandra and ScyllaDB; see
// docs/source/server-differences.md. It is fixed when the session opens, so
// requests pay nothing for it. API that only one dialect supports returns an
// error on a session of another dialect, before anything is sent.
//
// The dialects are ScyllaV4, CassandraV4 and CassandraV5. The zero Dialect
// names none, and Connect rejects it with a cql.ValidationError. Later releases
// may add values, so a switch over them needs a default case.
type Dialect uint8

const (
	// ScyllaV4 is ScyllaDB over native protocol 4 with ScyllaDB's protocol
	// extensions: shard awareness, tablet routing, the LWT flag in prepared
	// metadata and the result metadata ID. ScyllaDB marks conditional
	// statements in prepared metadata, so Exec rejects them before sending.
	ScyllaV4 Dialect = iota + 1
	// CassandraV4 is Apache Cassandra over native protocol 4.
	CassandraV4
	// CassandraV5 is Apache Cassandra 4.0 or later over native protocol 5:
	// checksummed transport segments, per-request keyspace and
	// now_in_seconds, and the result metadata ID.
	CassandraV5
)

// String returns the dialect's Go name, such as "ScyllaV4", or Dialect(n)
// for a value it does not define.
func (d Dialect) String() string {
	switch d {
	case ScyllaV4:
		return "ScyllaV4"
	case CassandraV4:
		return "CassandraV4"
	case CassandraV5:
		return "CassandraV5"
	}
	return fmt.Sprintf("Dialect(%d)", uint8(d))
}

// dialectSpec is what a dialect sets in the session's settings.
type dialectSpec struct {
	protocol int
	server   implSettings.Server
}

// spec returns d's settings; ok is false for a value that names no dialect.
func (d Dialect) spec() (spec dialectSpec, ok bool) {
	switch d {
	case ScyllaV4:
		return dialectSpec{protocol: protocol.ProtoVersion4, server: implSettings.ServerScylla}, true
	case CassandraV4:
		return dialectSpec{protocol: protocol.ProtoVersion4, server: implSettings.ServerCassandra}, true
	case CassandraV5:
		return dialectSpec{protocol: protocol.ProtoVersion5, server: implSettings.ServerCassandra}, true
	}
	return dialectSpec{}, false
}

// dialectOf returns the dialect a session was opened with: the one whose
// settings match protocol and server, or the zero Dialect.
func dialectOf(protocolVersion int, server implSettings.Server) Dialect {
	for _, d := range [...]Dialect{ScyllaV4, CassandraV4, CassandraV5} {
		if spec, _ := d.spec(); spec.protocol == protocolVersion && spec.server == server {
			return d
		}
	}
	return 0
}

// dialectSettings returns cfg's settings for dialect d.
func dialectSettings(cfg config.Config, d Dialect) (implSettings.Config, error) {
	spec, ok := d.spec()
	if !ok {
		return implSettings.Config{}, &cql.ValidationError{Field: "Dialect", Err: fmt.Errorf("%v is not ScyllaV4, CassandraV4 or CassandraV5", d)}
	}
	settings, err := implSettings.InternalResolvePublic(cfg)
	if err != nil {
		return settings, err
	}
	settings.Protocol, settings.Server = spec.protocol, spec.server
	return settings, nil
}
