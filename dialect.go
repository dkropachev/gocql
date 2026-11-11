// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import implSettings "github.com/scylladb/gocql/v2/internal/settings"

// Dialect is the database and native protocol version a Session speaks. It
// is the type parameter of Session and Connect, chosen by the caller, who
// knows which cluster they connect to:
//
//	session, err := gocql.Connect[gocql.ScyllaV4](ctx, config)
//
// The driver negotiates no protocol version. Every connection speaks the
// dialect's version, and Connect fails when the cluster does not speak it or
// is not the database the dialect names (cqlerr.ServerMismatchError).
//
// The dialect decides what the driver can know before a request runs and how
// it reads results that differ between Cassandra and ScyllaDB; see
// docs/source/server-differences.md. It is fixed when the session opens, so
// requests pay nothing for it. API that only one dialect supports takes a
// Session of that dialect, so using it with another does not compile.
//
// The dialects are ScyllaV4, CassandraV4 and CassandraV5. The interface is
// sealed: no other type satisfies it.
type Dialect interface {
	dialect() dialectSpec
}

// dialectSpec is what a dialect sets in the session's settings.
type dialectSpec struct {
	protocol int
	server   implSettings.Server
}

// ScyllaV4 is ScyllaDB over native protocol 4 with ScyllaDB's protocol
// extensions: shard awareness, tablet routing, the LWT flag in prepared
// metadata and the result metadata ID. ScyllaDB marks conditional statements
// in prepared metadata, so Exec rejects them before sending.
type ScyllaV4 struct{}

func (ScyllaV4) dialect() dialectSpec {
	return dialectSpec{protocol: 4, server: implSettings.ServerScylla}
}

// CassandraV4 is Apache Cassandra over native protocol 4.
type CassandraV4 struct{}

func (CassandraV4) dialect() dialectSpec {
	return dialectSpec{protocol: 4, server: implSettings.ServerCassandra}
}

// CassandraV5 is Apache Cassandra 4.0 or later over native protocol 5:
// checksummed transport segments, per-request keyspace and now_in_seconds,
// and the result metadata ID.
type CassandraV5 struct{}

func (CassandraV5) dialect() dialectSpec {
	return dialectSpec{protocol: 5, server: implSettings.ServerCassandra}
}

// dialectSettings returns config's settings for dialect D.
func dialectSettings[D Dialect](config Config) (implSettings.Config, error) {
	settings, err := config.resolved()
	if err != nil {
		return settings, err
	}
	var d D
	spec := d.dialect()
	settings.Protocol, settings.Server = spec.protocol, spec.server
	return settings, nil
}
