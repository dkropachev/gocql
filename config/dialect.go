// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package config

import (
	"fmt"
	"strings"

	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

// Dialect is the database and native protocol version a session speaks. The
// caller, who knows which cluster they connect to, passes it to New:
//
//	cfg, err := config.New(config.ScyllaV4, "10.0.0.1").Build()
//
// The driver negotiates no protocol version. Every connection speaks the
// dialect's version, and gocql.Connect fails when the cluster does not speak
// it or is not the database the dialect names (cqlerr.ServerMismatchError).
//
// The dialect decides what the driver can know before a request runs and how
// it reads results that differ between Cassandra and ScyllaDB; see
// docs/source/server-differences.md. Because the dialect is part of the
// configuration, Build rejects a setting the dialect does not support with a
// cqlerr.UnsupportedSettingError, before a session is opened. It is fixed when
// the session opens, so requests pay nothing for it.
//
// The dialects are ScyllaV4, CassandraV4 and CassandraV5. The zero Dialect
// names none, and Build rejects it with a cql.ValidationError. Later releases
// may add values, so a switch over them needs a default case.
type Dialect uint8

const (
	// ScyllaV4 is ScyllaDB over native protocol 4 with ScyllaDB's protocol
	// extensions: shard awareness, tablet routing, the LWT flag in prepared
	// metadata and the result metadata ID. ScyllaDB marks conditional
	// statements in prepared metadata, so Exec rejects them before sending.
	ScyllaV4 = Dialect(implSettings.DialectScyllaV4)
	// CassandraV4 is Apache Cassandra over native protocol 4.
	CassandraV4 = Dialect(implSettings.DialectCassandraV4)
	// CassandraV5 is Apache Cassandra 4.0 or later over native protocol 5:
	// checksummed transport segments, per-request keyspace and
	// now_in_seconds, and the result metadata ID.
	CassandraV5 = Dialect(implSettings.DialectCassandraV5)
)

// String returns the dialect's Go name, such as "ScyllaV4", or Dialect(n)
// for a value it does not define.
func (d Dialect) String() string {
	if server, protocol, ok := implSettings.DialectSpec(uint8(d)); ok {
		return implSettings.DialectName(server, protocol)
	}
	return fmt.Sprintf("Dialect(%d)", uint8(d))
}

// MarshalText implements encoding.TextMarshaler with the name String
// returns, and returns an error for a value String does not name, such as
// the zero Dialect.
func (d Dialect) MarshalText() ([]byte, error) {
	if _, _, ok := implSettings.DialectSpec(uint8(d)); !ok {
		return nil, fmt.Errorf("invalid dialect %v", d)
	}
	return []byte(d.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler, so a Dialect can be a
// flag (flag.TextVar) or a field of a JSON or YAML configuration. It accepts
// the names String returns, "ScyllaV4", "CassandraV4" and "CassandraV5", in
// any case, and returns an error for any other text.
func (d *Dialect) UnmarshalText(text []byte) error {
	for _, candidate := range [...]Dialect{ScyllaV4, CassandraV4, CassandraV5} {
		if strings.EqualFold(string(text), candidate.String()) {
			*d = candidate
			return nil
		}
	}
	return fmt.Errorf("invalid dialect %q; want ScyllaV4, CassandraV4 or CassandraV5", string(text))
}

// ParseDialect parses a dialect name, such as "ScyllaV4", ignoring case. It
// is Dialect.UnmarshalText for a string.
func ParseDialect(s string) (Dialect, error) {
	var d Dialect
	err := d.UnmarshalText([]byte(s))
	return d, err
}
