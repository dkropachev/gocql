// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import "fmt"

// Server is the database a session is opened for. The public dialects
// (config.ScyllaV4, config.CassandraV4, config.CassandraV5) set it together with
// the protocol version. Opening a session checks the server it reaches
// against it. The zero Server checks nothing; only the driver's own tests use
// it.
type Server uint8

const (
	ServerAny Server = iota
	ServerCassandra
	ServerScylla
)

func (s Server) String() string {
	switch s {
	case ServerCassandra:
		return "Cassandra"
	case ServerScylla:
		return "ScyllaDB"
	}
	return "any server"
}

// The values of the public config.Dialect. DialectSpec and DialectOf map them
// to and from the protocol version and server they set.
const (
	DialectScyllaV4 uint8 = iota + 1
	DialectCassandraV4
	DialectCassandraV5
)

// DialectSpec returns the server and protocol version public dialect d sets;
// ok is false for a value that names no dialect.
func DialectSpec(d uint8) (server Server, protocol int, ok bool) {
	switch d {
	case DialectScyllaV4:
		return ServerScylla, 4, true
	case DialectCassandraV4:
		return ServerCassandra, 4, true
	case DialectCassandraV5:
		return ServerCassandra, 5, true
	}
	return ServerAny, 0, false
}

// DialectOf returns the public dialect whose settings are server and
// protocol, or zero when none is.
func DialectOf(server Server, protocol int) uint8 {
	for _, d := range [...]uint8{DialectScyllaV4, DialectCassandraV4, DialectCassandraV5} {
		if s, p, _ := DialectSpec(d); s == server && p == protocol {
			return d
		}
	}
	return 0
}

// DialectName names the public dialect (config.Dialect) of server over
// protocol, such as "CassandraV4".
func DialectName(server Server, protocol int) string {
	switch {
	case server == ServerScylla && protocol == 4:
		return "ScyllaV4"
	case server == ServerCassandra && protocol == 4:
		return "CassandraV4"
	case server == ServerCassandra && protocol == 5:
		return "CassandraV5"
	}
	return fmt.Sprintf("%v over protocol %d", server, protocol)
}

// ScyllaOnly reports a setting that only the ScyllaV4 dialect supports, used
// on a session of c's Cassandra dialect, as an UnsupportedSettingError naming
// the setting and the dialect. It returns nil for a ScyllaDB session, and for
// one of ServerAny, which only the driver's own tests open.
func ScyllaOnly(c *Config, setting string) error {
	if c.Server != ServerCassandra {
		return nil
	}
	return &UnsupportedSettingError{Setting: setting, Reason: "only the ScyllaV4 dialect supports it, and the session's dialect is " + DialectName(c.Server, c.Protocol)}
}

// ProtocolV5Only reports a setting that only native protocol 5 (the
// CassandraV5 dialect) supports, used on a session of protocol 4 of any
// server, as ScyllaOnly does. It returns nil for a session whose protocol is
// not 4.
func ProtocolV5Only(c *Config, setting string) error {
	if c.Protocol != 4 {
		return nil
	}
	return &UnsupportedSettingError{Setting: setting, Reason: "only the CassandraV5 dialect supports it, and the session's dialect is " + DialectName(c.Server, c.Protocol)}
}
