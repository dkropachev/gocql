// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import "fmt"

// Server is the database a session is opened for. The public dialects
// (gocql.ScyllaV4, gocql.CassandraV4, gocql.CassandraV5) set it together with
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

// DialectName names the public dialect (gocql.Dialect) of server over
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
func (c *Config) ScyllaOnly(setting string) error {
	if c.Server != ServerCassandra {
		return nil
	}
	return &UnsupportedSettingError{Setting: setting, Reason: "only the ScyllaV4 dialect supports it, and the session's dialect is " + DialectName(c.Server, c.Protocol)}
}

// ProtocolV5Only reports a setting that only native protocol 5 (the
// CassandraV5 dialect) supports, used on a session of protocol 4, as
// ScyllaOnly does. It returns nil for a session whose protocol is not 4.
func (c *Config) ProtocolV5Only(setting string) error {
	if c.Protocol != 4 {
		return nil
	}
	return &UnsupportedSettingError{Setting: setting, Reason: "only the CassandraV5 dialect supports it, and the session's dialect is " + DialectName(c.Server, c.Protocol)}
}
