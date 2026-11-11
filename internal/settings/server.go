// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

// Server is the database a session is opened for. The public dialect types
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
