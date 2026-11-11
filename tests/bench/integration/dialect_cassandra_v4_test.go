//go:build integration && dialect_cassandra_v4

package integration

import gocql "github.com/scylladb/gocql/v2"

// testDialect: see dialect_scylla_test.go.
type testDialect = gocql.CassandraV4

const testDialectDistribution, testDialectProtocol = "cassandra", 4
