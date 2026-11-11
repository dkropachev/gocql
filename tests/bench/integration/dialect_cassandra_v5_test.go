//go:build integration && dialect_cassandra_v5

package integration

import gocql "github.com/scylladb/gocql/v2"

// testDialect: see dialect_scylla_test.go.
type testDialect = gocql.CassandraV5

const testDialectDistribution, testDialectProtocol = "cassandra", 5
