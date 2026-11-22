//go:build integration && dialect_cassandra_v5

package integration

import "github.com/scylladb/gocql/v2/config"

// testDialect: see dialect_scylla_test.go.
const testDialect = config.CassandraV5

const testDialectDistribution, testDialectProtocol = "cassandra", 5
