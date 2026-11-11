//go:build integration && !dialect_cassandra_v4 && !dialect_cassandra_v5

package integration

import gocql "github.com/scylladb/gocql/v2"

// testDialect is the dialect the suite connects with. The dialect is a type,
// so the suite is compiled once per dialect: ScyllaV4 by default, and
// CassandraV4 or CassandraV5 with the dialect_cassandra_v4 or
// dialect_cassandra_v5 build tag (make test-integration-cassandra sets it
// from TEST_CQL_PROTOCOL).
type testDialect = gocql.ScyllaV4

const testDialectDistribution, testDialectProtocol = "scylla", 4
