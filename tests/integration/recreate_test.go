//go:build integration

// Copyright (C) 2017 ScyllaDB

package integration

import (
	"context"
	"errors"
	"flag"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cqlerr"
)

var updateGolden = flag.Bool("update-golden", false, "update golden files")

// cassandraOverride returns override when running against Cassandra and it's set, else base.
func cassandraOverride(base, override string) string {
	if override != "" && *flagDistribution == "cassandra" {
		return override
	}
	return base
}

func isDescribeKeyspaceSupported(t *testing.T, s *gocql.Session) bool {
	t.Helper()

	rows, err := v2Query(context.Background(), s, `DESCRIBE KEYSPACE system WITH INTERNALS`)
	if err == nil {
		err = rows.Close()
	}
	if err != nil {
		var server cqlerr.RequestError
		if errors.As(err, &server) && server.GetCode() == cqlerr.ErrCodeSyntax {
			// DESCRIBE KEYSPACE is not supported on older versions of Cassandra and Scylla
			// For such case schema statement is going to be recreated on the client side
			return false
		}
		t.Fatalf("error querying keyspace schema: %v", err)
	}
	return true
}

func cleanup(t *testing.T, session *gocql.Session, keyspace string) {
	if err := v2CreateTable(context.Background(), session, `DROP KEYSPACE IF EXISTS `+keyspace); err != nil {
		t.Fatalf("unable to drop keyspace: %v", err)
	}
}

func sortQueries(in []string) []string {
	q := trimQueries(in)
	sort.Strings(q)
	return q
}

func trimQueries(in []string) []string {
	queries := in[:0]
	for _, q := range in {
		q = strings.TrimSpace(q)
		if q == "" {
			continue
		}
		if len(q) != 0 {
			queries = append(queries, q)
		}
	}
	return queries
}

var schemaVersion = regexp.MustCompile(` WITH ID = [0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}[ \t\n]+AND`)

func trimSchema(s string) string {
	// Remove temporary items from the scheme, in particular schema version:
	// ) WITH ID = cf0364d0-3b85-11ef-b79d-80a2ee1928c0
	return strings.ReplaceAll(schemaVersion.ReplaceAllString(s, " WITH"), "\n\n", "\n")
}

// Probe server-default tablet behavior in an isolated keyspace using typed rows.
func v2SchemaRoutingTabletsAutoEnabled(t *testing.T, session *gocql.Session) bool {
	t.Helper()
	supported, err := v2TabletsSupported(context.Background(), session)
	if err != nil {
		t.Fatal(err)
	}
	if !supported {
		return false
	}
	keyspace := v2SchemaRoutingName(t)
	v2CreateRoutingKeyspace(t, session, keyspace, 1)
	defer cleanup(t, session, keyspace)
	statements, err := getV2CreateStatements(context.Background(), newSchemaSource(session), keyspace)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Contains(strings.ToLower(string(statements)), "and tablets")
}

// Development group-0 schema transactions can explicitly abort on a concurrent
// topology/schema change. Retry only this server-declared aborted transaction;
// keep all input/dump/golden assertions and propagate every other failure.
func recreateExec(t testing.TB, s *gocql.Session, stmt string) (gocql.Result, error) {
	t.Helper()
	var result gocql.Result
	var err error
	for attempt := 1; attempt <= 4; attempt++ {
		result, err = v2Exec(context.Background(), s, stmt)
		var server cqlerr.RequestError
		if err == nil || !errors.As(err, &server) || server.GetMessage() != "Failed to apply group 0 change due to concurrent modification" || attempt == 4 {
			return result, err
		}
		t.Logf("retry explicitly aborted group-0 fixture transaction, attempt %d", attempt)
		time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
	}
	return result, err
}
