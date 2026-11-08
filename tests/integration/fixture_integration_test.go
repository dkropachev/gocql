//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	gocql "github.com/scylladb/gocql/v2"

	"github.com/scylladb/gocql/v2/cql"
)

// A nil allowlist cleans the whole owned run after its test process joins. A
// non-nil allowlist is used mid-package only for exact killed/joined children.
func cleanupNamespaces(t *testing.T, allowed []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prefix := os.Getenv("GOCQL_TEST_RUN")
	if prefix == "" {
		t.Skip("requires an owned fixture run")
	}
	owns := func(name string) bool { return name == prefix || strings.HasPrefix(name, prefix+"_") }
	selected := make(map[string]bool, len(allowed))
	for _, name := range allowed {
		if !owns(name) {
			t.Fatalf("cleanup namespace outside owned run: %q", name)
		}
		selected[name] = true
	}
	matches := func(name string) bool { return owns(name) && (allowed == nil || selected[name]) }
	config := createV2Cluster(func(c *gocql.ConfigBuilder) {
		c.WithIgnoreEvents(gocql.SchemaEvent, gocql.TopologyEvent, gocql.StatusEvent).
			WithManualSchemaAgreement().
			NoRefreshAfterAgreement()
	})
	s, err := gocql.Connect(ctx, mustBuild(t, config))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rows, err := v2Query(ctx, s, "SELECT keyspace_name FROM system_schema.keyspaces")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	var row cql.DynamicRow
	for rows.NextInto(&row) {
		value, err := row.At(0)
		if err != nil {
			t.Fatal(err)
		}
		name, err := value.Text()
		if err != nil {
			t.Fatal(err)
		}
		if matches(name.String()) {
			names = append(names, name.String())
		}
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if _, err := v2Exec(ctx, s, "DROP KEYSPACE "+(`"`+strings.ReplaceAll(name, `"`, `""`)+`"`)); err != nil {
			t.Fatal(err)
		}
		t.Log("DROPPED", name)
	}
	if err := s.AwaitSchemaAgreement(ctx); err != nil {
		t.Fatal(err)
	}
	remaining, err := v2Query(ctx, s, "SELECT keyspace_name FROM system_schema.keyspaces")
	if err != nil {
		t.Fatal(err)
	}
	defer remaining.Close()
	for remaining.NextInto(&row) {
		value, err := row.At(0)
		if err != nil {
			t.Fatal(err)
		}
		name, err := value.Text()
		if err != nil {
			t.Fatal(err)
		}
		if matches(name.String()) {
			t.Errorf("owned namespace survived cleanup: %s", name.String())
		}
	}
	if err := remaining.Close(); err != nil {
		t.Fatal(err)
	}

}

func checkAggregate(t *testing.T, s *gocql.Session, keyspace string) {
	t.Helper()
	table := keyspace + ".fixture_values"
	if err := v2CreateTable(context.Background(), s, "CREATE TABLE "+table+" (id int PRIMARY KEY, value double)"); err != nil {
		t.Fatal(err)
	}
	for id, value := range []float64{2, 4, 9} {
		if _, err := v2Exec(context.Background(), s, "INSERT INTO "+table+" (id,value) VALUES (?,?)", cql.IntValue(int32(id)), cql.DoubleValue(value)); err != nil {
			t.Fatal(err)
		}
	}
	row, err := v2ReadOne(context.Background(), s, fmt.Sprintf("SELECT %s.average(value) AS average FROM %s", keyspace, table))
	if err != nil {
		t.Fatal(err)
	}
	value, err := row.At(0)
	if err != nil {
		t.Fatal(err)
	}
	average, err := value.Float()
	if err != nil || average != 5 {
		t.Fatalf("equivalent average = %v, %v", average, err)
	}
	if _, err := v2Exec(context.Background(), s, "DROP TABLE "+table); err != nil {
		t.Fatal(err)
	}
}
