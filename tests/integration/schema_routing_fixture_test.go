//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cql/dynamic"
	"github.com/scylladb/gocql/v2/exec"
	"github.com/scylladb/gocql/v2/routing"
)

func v2SchemaRoutingName(t testing.TB) string {
	t.Helper()
	return testKeyspaceName(t, strings.ReplaceAll(cql.RandomUUID().String(), "-", ""))
}

func v2ConfigureRouting(t testing.TB, cfg *config.ConfigBuilder, options routing.Options) routing.Policy {
	t.Helper()
	policy, err := routing.NewPolicy(options)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConfigureDefaultProfile(func(p *config.ProfileBuilder) { p.WithRouting(policy) })
	return policy
}

func v2CreateRoutingKeyspace(t testing.TB, session *gocql.Session[testDialect], keyspace string, replicas int) {
	t.Helper()
	ctx := context.Background()
	stmt := fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '%d'}", keyspace, replicas)
	supported, err := v2TabletsSupported(ctx, session)
	if err != nil {
		t.Fatal(err)
	}
	if supported {
		stmt += " AND tablets = {'enabled': false}"
	}
	if err := v2CreateTable(ctx, session, stmt); err != nil {
		t.Fatal(err)
	}
}

func v2SchemaRoutingExec(ctx context.Context, session *gocql.Session[testDialect], stmt string, options exec.Options, values ...dynamic.Value) (gocql.Result, error) {
	statement := cql.Statement[[]dynamic.Value, cql.EmptyRow]{CQL: stmt, Params: dynamic.Params, Rows: cql.EmptyRowDecoder}
	prepared, err := session.Prepare(ctx, statement, exec.PrepareOptions{})
	if err != nil {
		return gocql.Result{}, err
	}
	return prepared.Exec(ctx, values, bindOptions(session, options))
}

func v2SchemaRoutingInt(t testing.TB, row dynamic.Row, name string) int32 {
	t.Helper()
	value, err := row.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	result, err := value.Signed()
	if err != nil {
		t.Fatal(err)
	}
	return int32(result)
}
