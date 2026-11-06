//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/routing"
)

func v2SchemaRoutingName(t testing.TB) string {
	t.Helper()
	return testKeyspaceName(t, strings.ReplaceAll(cql.MustRandomUUID().String(), "-", ""))
}

func v2ConfigureRouting(t testing.TB, config *gocql.Config, options routing.Options) routing.Policy {
	t.Helper()
	policy, err := routing.NewPolicy(options)
	if err != nil {
		t.Fatal(err)
	}
	profile := *config.DefaultProfile
	profile.Routing = policy
	config.DefaultProfile = &profile
	return policy
}

func v2CreateRoutingKeyspace(t testing.TB, session *gocql.Session, keyspace string, replicas int) {
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

func v2SchemaRoutingExec(ctx context.Context, session *gocql.Session, stmt string, options gocql.Options, values ...cql.Value) (gocql.Result, error) {
	statement := cql.Statement[[]cql.Value, cql.EmptyRecord]{CQL: stmt, Params: cql.DynamicParams, Rows: cql.NoRows}
	prepared, err := gocql.Prepare(ctx, session, statement, gocql.PrepareOptions{})
	if err != nil {
		return gocql.Result{}, err
	}
	bound, err := BindOwned(prepared, values)
	if err != nil {
		return gocql.Result{}, err
	}
	defer bound.Close()
	return Exec(ctx, session, bound, options)
}

func v2SchemaRoutingInt(t testing.TB, row cql.DynamicRow, name string) int32 {
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
