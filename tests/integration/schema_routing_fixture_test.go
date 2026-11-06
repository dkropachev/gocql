//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"

	gocql "github.com/scylladb/gocql/v2"
)

func v2SchemaRoutingName(t testing.TB) string {
	t.Helper()
	return testKeyspaceName(t, strings.ReplaceAll(gocql.MustRandomUUID().String(), "-", ""))
}

func v2ConfigureRouting(t testing.TB, config *gocql.Config, options gocql.RoutingOptions) gocql.RoutingPolicy {
	t.Helper()
	policy, err := gocql.NewRoutingPolicy(options)
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
	cql := fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '%d'}", keyspace, replicas)
	supported, err := v2TabletsSupported(ctx, session)
	if err != nil {
		t.Fatal(err)
	}
	if supported {
		cql += " AND tablets = {'enabled': false}"
	}
	if err := v2CreateTable(ctx, session, cql); err != nil {
		t.Fatal(err)
	}
}

func v2SchemaRoutingExec(ctx context.Context, session *gocql.Session, cql string, options gocql.Options, values ...gocql.Value) (gocql.Result, error) {
	statement := gocql.Statement[[]gocql.Value, gocql.Unit]{CQL: cql, Params: gocql.DynamicParams, Rows: gocql.NoRows}
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

func v2SchemaRoutingInt(t testing.TB, row gocql.DynamicRow, name string) int32 {
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
