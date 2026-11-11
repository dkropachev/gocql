//go:build integration

package integration

import (
	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cql/dynamic"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/metadata"
	routing "github.com/scylladb/gocql/v2/routing"

	"context"
	"errors"
	"fmt"
	"testing"

	implRouting "github.com/scylladb/gocql/v2/internal/routing"
)

func testV2InvalidDatacenter(t *testing.T, options routing.Options) {
	t.Helper()
	cluster := createV2Cluster()
	v2ConfigureRouting(t, cluster, options)
	session, err := gocql.Connect[testDialect](context.Background(), mustBuild(t, cluster))
	if session != nil {
		session.Close()
	}
	if err == nil {
		t.Fatal("Connect was expected to fail with an unknown datacenter")
	}
	if _, ok := errors.AsType[*cqlerr.ValidationError](err); !ok {
		t.Fatalf("expected datacenter validation error, got %T: %v", err, err)
	}
}

func testIfPolicyInitializedProperly(t *testing.T, cluster gocql.Config, keyspace string) {
	t.Helper()
	ctx := context.Background()
	session, err := gocql.Connect[testDialect](ctx, cluster)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer session.Close()
	metadata := session.Metadata()
	hosts := metadata.Hosts()
	if len(hosts) == 0 {
		t.Fatal("token-aware metadata has no hosts")
	}
	tokens := 0
	for _, host := range hosts {
		tokens += len(host.Tokens)
	}
	if tokens == 0 {
		t.Fatal("token-aware metadata has no tokens")
	}
	local, err := v2ReadOne(ctx, session, "SELECT partitioner FROM system.local")
	if err != nil {
		t.Fatal(err)
	}
	partitionerValue, err := local.Get("partitioner")
	if err != nil {
		t.Fatal(err)
	}
	partitioner, err := partitionerValue.Text()
	if err != nil || partitioner.String() == "" {
		t.Fatalf("missing partitioner: %v", err)
	}
	// The no-keyspace bootstrap validates its original ring assertions above.
	// Explicit refresh loads schema for the additional qualified-query checks.
	if err := session.RefreshMetadata(ctx); err != nil {
		t.Fatal(err)
	}
	metadata = session.Metadata()
	ranges, err := metadata.TokenRanges(keyspace, "")
	if err != nil || len(ranges) == 0 {
		t.Fatalf("token-aware metadata has no keyspace ranges: %v", err)
	}
	for _, tokenRange := range ranges {
		if len(tokenRange.Replicas) == 0 {
			t.Fatal("token-aware metadata has a range without replicas")
		}
	}
	table := v2SchemaRoutingName(t)
	if err := v2CreateTable(ctx, session, fmt.Sprintf("CREATE TABLE %s.%s (pk int PRIMARY KEY, v int)", keyspace, table)); err != nil {
		t.Fatal(err)
	}
	if _, err := v2Exec(ctx, session, fmt.Sprintf("INSERT INTO %s.%s (pk, v) VALUES (?, ?)", keyspace, table), dynamic.Int(7), dynamic.Int(9)); err != nil {
		t.Fatal(err)
	}
	prepared, err := session.Prepare(ctx, cql.Statement[[]dynamic.Value, dynamic.Row]{CQL: fmt.Sprintf("SELECT pk, v FROM %s.%s WHERE pk = ?", keyspace, table), Params: dynamic.Params, Rows: dynamic.Rows}, gocql.PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	key := []dynamic.Value{dynamic.Int(7)}
	query, err := prepared.Query(gocql.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ri, err := query.Routing(key)
	query.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !ri.Token.Set {
		t.Fatal("prepared partition key has no routing token")
	}
	replicas, err := session.Metadata().Replicas(keyspace, table, ri.Token.Value)
	if err != nil || len(replicas) == 0 {
		t.Fatalf("missing routing token replicas: %v", err)
	}
	for _, options := range []routing.Options{
		{TokenAware: true},
		{TokenAware: true, Datacenter: hosts[0].Datacenter},
		{TokenAware: true, Datacenter: hosts[0].Datacenter, Rack: hosts[0].Rack},
	} {
		policy, err := routing.NewPolicy(options)
		if err != nil {
			t.Fatal(err)
		}
		builder := gocql.NewProfile()
		fixtureProfile(builder)
		profile, err := builder.WithRouting(policy).Build()
		if err != nil {
			t.Fatal(err)
		}
		handle := new(profile)
		rows, err := prepared.Run(ctx, key, bindOptions(session, gocql.Options{Profile: handle, Consistency: cql.Option[cql.Consistency]{Value: cql.One, Set: true}}))
		if err != nil {
			t.Fatal(err)
		}
		var row dynamic.Row
		if !rows.NextInto(&row) {
			rows.Close()
			t.Fatalf("missing routed row: %v", rows.Err())
		}
		if v2SchemaRoutingInt(t, row, "pk") != 7 || v2SchemaRoutingInt(t, row, "v") != 9 {
			rows.Close()
			t.Fatal("unexpected routed row")
		}
		coordinator := rows.Info().HostID
		if rows.NextInto(&row) {
			rows.Close()
			t.Fatal("unexpected additional routed row")
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, replica := range replicas {
			found = found || replica.HostID == coordinator
		}
		if !found {
			t.Fatalf("coordinator %s is not a token replica: %v", coordinator, replicas)
		}
		for _, host := range session.Metadata().Hosts() {
			if host.ID == coordinator && options.Datacenter != "" && host.Datacenter != options.Datacenter {
				t.Fatalf("coordinator is outside configured datacenter: %+v", host)
			}
		}
	}
}

// A test-only metadata input models down/removal events while retaining actual
// query admission and routing. Session metadata snapshots are immutable, so this
// fault injection cannot change shared HostInfo objects used by other sessions.
type v2UnavailableHostsRouting struct {
	routing.Policy
	oneHost bool
}

func (p v2UnavailableHostsRouting) Pick(info routing.Info, metadata *metadata.Snapshot) (routing.Route, bool) {
	return p.Policy.Pick(info, p.unavailable(metadata))
}

func (p v2UnavailableHostsRouting) Fallback(info routing.Info, metadata *metadata.Snapshot) routing.Plan {
	return p.Policy.Fallback(info, p.unavailable(metadata))
}

// unavailable returns metadata with every host down.
func (p v2UnavailableHostsRouting) unavailable(metadata *metadata.Snapshot) *metadata.Snapshot {
	snapshot := *metadata
	implRouting.SnapshotData(&snapshot).InternalHostIndex = nil
	implRouting. // rebuild indices after test-only membership changes
			SnapshotData(&snapshot).InternalHosts = metadata.Hosts()
	for i := range implRouting.SnapshotData(&snapshot).InternalHosts {
		implRouting.SnapshotData(&snapshot).InternalHosts[i].Up = false
	}
	if p.oneHost && len(implRouting.SnapshotData(&snapshot).InternalHosts) > 1 {
		implRouting.SnapshotData(&snapshot).InternalHosts = implRouting.SnapshotData(&snapshot).InternalHosts[:1]
	}
	return &snapshot
}
