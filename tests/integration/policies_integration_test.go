//go:build integration

package integration

import (
	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/metadata"
	routing "github.com/scylladb/gocql/v2/routing"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	implRouting "github.com/scylladb/gocql/v2/internal/routing"
)

func TestDCValidationTokenAware(t *testing.T) {
	t.Parallel()
	testV2InvalidDatacenter(t, routing.Options{TokenAware: true, Datacenter: "WRONG_DC"})
}

func TestDCValidationDCAware(t *testing.T) {
	t.Parallel()
	testV2InvalidDatacenter(t, routing.Options{Datacenter: "WRONG_DC"})
}

func TestDCValidationRackAware(t *testing.T) {
	t.Parallel()
	testV2InvalidDatacenter(t, routing.Options{Datacenter: "WRONG_DC", Rack: "RACK"})
}

func testV2InvalidDatacenter(t *testing.T, options routing.Options) {
	t.Helper()
	cluster := createV2Cluster()
	v2ConfigureRouting(t, &cluster, options)
	session, err := gocql.Connect(context.Background(), cluster)
	if session != nil {
		session.Close()
	}
	if err == nil {
		t.Fatal("Connect was expected to fail with an unknown datacenter")
	}
	var validation *cql.ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("expected datacenter validation error, got %T: %v", err, err)
	}
}

func TestTokenAwareHostPolicy(t *testing.T) {
	t.Parallel()
	for _, configuredKeyspace := range []bool{true, false} {
		name := "no-keyspace"
		if configuredKeyspace {
			name = "keyspace"
		}
		t.Run(name, func(t *testing.T) {
			bootstrap := createV2Session(t)
			defer bootstrap.Close()
			keyspace := v2SchemaRoutingName(t)
			v2CreateRoutingKeyspace(t, bootstrap, keyspace, 1)
			defer cleanup(t, bootstrap, keyspace)
			cluster := createV2Cluster()
			v2ConfigureRouting(t, &cluster, routing.Options{TokenAware: true})
			if configuredKeyspace {
				cluster.Keyspace = keyspace
			}
			testIfPolicyInitializedProperly(t, cluster, keyspace)
		})
	}
}

func testIfPolicyInitializedProperly(t *testing.T, cluster gocql.Config, keyspace string) {
	t.Helper()
	ctx := context.Background()
	session, err := gocql.Connect(ctx, cluster)
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
	if _, err := v2Exec(ctx, session, fmt.Sprintf("INSERT INTO %s.%s (pk, v) VALUES (?, ?)", keyspace, table), cql.IntValue(7), cql.IntValue(9)); err != nil {
		t.Fatal(err)
	}
	prepared, err := gocql.Prepare(ctx, session, cql.Statement[[]cql.Value, cql.DynamicRow]{CQL: fmt.Sprintf("SELECT pk, v FROM %s.%s WHERE pk = ?", keyspace, table), Params: cql.DynamicParams, Rows: cql.DynamicRows}, gocql.PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindOwned(prepared, []cql.Value{cql.IntValue(7)})
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Close()
	ri := bound.Routing()
	if !ri.Token.Set {
		t.Fatal("prepared partition key has no routing token")
	}
	replicas, err := session.Metadata().Replicas(keyspace, table, ri.Token.Value)
	if err != nil || len(replicas) == 0 {
		t.Fatalf("missing bound token replicas: %v", err)
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
		profile := *cluster.DefaultProfile
		profile.Routing = policy
		handle := profileOf(profile)
		rows, err := Run(ctx, session, bound, testOptions(gocql.OptionValues{Profile: handle, Consistency: cql.Option[cql.Consistency]{Value: cql.One, Set: true}}))
		if err != nil {
			t.Fatal(err)
		}
		if !rows.Next() {
			rows.Close()
			t.Fatalf("missing routed row: %v", rows.Err())
		}
		if v2SchemaRoutingInt(t, rows.Row(), "pk") != 7 || v2SchemaRoutingInt(t, rows.Row(), "v") != 9 {
			rows.Close()
			t.Fatal("unexpected routed row")
		}
		coordinator := rows.Info().HostID
		if rows.Next() {
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

// Keep the all-hosts-down disruption test serialized as in the original suite.
//
//nolint:paralleltest // it brings every host down, so it cannot share the cluster
func TestNoHangAllHostsDown(t *testing.T) {
	session := createV2Session(t)
	defer session.Close()
	hosts := session.Metadata().Hosts()
	if len(hosts) == 0 {
		t.Fatal("no hosts")
	}
	// Match the original fault injection at the actual connection picker too:
	// routing intentionally includes disconnected fallback candidates.
	for _, host := range implCluster.SessionData(session).InternalHostSource.InternalGetHostsList() {
		implEndpoint.InternalHostInfoSetState(host, NodeDown)
		defer implEndpoint.InternalHostInfoSetState(host, NodeUp)
	}
	dc, rack := hosts[0].Datacenter, hosts[0].Rack
	policies := []routing.Options{
		{Datacenter: dc, AllowRemote: true},
		{Datacenter: dc},
		{TokenAware: true, Datacenter: dc, AllowRemote: true},
		{TokenAware: true, Datacenter: dc},
		{Datacenter: dc, Rack: rack, AllowRemote: true},
		{Datacenter: dc, Rack: rack},
		{TokenAware: true, Datacenter: dc, Rack: rack, AllowRemote: true},
		{TokenAware: true, Datacenter: dc, Rack: rack},
		{},
	}
	bound, err := BindUnpreparedOwned(cql.Statement[cql.EmptyParams, cql.DynamicRow]{CQL: "SELECT host_id FROM system.local", Params: cql.NoParams, Rows: cql.DynamicRows}, cql.NewColumns(), cql.EmptyParams{})
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Close()
	for i, options := range policies {
		policy, err := routing.NewPolicy(options)
		if err != nil {
			t.Fatal(err)
		}
		for _, oneHost := range []bool{false, true} {
			profile := *createV2Cluster().DefaultProfile
			profile.Routing = v2UnavailableHostsRouting{Policy: policy, oneHost: oneHost}
			handle := profileOf(profile)
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			rows, queryErr := Run(ctx, session, bound, testOptions(gocql.OptionValues{Profile: handle}))
			if queryErr == nil {
				queryErr = rows.Close()
			}
			if ctx.Err() != nil {
				t.Errorf("policy %d oneHost=%t hung when every host was down: %v", i, oneHost, queryErr)
			}
			if queryErr == nil {
				t.Errorf("policy %d oneHost=%t unexpectedly executed with every host down", i, oneHost)
			}
			cancel()
		}
	}
}
