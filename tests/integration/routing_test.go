//go:build integration

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package integration

import (
	stdcmp "cmp"
	"context"
	"fmt"
	"math"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/murmur"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	"github.com/scylladb/gocql/v2/internal/tests"
	"github.com/scylladb/gocql/v2/routing"
)

func TestRouting(t *testing.T) {
	// Keep the all-hosts-down disruption test serialized as in the original suite.
	//
	//nolint:paralleltest // it brings every host down, so it cannot share the cluster
	t.Run("AllHostsDown", func(t *testing.T) {
		session := createV2Session(t)
		defer session.Close()
		hosts := session.Metadata().Hosts()
		if len(hosts) == 0 {
			t.Fatal("no hosts")
		}
		// Match the original fault injection at the actual connection picker too:
		// routing intentionally includes disconnected fallback candidates.
		for _, host := range (*implSession.Session)(session).Data().InternalHostSource.InternalGetHostsList() {
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
		bound, err := BindUnpreparedOwned(cql.Statement[cql.EmptyParams, cql.DynamicRow]{CQL: "SELECT host_id FROM system.local", Params: cql.EmptyParamsEncoder, Rows: cql.DynamicRows}, cql.NewColumns(), cql.EmptyParams{})
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
				builder := gocql.NewProfile()
				fixtureProfile(builder)
				profile, err := builder.WithRouting(v2UnavailableHostsRouting{Policy: policy, oneHost: oneHost}).Build()
				if err != nil {
					t.Fatal(err)
				}
				handle := new(profile)
				ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
				rows, queryErr := Run(ctx, session, bound, bindOptions(session, gocql.Options{Profile: handle}))
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
	})

	t.Run("ClientRoutes", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		session := createV2Session(t)
		defer session.Close()
		keyspace := fixtureKeyspace("")

		table := testTableName(t)
		qualifiedTable := keyspace + "." + table

		if err := v2CreateTable(ctx, session, fmt.Sprintf(`CREATE TABLE %s.%s (
    connection_id uuid,
    host_id uuid,
    Address text,
    port int,
    tls_port int,
    alternator_port int,
    alternator_https_port int,
    Datacenter text,
    Rack text,
    PRIMARY KEY (connection_id, host_id))`, keyspace, table)); err != nil {
			t.Fatal(err)
		}

		var hostIDs []string
		for i := 0; i < 3; i++ {
			hostIDs = append(hostIDs, cql.MustRandomUUID().String())
		}
		var connectionIDs []string
		for i := 0; i < 3; i++ {
			connectionIDs = append(connectionIDs, cql.MustRandomUUID().String())
		}

		// These rows are read back below through session.control, and controlConn.query
		// pins every query it issues to Consistency(One). The session default is Quorum,
		// which acks once 2 of the 3 replicas have the row -- so a ONE read can be served
		// by the third replica before it has been written, and rows go missing. Write the
		// fixture at All so the read cannot outrun replication.
		racks := []string{"rack1", "rack2", "rack3"}
		var expected []clientRoute
		var expectedTLS []clientRoute
		for id, hostID := range hostIDs {
			rack := racks[id]
			ip := net.ParseIP(fmt.Sprintf("127.0.0.%d", id+1))
			for _, connectionID := range connectionIDs {
				prepared, err := session.Prepare(ctx, cql.Statement[[]cql.Value, cql.EmptyRow]{
					CQL: fmt.Sprintf(`INSERT INTO %s (
                    connection_id, host_id, Address, port, tls_port, alternator_port, alternator_https_port, Datacenter, Rack)
                    VALUES (?, ?, ?, 9042, 9142, 0, 0, 'dc1', ?)`, qualifiedTable),
					Kind: cql.OrdinaryStatement, Params: cql.DynamicParams, Rows: cql.EmptyRowDecoder,
				}, gocql.PrepareOptions{})
				if err != nil {
					t.Fatal(err)
				}
				bound, err := BindOwned(prepared, []cql.Value{cql.UUIDValue(codecTestMust(cql.ParseUUID(connectionID))), cql.UUIDValue(codecTestMust(cql.ParseUUID(hostID))), codecTestMust(cql.TextValue(ip.String())), codecTestMust(cql.TextValue(rack))})
				if err != nil {
					t.Fatal(err)
				}
				_, err = Exec(ctx, session, bound, bindOptions(session, gocql.Options{Consistency: cql.Option[cql.Consistency]{Set: true, Value: cql.All}}))
				bound.Close()
				if waitErr := bound.Wait(ctx); waitErr != nil {
					t.Fatal(waitErr)
				}

				if err != nil {
					t.Fatalf("unable to insert connection metadata: %s", err.Error())
				}
				expected = append(expected, clientRoute{
					InternalConnectionID: connectionID,
					InternalHostID:       hostID,
					InternalAddress:      ip.String(),
					InternalPort:         9042,
				})
				expectedTLS = append(expectedTLS, clientRoute{
					InternalConnectionID: connectionID,
					InternalHostID:       hostID,
					InternalAddress:      ip.String(),
					InternalPort:         9142,
				})
			}
		}

		sortClientRoutes(expected)
		sortClientRoutes(expectedTLS)
		expectedFirstHost := filterClientRoutesByHostID(expected, hostIDs[0])

		tcases := []struct {
			name     string
			method   func(controlConnection) ([]clientRoute, error)
			expected []clientRoute
		}{
			{
				name: "get-for-connection-ids",
				method: func(controlConnection) ([]clientRoute, error) {
					return getHostPortMappingForConnectionIDs((*implSession.Session)(session).Data().InternalControl, qualifiedTable, connectionIDs, false)
				},
				expected: expected,
			},
			{
				name: "get-for-pairs",
				method: func(controlConnection) ([]clientRoute, error) {
					return getHostPortMappingForPairs((*implSession.Session)(session).Data().InternalControl, qualifiedTable, makeClientRoutePairs(connectionIDs, hostIDs), false)
				},
				expected: expected,
			},
			{
				name: "get-for-single-host-pairs",
				method: func(controlConnection) ([]clientRoute, error) {
					return getHostPortMappingForPairs((*implSession.Session)(session).Data().InternalControl, qualifiedTable, makeClientRoutePairs(connectionIDs, hostIDs[:1]), false)
				},
				expected: expectedFirstHost,
			},
			{
				name: "get-for-connection-ids-tls",
				method: func(controlConnection) ([]clientRoute, error) {
					return getHostPortMappingForConnectionIDs((*implSession.Session)(session).Data().InternalControl, qualifiedTable, connectionIDs, true)
				},
				expected: expectedTLS,
			},
		}

		for _, tc := range tcases {
			t.Run(tc.name, func(t *testing.T) {
				got, err := tc.method((*implSession.Session)(session).Data().InternalControl)
				if err != nil {
					t.Fatal(err)
				}

				sortClientRoutes(got)

				if diff := cmp.Diff(got, tc.expected, cmp.AllowUnexported(clientRoute{})); diff != "" {
					t.Errorf("got unexpected result: %s", diff)
				}
			})
		}
	})

	t.Run("DCValidation", func(t *testing.T) {
		t.Parallel()

		t.Run("DCAware", func(t *testing.T) {
			t.Parallel()
			testV2InvalidDatacenter(t, routing.Options{Datacenter: "WRONG_DC"})
		})
		t.Run("RackAware", func(t *testing.T) {
			t.Parallel()
			testV2InvalidDatacenter(t, routing.Options{Datacenter: "WRONG_DC", Rack: "RACK"})
		})
		t.Run("TokenAware", func(t *testing.T) {
			t.Parallel()
			testV2InvalidDatacenter(t, routing.Options{TokenAware: true, Datacenter: "WRONG_DC"})
		})
	})

	t.Run("HostFilter", func(t *testing.T) {
		t.Parallel()

		// Ensures that host filtering works even when we discover hosts
		t.Run("Discovery", func(t *testing.T) {
			t.Parallel()

			clusterHosts := getClusterHosts()
			if len(clusterHosts) < 2 {
				t.Skip("skipping because we don't have 2 or more hosts")
			}
			cluster := newFixtureConfig(clusterHosts[:1], 0)
			rr, err := routing.NewPolicy(routing.Options{})
			if err != nil {
				t.Fatal(err)
			}
			cluster.ConfigureDefaultProfile(func(p *gocql.ProfileBuilder) { p.WithRouting(rr) })
			// we'll filter out the second host
			filtered := clusterHosts[1]
			cluster.WithHostFilter(func(host routing.HostSnapshot) bool { return host.Address.Addr().String() != filtered })
			session := createV2SessionFromConfig(cluster, t, false)
			defer session.Close()

			plan := routing.NewPlan(rr, routing.Info{}, session.Metadata())
			targets := make(map[cql.UUID]bool)
			for target, ok := plan.Next(); ok; target, ok = plan.Next() {
				targets[target.HostID()] = true
			}
			tests.AssertEqual(t, "eligible routing hosts", len(clusterHosts)-1, len(targets))
			for _, host := range session.Metadata().Hosts() {
				if host.Address.Addr().String() == filtered && (host.Enabled || targets[host.ID]) {
					t.Fatalf("filtered host remains eligible: %+v", host)
				}
			}
			if ch := (*implSession.Session)(session).Data().InternalControl.InternalGetConn(); ch != nil && ch.InternalHost.ConnectAddress().String() == filtered {
				t.Fatal("control connection selected a filtered host")
			}
		})
		// Ensures that host filtering works for the initial
		// connection including the control connection
		t.Run("Initial", func(t *testing.T) {
			t.Parallel()

			clusterHosts := getClusterHosts()
			if len(clusterHosts) < 2 {
				t.Skip("skipping because we don't have 2 or more hosts")
			}
			cluster := createV2Cluster()
			rr, err := routing.NewPolicy(routing.Options{})
			if err != nil {
				t.Fatal(err)
			}
			cluster.ConfigureDefaultProfile(func(p *gocql.ProfileBuilder) { p.WithRouting(rr) })
			// we'll filter out the second host
			filtered := clusterHosts[1]
			cluster.WithHostFilter(func(host routing.HostSnapshot) bool { return host.Address.Addr().String() != filtered })
			session := createV2SessionFromConfig(cluster, t, false)
			defer session.Close()

			plan := routing.NewPlan(rr, routing.Info{}, session.Metadata())
			targets := make(map[cql.UUID]bool)
			for target, ok := plan.Next(); ok; target, ok = plan.Next() {
				targets[target.HostID()] = true
			}
			tests.AssertEqual(t, "eligible routing hosts", len(clusterHosts)-1, len(targets))
			for _, host := range session.Metadata().Hosts() {
				if host.Address.Addr().String() == filtered && (host.Enabled || targets[host.ID]) {
					t.Fatalf("filtered host remains eligible: %+v", host)
				}
			}
			if ch := (*implSession.Session)(session).Data().InternalControl.InternalGetConn(); ch != nil && ch.InternalHost.ConnectAddress().String() == filtered {
				t.Fatal("control connection selected a filtered host")
			}
		})
	})

	t.Run("RoutingKey", func(t *testing.T) {
		// Issue 475
		t.Run("Bind", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(key varchar PRIMARY KEY,value int)", table))
			type input struct {
				Key   string
				Value int32
			}
			params := cql.Params(cql.Param("key", cql.StringEncoder, func(v input) string { return v.Key }), cql.Param("value", cql.Int32Codec, func(v input) int32 { return v.Value }))
			prepared := cassandraPrepared(t, session, cql.Statement[input, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(key,value) VALUES(?,?)", table), Params: params, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
			require.Equal(t, []int{0}, prepared.Info().PartitionKeyIndexes)
			bound := cassandraBound(t, prepared, input{"routing-key", 5})
			require.True(t, bound.Routing().Token.Set)
			require.Equal(t, murmur.Murmur3H1([]byte("routing-key")), bound.Routing().Token.Value)
			_, err := Exec(context.Background(), session, bound, gocql.CompiledOptions{})
			require.NoError(t, err)
		})
		// Integration test of the routing key calculation
		t.Run("Computed", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t, func(c *gocql.ConfigBuilder) { c.WithIgnoreEvents(gocql.SchemaEvent) })
			defer session.Close()
			single, composite := testTableName(t, "single"), testTableName(t, "composite")
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(first_id int,second_id int,PRIMARY KEY(first_id,second_id))", single))
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(first_id int,second_id int,PRIMARY KEY((first_id,second_id)))", composite))
			runtime := runtimeFor(session)
			runtime.InternalMu.Lock()
			initial := len(runtime.InternalPrepared)
			runtime.InternalMu.Unlock()
			for i, tc := range []struct {
				table   string
				indexes []int
				key     []byte
			}{{single, []int{1}, []byte{0, 0, 0, 2}}, {composite, []int{1, 0}, []byte{0, 4, 0, 0, 0, 2, 0, 0, 4, 0, 0, 0, 1, 0}}} {
				statement := cql.Statement[[]cql.Value, cql.DynamicRow]{CQL: fmt.Sprintf("SELECT * FROM %s WHERE second_id=? AND first_id=?", tc.table), Params: cql.DynamicParams, Rows: cql.DynamicRows}
				prepared := cassandraPrepared(t, session, statement, gocql.PrepareOptions{})
				require.Equal(t, tc.indexes, prepared.Info().PartitionKeyIndexes)
				for _, index := range tc.indexes {
					column, ok := prepared.Parameters().At(index)
					require.True(t, ok)
					require.Equal(t, cql.TypeInt, column.Type.Kind())
				}
				again := cassandraPrepared(t, session, statement, gocql.PrepareOptions{})
				require.Same(t, preparedData(prepared).InternalState, preparedData(again).InternalState, "preparing again must hit typed cache")
				bound := cassandraBound(t, prepared, []cql.Value{cql.IntValue(1), cql.IntValue(2)})
				key, present, err := newPartitionKey(implSession.BoundData(bound).InternalRequest.InternalValues.Cells, prepared.Info().PartitionKeyIndexes)
				require.NoError(t, err)
				require.True(t, present)
				var keyBytes []byte
				require.NoError(t, key.ForEach(func(span cql.Bytes) error { keyBytes = append(keyBytes, span.OwnedBytes()...); return nil }))
				require.Equal(t, tc.key, keyBytes, "canonical single/composite partition-key bytes")
				require.True(t, bound.Routing().Token.Set)
				require.Equal(t, murmur.Murmur3H1(tc.key), bound.Routing().Token.Value)
				runtime.InternalMu.Lock()
				size := len(runtime.InternalPrepared)
				runtime.InternalMu.Unlock()
				require.Equal(t, initial+i+1, size)
			}
		})
		t.Run("OverriddenKeyspace", func(t *testing.T) {
			session := createV2Session(t)
			defer session.Close()
			if (*implSession.Session)(session).Data().InternalCfg.ProtoVersion < 5 {
				t.Skip("This tests only runs on proto > 4 due SetKeyspace availability")
			}
			ks := testKeyspaceName(t)
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE KEYSPACE %s WITH replication={'class':'SimpleStrategy','replication_factor':1}", ks))
			defer cassandraDDL(t, session, "DROP KEYSPACE "+ks)
			for _, keyspace := range []string{fixtureKeyspace(""), ks} {
				cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s.%s(id int PRIMARY KEY)", keyspace, table))
			}
			defer cassandraDDL(t, session, ("DROP TABLE "+fixtureKeyspace("")+".")+table)
			insert := "INSERT INTO " + table + "(id) VALUES(?)"
			selectCQL := "SELECT * FROM " + table + " WHERE id=?"
			for i, keyspace := range []string{fixtureKeyspace(""), ks} {
				write := cassandraWriteBound(t, session, insert, []cql.Value{cql.IntValue(int32(i + 1))}, gocql.PrepareOptions{Keyspace: keyspace})
				require.Equal(t, keyspace, write.Routing().Keyspace)
				require.True(t, write.Routing().Token.Set)
				runtime := runtimeFor(session)
				runtime.InternalMu.Lock()
				found := false
				for key, state := range runtime.InternalPrepared {
					if key.InternalCql == insert && key.InternalKeyspace == keyspace {
						found = true
						require.Equal(t, keyspace, state.InternalInfo.Keyspace)
					}
				}
				runtime.InternalMu.Unlock()
				require.True(t, found, "cache must include keyspace-specific binding")
				read := cassandraDynamicBound(t, session, selectCQL, []cql.Value{cql.IntValue(1)}, gocql.PrepareOptions{Keyspace: keyspace})
				require.Equal(t, keyspace, read.Routing().Keyspace)
				require.True(t, read.Routing().Token.Set)
			}
		})
	})

	t.Run("Tablets", func(t *testing.T) {
		// Token-aware execution must converge on tablet host/shard targets without a
		// server redirection hint, while retaining immutable public routing snapshots.
		t.Run("Converge", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := createV2ClusterWithout(fixtureConsistency)
			v2ConfigureRouting(t, cluster, routing.Options{TokenAware: true})
			// The rows are read back at ONE from whichever replica the tablet
			// routes to: write them at ALL so a lagging replica cannot miss one.
			cluster.ConfigureDefaultProfile(func(p *gocql.ProfileBuilder) { p.WithConsistency(cql.All) })
			session := createV2SessionFromConfig(cluster, t, false)
			defer session.Close()
			supported, err := v2TabletsSupported(ctx, session)
			if err != nil {
				t.Fatal(err)
			}
			if !supported {
				t.Skip("Tablets are not supported by this server")
			}
			table := v2SchemaRoutingName(t)
			if err := v2CreateTable(ctx, session, fmt.Sprintf(`CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))`, table)); err != nil {
				t.Fatal(err)
			}
			for i := int32(0); i < 5; i++ {
				if _, err := v2Exec(ctx, session, fmt.Sprintf(`INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)`, table), cql.IntValue(i), cql.IntValue(i%5), cql.IntValue(i%2)); err != nil {
					t.Fatal(err)
				}
			}
			prepared, err := session.Prepare(ctx, cql.Statement[[]cql.Value, cql.DynamicRow]{CQL: fmt.Sprintf(`SELECT pk, ck, v FROM %s WHERE pk = ?`, table), Params: cql.DynamicParams, Rows: cql.DynamicRows}, gocql.PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			for i := int32(0); i < 5; i++ {
				bound, err := BindOwned(prepared, []cql.Value{cql.IntValue(i)})
				if err != nil {
					t.Fatal(err)
				}
				defer bound.Close()
				// Tablet placement settles asynchronously, and under the parallel suite
				// a 2s budget left only about four attempts (scylladb/gocql#635). The
				// budget bounds waiting, not success: a converged key returns at once.
				startTime, timeout, backoffDelay := time.Now(), 30*time.Second, 100*time.Millisecond
				success := false
				for attempt := 1; time.Since(startTime) < timeout; attempt++ {
					observer := new(v2TabletAttemptObserver)
					options := gocql.Options{Consistency: cql.Option[cql.Consistency]{Value: cql.One, Set: true}, Observer: observer}
					// Warm the cache by visiting existing shard slots. With RF equal to the
					// cluster size and token-aligned tablet shards, every ordinary V1 request
					// can already reach an owner and legitimately receive no feedback.
					cached, _ := session.Metadata().Replicas(bound.Routing().Keyspace, table, bound.Routing().Token.Value)
					if len(cached) == 0 {
						hosts := session.Metadata().Hosts()
						if len(hosts) > 0 && hosts[0].Shards > 1 {
							values := options
							values.HostID = cql.Option[cql.UUID]{Value: hosts[0].ID, Set: true}
							values.ShardID = cql.Option[uint32]{Value: uint32(attempt) % hosts[0].Shards, Set: true}
							options = values
						}
					}
					rows, err := Run(ctx, session, bound, bindOptions(session, options))
					if err != nil {
						t.Fatal(err)
					}
					var row cql.DynamicRow
					if !rows.NextInto(&row) {
						rows.Close()
						t.Fatalf("missing tablet row %d: %v", i, rows.Err())
					}
					if v2SchemaRoutingInt(t, row, "pk") != i || v2SchemaRoutingInt(t, row, "ck") != i%5 || v2SchemaRoutingInt(t, row, "v") != i%2 {
						rows.Close()
						t.Fatal("unexpected tablet row")
					}
					info := rows.Info()
					hinted := false
					for _, entry := range info.CustomPayload() {
						if (entry.Key != "tablets-routing-v1" && entry.Key != "tablets-routing-v2") || !entry.Value.Valid {
							continue
						}
						hinted = true
						tablet, err := parseTypedTabletPayload(entry.Value.Value, bound.Routing().Keyspace, table, entry.Key == "tablets-routing-v2")
						if err != nil {
							rows.Close()
							t.Fatalf("failed to extract tablet information: %v", err)
						}
						t.Logf("Attempt %d: received tablet hint (replicas: %s), backing off %v", attempt, tablet.Replicas(), backoffDelay)
					}
					if rows.NextInto(&row) {
						rows.Close()
						t.Fatal("unexpected additional tablet row")
					}
					if err := rows.Close(); err != nil {
						t.Fatal(err)
					}
					if !hinted {
						snapshot := session.Metadata()
						replicas, err := snapshot.Replicas(bound.Routing().Keyspace, table, bound.Routing().Token.Value)
						if err != nil {
							t.Fatal(err)
						}
						// V1 sends no hint when a cold request happens to hit an owning
						// shard. A cache miss is still warm-up, even without feedback.
						if len(replicas) == 0 {
							remaining := timeout - time.Since(startTime)
							if remaining > 0 {
								time.Sleep(min(backoffDelay, remaining))
							}
							continue
						}
						targets := observer.snapshot()
						if len(targets) == 0 {
							t.Fatal("tablet execution did not record an attempt")
						}
						target := targets[len(targets)-1]
						if target.HostID != info.HostID {
							t.Fatalf("attempt/coordinator mismatch: %v, %v", target, info.HostID)
						}
						if !slices.Contains(replicas, target) || !target.ShardID.Set {
							t.Fatalf("tablet execution target %v is not a shard-specific replica in %v", target, replicas)
						}
						success = true
						break
					}
					remaining := timeout - time.Since(startTime)
					if remaining <= 0 {
						break
					}
					time.Sleep(min(backoffDelay, remaining))
					backoffDelay = min(2*backoffDelay, time.Second)
				}
				if !success {
					t.Fatalf("Timed out after %v (elapsed %v) waiting for tablets to stabilize (migrations still in progress)", timeout, time.Since(startTime))
				}
			}
			snapshot := session.Metadata()
			ranges, err := snapshot.TokenRanges(prepared.Info().Keyspace, table)
			if err != nil || len(ranges) == 0 {
				t.Fatalf("missing tablet ranges: %v", err)
			}
			for _, tokenRange := range ranges {
				for _, token := range []int64{tokenRange.End, tokenRange.Start, math.MinInt64, math.MaxInt64} {
					var expected []routing.Target
					for _, candidate := range ranges {
						if token > candidate.Start && token <= candidate.End || candidate.Start >= candidate.End && (token > candidate.Start || token <= candidate.End) {
							expected = candidate.Replicas
							break
						}
					}
					actual, err := snapshot.Replicas(prepared.Info().Keyspace, table, token)
					if err != nil || !slices.Equal(actual, expected) {
						t.Fatalf("tablet boundary %d: got %v, want %v, error %v", token, actual, expected, err)
					}
				}
			}
			firstRange := ranges[0]
			expectedReplicas := append([]routing.Target(nil), firstRange.Replicas...)
			ranges[0].Replicas[0].ShardID.Value++
			actual, err := snapshot.Replicas(prepared.Info().Keyspace, table, firstRange.End)
			if err != nil || !slices.Equal(actual, expectedReplicas) {
				t.Fatalf("mutating returned range changed snapshot: %v, %v", actual, err)
			}
			if err := session.RefreshMetadata(ctx); err != nil {
				t.Fatal(err)
			}
			actual, err = snapshot.Replicas(prepared.Info().Keyspace, table, firstRange.End)
			if err != nil || !slices.Equal(actual, expectedReplicas) {
				t.Fatalf("refresh changed retained snapshot: %v, %v", actual, err)
			}
		})
		t.Run("Leader", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			config := createV2Cluster()
			v2ConfigureRouting(t, config, routing.Options{TokenAware: true, LeaderAware: true, ShuffleReplicas: true})
			s, err := gocql.Connect(ctx, mustBuild(t, config))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if !(*implSession.Session)(s).Core().GetConn().InternalFramers.InternalDefaults.InternalTabletsRoutingV2 {
				t.Skip("server did not advertise TABLETS_ROUTING_V2_EXPERIMENTAL; source-accurate wire fixtures cover V2 separately")
			}
			ks := testKeyspaceName(t, fmt.Sprintf("tablet_leader_%x", time.Now().UnixNano()))
			if err := v2CreateTable(ctx, s, "CREATE KEYSPACE "+ks+" WITH replication={'class':'NetworkTopologyStrategy','replication_factor':3} AND tablets={'enabled':true} AND consistency='global'"); err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if err := v2CreateTable(cleanup, s, "DROP KEYSPACE "+ks); err != nil {
					t.Error(err)
				}
			}()
			if err := v2CreateTable(ctx, s, "CREATE TABLE "+ks+".records(pk int PRIMARY KEY,v int)"); err != nil {
				t.Fatal(err)
			}
			meta, _ := s.Metadata().Keyspace(ks)
			if !meta.TabletBased || meta.ConsistencyMode != "global" {
				t.Fatal("global tablet metadata absent", meta)
			}
			if _, err := v2Exec(ctx, s, "INSERT INTO "+ks+".records(pk,v) VALUES(1,7)"); err != nil {
				t.Fatal(err)
			}
			p := codecTestMust(s.Prepare(ctx, cql.Statement[int32, cql.DynamicRow]{CQL: "SELECT v FROM " + ks + ".records WHERE pk=?", Params: cql.OneParam(cql.Int32Codec), Rows: cql.DynamicRows}, gocql.PrepareOptions{}))
			b := codecTestMust(BindBorrowed(p, int32(1)))
			defer b.Close()
			var leader cql.UUID
			stable := 0
			feedback := 0
			for attempt := 0; attempt < 256 && stable < 16; attempt++ {
				rows, err := Run(ctx, s, b, bindOptions(s, gocql.Options{Consistency: cql.Option[cql.Consistency]{Value: cql.LocalQuorum, Set: true}}))
				if err != nil {
					t.Fatal(err)
				}
				var row cql.DynamicRow
				if !rows.NextInto(&row) {
					t.Fatal("missing strong row", rows.Err())
				}
				if v2SchemaRoutingInt(t, row, "v") != 7 {
					t.Fatal("bad strong row")
				}
				result := rows.Info()
				moved := false
				for _, entry := range result.CustomPayload() {
					if entry.Key != "tablets-routing-v2" || !entry.Value.Valid {
						continue
					}
					raw := entry.Value.Value.OwnedBytes()
					if len(raw) < 64 {
						t.Fatal("short v2 leader payload")
					}
					copy(leader[:], raw[40:56])
					feedback++
					moved = true
					stable = 0
				}
				for rows.NextInto(&row) {
				}
				if err := rows.Close(); err != nil {
					t.Fatal(err)
				}
				if !leader.IsEmpty() && !moved {
					if result.HostID != leader {
						t.Fatalf("strong read coordinator %s differs from original v2 leader %s", result.HostID, leader)
					}
					stable++
				}
				if moved {
					select {
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(10 * time.Millisecond):
					}
				}
			}
			if feedback == 0 || stable != 16 {
				t.Fatal("V2 leader did not converge", feedback, stable)
			}
			t.Logf("independently decoded leader=%s, feedback=%d, stable LOCAL_QUORUM reads=%d", leader, feedback, stable)
		})
		t.Run("Live", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			config := createV2Cluster()
			v2ConfigureRouting(t, config, routing.Options{TokenAware: true, LeaderAware: true, ShuffleReplicas: true})
			s, err := gocql.Connect(ctx, mustBuild(t, config))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			supported, err := v2TabletsSupported(ctx, s)
			if err != nil {
				t.Fatal(err)
			}
			if !supported {
				t.Skip("server does not provide tablets")
			}
			keyspace := testKeyspaceName(t, fmt.Sprintf("tablet_routing_%x", time.Now().UnixNano()))
			if err := v2CreateTable(ctx, s, "CREATE KEYSPACE "+keyspace+" WITH replication={'class':'NetworkTopologyStrategy','replication_factor':2} AND tablets={'enabled':true}"); err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if err := v2CreateTable(cleanup, s, "DROP KEYSPACE "+keyspace); err != nil {
					t.Error(err)
				}
			}()
			if err := v2CreateTable(ctx, s, "CREATE TABLE "+keyspace+".records (pk int PRIMARY KEY, v int)"); err != nil {
				t.Fatal(err)
			}
			ks, _ := s.Metadata().Keyspace(keyspace)
			if !ks.TabletBased || ks.Tables["records"].ID == "" {
				t.Fatal("server tablet/table UUID metadata absent", ks)
			}
			insert := codecTestMust(s.Prepare(ctx, cql.Statement[int32, cql.EmptyRow]{CQL: "INSERT INTO " + keyspace + ".records(pk,v) VALUES(?,7)", Params: cql.OneParam(cql.Int32Codec), Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{}))
			read := codecTestMust(s.Prepare(ctx, cql.Statement[int32, cql.DynamicRow]{CQL: "SELECT pk,v FROM " + keyspace + ".records WHERE pk=?", Params: cql.OneParam(cql.Int32Codec), Rows: cql.DynamicRows}, gocql.PrepareOptions{}))
			feedback := 0
			for key := int32(0); key < 8; key++ {
				bound := codecTestMust(BindBorrowed(insert, key))
				result, err := Exec(ctx, s, bound, gocql.CompiledOptions{})
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range result.CustomPayload() {
					if entry.Key == "tablets-routing-v1" || entry.Key == "tablets-routing-v2" {
						feedback++
					}
				}
				bound.Close()
			}
			// Read the server's authoritative tablet table independently of feedback.
			id := codecTestMust(cql.ParseUUID(ks.Tables["records"].ID))
			serverRows, err := v2Query(ctx, s, "SELECT last_token,replicas FROM system.tablets WHERE table_id=?", cql.UUIDValue(id))
			if err != nil {
				t.Fatal(err)
			}
			var authoritative []routing.TokenRange
			var row cql.DynamicRow
			for serverRows.NextInto(&row) {
				last := codecTestMust(codecTestMust(row.Get("last_token")).Signed())
				list := codecTestMust(codecTestMust(row.Get("replicas")).Elements())
				r := routing.TokenRange{End: last}
				for list.Next() {
					tuple := codecTestMust(list.Value().Elements())
					if !tuple.Next() {
						t.Fatal("missing host")
					}
					host := codecTestMust(tuple.Value().UUID())
					if !tuple.Next() {
						t.Fatal("missing shard")
					}
					shard := codecTestMust(tuple.Value().Signed())
					r.Replicas = append(r.Replicas, routing.Target{HostID: host, ShardID: cql.Option[uint32]{Value: uint32(shard), Set: true}})
				}
				if err := list.Err(); err != nil {
					t.Fatal(err)
				}
				authoritative = append(authoritative, r)
			}
			if err := serverRows.Err(); err != nil {
				t.Fatal(err)
			}
			t.Logf("initial authoritative tablet read-node=%s", serverRows.Info().HostID)
			serverRows.Close()
			if len(authoritative) == 0 {
				t.Fatal("no authoritative tablet records")
			}
			slices.SortFunc(authoritative, func(a, b routing.TokenRange) int { return stdcmp.Compare(a.End, b.End) })
			for i := range authoritative {
				authoritative[i].Start = math.MinInt64
				if i > 0 {
					authoritative[i].Start = authoritative[i-1].End
				}
			}
			v2 := (*implSession.Session)(s).Core().GetConn().InternalFramers.InternalDefaults.InternalTabletsRoutingV2
			t.Logf("tablet v2 negotiated=%t, server table ID=%s, authoritative ranges=%d", v2, id, len(authoritative))
			for key := int32(0); key < 8; key++ {
				b := codecTestMust(BindBorrowed(read, key))
				token := b.Routing().Token
				if !token.Set {
					t.Fatal("prepared partition key did not produce token")
				}
				var expected []routing.Target
				for _, r := range authoritative {
					if token.Value > r.Start && token.Value <= r.End {
						expected = r.Replicas
						break
					}
				}
				if len(expected) != 2 {
					t.Fatal("server replica count", expected)
				}
				// Tablet placement may move after the initial independent read. Require
				// one exact, non-transitioning authority on every node before and after
				// each query; the initial map is diagnostic, not permanent authority.
				verified := 0
				var latest tabletObservation
				for attempt := 0; attempt < 96 && verified < 8; attempt++ {
					before := tabletAuthority(t, ctx, s, id, token.Value)
					opts := gocql.Options{}
					if attempt < 3 {
						opts = gocql.Options{HostID: cql.Option[cql.UUID]{Value: implRouting.SnapshotData(s.Metadata()).InternalHosts[attempt%len(implRouting.SnapshotData(s.Metadata()).InternalHosts)].ID, Set: true}}
					}
					rows, err := Run(ctx, s, b, bindOptions(s, opts))
					if err != nil {
						t.Fatal(err)
					}
					var row cql.DynamicRow
					if !rows.NextInto(&row) {
						t.Fatal("missing data", rows.Err())
					}
					if v2SchemaRoutingInt(t, row, "pk") != key || v2SchemaRoutingInt(t, row, "v") != 7 {
						t.Fatal("bad row")
					}
					result := rows.Info()
					for _, entry := range result.CustomPayload() {
						if entry.Key == "tablets-routing-v1" || entry.Key == "tablets-routing-v2" {
							feedback++
							hint, parseErr := parseTypedTabletPayload(entry.Value.Value, keyspace, "records", entry.Key == "tablets-routing-v2")
							if parseErr != nil {
								t.Fatal("actual tablet feedback", parseErr)
							}
							t.Logf("key=%d token=%d attempt=%d actual feedback replicas=%s", key, token.Value, attempt, hint.Replicas())
						}
					}
					if rows.NextInto(&row) {
						t.Fatal("extra row")
					}
					if err := rows.Close(); err != nil {
						t.Fatal(err)
					}
					after := tabletAuthority(t, ctx, s, id, token.Value)
					latest = after
					replicas, err := s.Metadata().Replicas(keyspace, "records", token.Value)
					if err != nil {
						t.Fatal(err)
					}
					stable := before.stable && after.stable && before.last == after.last && sameReplicaSet(before.replicas, after.replicas)
					cacheMatches := len(after.replicas) == 2 && sameReplicaSet(replicas, after.replicas)
					coordinatorMatches := false
					for _, replica := range after.replicas {
						coordinatorMatches = coordinatorMatches || replica.HostID == result.HostID
					}
					// Preserve all three cold host pins, including the nonreplica, before
					// requiring eight consecutive unpinned exact-coordinator assertions.
					if attempt >= 3 && stable && cacheMatches && coordinatorMatches {
						verified++
					} else {
						verified = 0
						t.Logf("tablet observation key=%d token=%d attempt=%d initial=%+v before=%+v after=%+v driver=%+v coordinator=%s generation=%d", key, token.Value, attempt, expected, before, after, replicas, result.HostID, s.Metadata().Generation())
					}
					if verified < 8 {
						select {
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						case <-time.After(5 * time.Millisecond):
						}
					}
				}
				if verified != 8 {
					t.Fatalf("key %d did not converge on stable authoritative replicas and eight routed coordinators within 96 total attempts: latest=%+v", key, latest)
				}
				b.Close()
			}
			if feedback == 0 {
				t.Fatal("cold cache did not receive real tablet feedback")
			}
			t.Logf("accepted live feedback responses=%d", feedback)
		})
	})

	t.Run("TokenAware", func(t *testing.T) {
		t.Parallel()

		// Integration test of the token-aware policy-based connection pool
		t.Run("ConnPool", func(t *testing.T) {
			t.Parallel()
			session := createV2SessionFromConfig(createV2ClusterWithout(fixtureConsistency, func(c *gocql.ConfigBuilder) {
				c.WithConnectionsPerHost(1)
				// The row is read back at ONE: write it at ALL so any replica has it.
				c.ConfigureDefaultProfile(func(p *gocql.ProfileBuilder) { p.WithPageSize(1).WithConsistency(cql.All) })
			}), t, false)
			defer session.Close()
			conns := waitForHostConns(t, session)
			require.Len(t, conns, len(session.Metadata().Hosts()))
			table, other := testTableName(t), testTableName(t, "other")
			for _, name := range []string{table, other} {
				cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,data text)", name))
			}
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,data) VALUES(?,?)", table), cql.IntValue(42), codecTestMust(cql.TextValue("8 * 6 =")))
			bound := cassandraDynamicBound(t, session, fmt.Sprintf("SELECT data FROM %s WHERE id=?", table), []cql.Value{cql.IntValue(42)}, gocql.PrepareOptions{})
			require.True(t, bound.Routing().Token.Set)
			require.Equal(t, "8 * 6 =", cassandraText(t, cassandraReadBound(t, session, bound, gocql.Options{Consistency: cql.Option[cql.Consistency]{Value: cql.One, Set: true}}), "data"))
		})
		t.Run("HostPolicy", func(t *testing.T) {
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
					v2ConfigureRouting(t, cluster, routing.Options{TokenAware: true})
					if configuredKeyspace {
						cluster.WithKeyspace(keyspace)
					}
					testIfPolicyInitializedProperly(t, mustBuild(t, cluster), keyspace)
				})
			}
		})
	})
}
