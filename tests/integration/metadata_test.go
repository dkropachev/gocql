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
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
	"github.com/scylladb/gocql/v2/internal/tests"
	"github.com/scylladb/gocql/v2/routing"
)

func TestMetadata(t *testing.T) {
	t.Run("Control", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		contactPoints := getClusterHosts()
		config := createV2Cluster(func(c *gocql.ConfigBuilder) {
			c.NoDiscovery().WithConnectionsPerHost(1).NoAdvancedShardAwareness()
		})
		s, err := gocql.Connect(ctx, mustBuild(t, config))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		identities := map[cql.UUID]bool{}
		for _, host := range s.Metadata().Hosts() {
			if host.ID.IsEmpty() || identities[host.ID] {
				t.Fatal("invalid configured contact identities", s.Metadata().Hosts())
			}
			identities[host.ID] = true
		}
		if len(identities) != len(contactPoints) {
			t.Fatal("configured contacts collapsed", s.Metadata().Hosts())
		}
		if err := s.AwaitSchemaAgreement(ctx); err != nil {
			t.Fatal(err)
		}
		keyspace := fmt.Sprintf("metadata_control_%x", time.Now().UnixNano())
		sub, err := s.Subscribe(gocql.EventFilter{Kinds: []gocql.EventKind{gocql.SchemaEvent}, Keyspace: keyspace, Capacity: 64})
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
		before := s.Metadata() // subscribe-before-snapshot, then discard <= generation
		conn, err := implCluster.InternalSessionSelectTypedConn(implExecution.InternalCore((*implExecution.Session)(s)), ctx, routing.Info{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		exec := func(stmt string) {
			t.Helper()
			b, err := BindUnpreparedOwned(cql.Command(stmt), cql.NewColumns(), cql.EmptyParams{})
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			if _, err := Exec(ctx, s, b, bindOptions(s, *func() *gocql.Options {
				opaqueStorageValue := new(gocql.Options)
				implExecution.OptionsData(opaqueStorageValue).InternalConnection = conn
				return opaqueStorageValue
			}())); err != nil {
				t.Fatal(err)
			}
		}
		exec("CREATE KEYSPACE " + keyspace + " WITH replication={'class':'NetworkTopologyStrategy','replication_factor':1}")
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			b, err := BindUnpreparedOwned(cql.Command("DROP KEYSPACE IF EXISTS "+keyspace), cql.NewColumns(), cql.EmptyParams{})
			if err == nil {
				defer b.Close()
				_, err = Exec(cleanup, s, b, bindOptions(s, *func() *gocql.Options {
					opaqueStorageValuex := new(gocql.Options)
					implExecution.OptionsData(opaqueStorageValuex).InternalConnection = conn
					return opaqueStorageValuex
				}()))
			}
			if err != nil {
				t.Error("cleanup unique keyspace", err)
			}
		}()
		exec("CREATE TYPE " + keyspace + ".record (value text)")
		exec("CREATE TABLE " + keyspace + ".records (id int PRIMARY KEY,value frozen<record>)")
		if err := s.RefreshMetadata(ctx); err != nil {
			t.Fatal(err)
		}
		snapshot := s.Metadata()
		schema, ok := snapshot.Keyspace(keyspace)
		if !ok || schema.Tables["records"] == nil || schema.Types["record"] == nil {
			t.Fatal("typed explicit refresh omitted schema", schema)
		}
		if _, ok := before.Keyspace(keyspace); ok {
			t.Fatal("retained prior snapshot mutated")
		}
		deadline := time.NewTimer(15 * time.Second)
		defer deadline.Stop()
		seen := false
		for !seen {
			select {
			case event, ok := <-sub.Events():
				if !ok {
					t.Fatal("unexpected event overflow")
				}
				if event.Generation <= before.Generation() {
					continue
				}
				if event.Target == "TABLE" && event.Object == "records" && event.Change == "CREATED" {
					current, ok := s.Metadata().Keyspace(keyspace)
					if !ok || current.Tables["records"] == nil {
						t.Fatal("event preceded snapshot publication", event)
					}
					seen = true
				}
			case <-deadline.C:
				t.Fatal("schema event not delivered")
			}
		}
		exec("DROP TABLE " + keyspace + ".records")
		if err := s.RefreshMetadata(ctx); err != nil {
			t.Fatal(err)
		}
		if current, _ := s.Metadata().Keyspace(keyspace); current.Tables["records"] != nil {
			t.Fatal("dropped table resurrected")
		}
		if retained, _ := snapshot.Keyspace(keyspace); retained.Tables["records"] == nil {
			t.Fatal("retained complete snapshot changed after drop")
		}
	})

	t.Run("Events", func(t *testing.T) {
		t.Run("ControlReconnect", func(t *testing.T) {
			t.Parallel()

			cluster := createV2Cluster().WithIgnoreEvents(gocql.TopologyEvent, gocql.StatusEvent)

			sess, err := gocql.Connect(context.Background(), mustBuild(t, cluster))
			if err != nil {
				t.Fatalf("unable to create session: %v", err)
			}
			defer sess.Close()

			sub, err := sess.Subscribe(gocql.EventFilter{Kinds: []gocql.EventKind{gocql.ControlEvent}, Capacity: 10})
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()

			if err := implExecution.InternalSessionData((*implExecution.Session)(sess)).InternalControl.InternalReconnect(); err != nil {
				t.Fatalf("control reconnect: %v", err)
			}

			select {
			case ev := <-sub.Events():
				if ev.Kind != gocql.ControlEvent || ev.Change != "RECONNECTED" || ev.HostID.IsEmpty() || !ev.Address.IsValid() {
					t.Fatalf("unexpected reconnect event: %+v", ev)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("timeout waiting for control reconnect event")
			}
		})
		// WARNING: This test must NOT use t.Parallel(). It listens for schema events
		// and concurrent DDL from parallel tests could cause spurious matches.
		//
		//nolint:paralleltest // listens for schema events from the global control connection
		t.Run("SchemaChange", func(t *testing.T) {
			cluster := createV2Cluster()

			sess, err := gocql.Connect(context.Background(), mustBuild(t, cluster))
			if err != nil {
				t.Fatalf("unable to create session: %v", err)
			}
			defer sess.Close()

			keyspace := fmt.Sprintf("eventbus_schema_%d", time.Now().UnixNano())

			// Filter events to the specific keyspace this test creates, so that
			// concurrent DDL from parallel tests does not cause spurious matches.
			sub, err := sess.Subscribe(gocql.EventFilter{Kinds: []gocql.EventKind{gocql.SchemaEvent}, Keyspace: keyspace, Capacity: 10})
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()

			createStmt := fmt.Sprintf(`CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}`, keyspace)
			if _, err := v2Exec(context.Background(), sess, createStmt); err != nil {
				t.Fatalf("create keyspace: %v", err)
			}
			defer func() {
				if _, err := v2Exec(context.Background(), sess, "DROP KEYSPACE "+keyspace); err != nil {
					t.Error(err)
				}
			}()

			select {
			case ev := <-sub.Events():
				if ev.Kind != gocql.SchemaEvent || ev.Target != "KEYSPACE" || ev.Keyspace != keyspace || ev.Change != "CREATED" {
					t.Fatalf("unexpected schema event: %+v", ev)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("timeout waiting for schema change event")
			}
		})
	})

	t.Run("Hosts", func(t *testing.T) {
		t.Parallel()

		t.Run("FromSystem", func(t *testing.T) {
			t.Parallel()

			clusterHosts := getClusterHosts()
			cluster := createV2Cluster()
			session := createV2SessionFromConfig(cluster, t, false)
			defer session.Close()
			if err := session.RefreshMetadata(context.Background()); err != nil {
				t.Fatal(err)
			}
			hosts := session.Metadata().Hosts()
			row, err := v2ReadOne(context.Background(), session, "SELECT partitioner FROM system.local")
			if err != nil {
				t.Fatal(err)
			}
			value, err := row.At(0)
			if err != nil {
				t.Fatal(err)
			}
			partitioner, err := value.Text()
			tests.AssertTrue(t, "err == nil", err == nil)
			tests.AssertEqual(t, "len(hosts)", len(clusterHosts), len(hosts))
			tests.AssertTrue(t, "len(partitioner) != 0", len(partitioner.String()) != 0)
		})
		t.Run("InvalidPeer", func(t *testing.T) {
			t.Parallel()
			if *flagDistribution != "cassandra" {
				t.Skip("incomplete-peer wire fixture uses Cassandra's native v4 metadata")
			}
			if *flagRunSslTest {
				// The fixture rewrites plaintext protocol bytes, so it needs a cluster
				// that accepts unencrypted clients; with -runssl the server resets them.
				t.Skip("incomplete-peer wire fixture needs a plaintext native transport")
			}
			fixture := &incompletePeerDialer{}
			// TLS is never set here: the test skips under -runssl above.
			config := createV2ClusterWithout(fixtureProtocol|fixtureCompression, func(c *gocql.ConfigBuilder) {
				c.WithProtocolVersion(gocql.Protocol4).WithCompression(gocql.NoCompression).WithDialer(fixture)
			})
			session, err := gocql.Connect(context.Background(), mustBuild(t, config))
			require.NoError(t, err)
			defer session.Close()
			require.Positive(t, fixture.injected.Load(), "discovery must consume injected incomplete peer")
			hosts := session.Metadata().Hosts()
			require.Len(t, hosts, *clusterSize)
			for _, host := range hosts {
				require.NotEqual(t, "35c0ec48-5109-40fd-9281-9e9d4add2f1e", host.ID.String())
			}
			require.Equal(t, "local", cassandraText(t, cassandraRead(t, session, "SELECT key FROM system.local"), "key"))
		})
		// Makes sure that you can autodiscover other cluster members
		// when you seed a cluster config with just one node
		t.Run("RingDiscovery", func(t *testing.T) {
			t.Parallel()

			clusterHosts := getClusterHosts()
			cluster := newFixtureConfig(clusterHosts[:1], 0)

			session := createV2SessionFromConfig(cluster, t, false)
			defer session.Close()

			if *clusterSize > 1 {
				// wait for autodiscovery to update the pool with the list of known hosts
				time.Sleep(*flagAutoWait)
			}
			implExecution.InternalSessionData((*implExecution.Session)(session)).InternalPool.InternalMu.RLock()
			defer implExecution.InternalSessionData((*implExecution.Session)(session)).InternalPool.InternalMu.RUnlock()
			size := len(implExecution.InternalSessionData((*implExecution.Session)(session)).InternalPool.InternalHostConnPools)

			if *clusterSize != size {
				for p, pool := range implExecution.InternalSessionData((*implExecution.Session)(session)).InternalPool.InternalHostConnPools {
					t.Logf("p=%q host=%v ips=%s", p, pool.InternalHost, pool.InternalHost.ConnectAddress().String())

				}
				t.Errorf("Expected a cluster size of %d, but actual size was %d", *clusterSize, size)
			}
		})
	})

	t.Run("Keyspace", func(t *testing.T) {
		t.Parallel()

		// Integration test of just querying for data from the system.schema_keyspace table where the keyspace DOES exist.
		t.Run("Exists", func(t *testing.T) {
			t.Parallel()

			session := createV2Session(t)
			defer session.Close()

			keyspaceMetadata, err := cassandraKeyspace(session, fixtureKeyspace(""))
			if err != nil {
				t.Fatalf("failed to query the keyspace metadata with err: %v", err)
			}
			if keyspaceMetadata == nil {
				t.Fatal("failed to query the keyspace metadata, nil returned")
			}
			if keyspaceMetadata.Name != fixtureKeyspace("") {
				t.Errorf("Expected keyspace name to be 'gocql' but was '%s'", keyspaceMetadata.Name)
			}
			if keyspaceMetadata.StrategyClass != "org.apache.cassandra.locator.NetworkTopologyStrategy" {
				t.Errorf("Expected replication strategy class to be 'org.apache.cassandra.locator.NetworkTopologyStrategy' but was '%s'", keyspaceMetadata.StrategyClass)
			}
			if keyspaceMetadata.StrategyOptions == nil {
				t.Error("Expected replication strategy options map but was nil")
			}
			rfStr, ok := keyspaceMetadata.StrategyOptions["datacenter1"]
			if !ok {
				t.Fatalf("Expected strategy option 'datacenter1' but was not found in %v", keyspaceMetadata.StrategyOptions)
			}
			rfInt, err := strconv.Atoi(rfStr.(string))
			if err != nil {
				t.Fatalf("Error converting string to int with err: %v", err)
			}
			if rfInt != *flagRF {
				t.Errorf("Expected replication factor to be %d but was %d", *flagRF, rfInt)
			}
		})
		// Integration test of just querying for data from the system.schema_keyspace table where the keyspace DOES NOT exist.
		t.Run("Missing", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			_, found := session.Metadata().Keyspace("gocql_keyspace_does_not_exist")
			require.False(t, found, "snapshot must report a missing keyspace")
		})
		t.Run("TokenAwareSession", func(t *testing.T) {
			t.Parallel()

			cluster := createV2Cluster()

			v2ConfigureRouting(t, cluster, routing.Options{TokenAware: true})

			session := createV2SessionFromConfig(cluster, t, false)
			defer session.Close()

			keyspaceMetadata, ok := session.Metadata().Keyspace(fixtureKeyspace(""))
			if !ok {
				t.Fatal("keyspace missing from typed metadata snapshot")
			}
			tests.AssertTrue(t, "keyspace present in metadata snapshot", keyspaceMetadata.Name == fixtureKeyspace(""))
		})
	})

	t.Run("Recreate", func(t *testing.T) {
		t.Parallel()

		failsOnOldScylla := false
		if *flagDistribution == "scylla" && flagCassVersion.Before(2024, 0, 0) {
			failsOnOldScylla = true
		}

		session := createV2SessionFromConfig(createV2Cluster(), t, true)
		defer session.Close()

		getStmtFromCluster := isDescribeKeyspaceSupported(t, session)
		tabletsAutoEnabled := v2SchemaRoutingTabletsAutoEnabled(t, session)

		tcs := []struct {
			Name            string
			FixedKeyspace   string // original keyspace name used in .cql files
			FailWithTablets bool
			Input           string
			CassandraInput  string // overrides Input on Cassandra, when its CQL syntax requirements differ
			Golden          string
			CassandraGolden string // overrides Golden on Cassandra, when the rendered dump differs
		}{
			{
				Name:          "Keyspace",
				FixedKeyspace: "gocqlx_keyspace",
				Input:         "../../testdata/recreate/keyspace.cql",
				Golden:        "../../testdata/recreate/keyspace_golden.cql",
			},
			{
				Name:           "Table",
				FixedKeyspace:  "gocqlx_table",
				Input:          "../../testdata/recreate/table.cql",
				CassandraInput: "../../testdata/recreate/table_cassandra.cql",
				Golden:         "../../testdata/recreate/table_golden.cql",
			},
			{
				Name:            "Materialized Views",
				FixedKeyspace:   "gocqlx_mv",
				FailWithTablets: failsOnOldScylla,
				Input:           "../../testdata/recreate/materialized_views.cql",
				// Cassandra requires every MV primary key column not in the SELECT list
				// to be explicitly restricted with IS NOT NULL; Scylla does not.
				CassandraInput: "../../testdata/recreate/materialized_views_cassandra.cql",
				Golden:         "../../testdata/recreate/materialized_views_golden.cql",
				// Cassandra's rendered dump includes the extra IS NOT NULL restriction above.
				CassandraGolden: "../../testdata/recreate/materialized_views_golden_cassandra.cql",
			},
			{
				Name:            "Index",
				FixedKeyspace:   "gocqlx_idx",
				FailWithTablets: failsOnOldScylla,
				Input:           "../../testdata/recreate/index.cql",
				Golden:          "../../testdata/recreate/index_golden.cql",
			},
			{
				Name:            "Secondary Index",
				FixedKeyspace:   "gocqlx_sec_idx",
				FailWithTablets: failsOnOldScylla,
				Input:           "../../testdata/recreate/secondary_index.cql",
				// Cassandra's CQL grammar doesn't accept Scylla's multi-column
				// index target syntax "((location), name)"; use a single-column index.
				CassandraInput:  "../../testdata/recreate/secondary_index_cassandra.cql",
				Golden:          "../../testdata/recreate/secondary_index_golden.cql",
				CassandraGolden: "../../testdata/recreate/secondary_index_cassandra_golden.cql",
			},
			{
				Name:          "UDT",
				FixedKeyspace: "gocqlx_udt",
				Input:         "../../testdata/recreate/udt.cql",
				Golden:        "../../testdata/recreate/udt_golden.cql",
			},
			{
				Name:            "Aggregates",
				CassandraInput:  "../../testdata/recreate/aggregates_cassandra.cql",
				CassandraGolden: "../../testdata/recreate/aggregates_cassandra_golden.cql",
				FixedKeyspace:   "gocqlx_aggregates",
				Input:           "../../testdata/recreate/aggregates.cql",
				Golden:          "../../testdata/recreate/aggregates_golden.cql",
			},
		}

		for i := range tcs {
			test := tcs[i]
			t.Run(test.Name, func(t *testing.T) {
				if test.Name == "UDT" && *flagDistribution == "scylla" && flagCassVersion.Major == 2024 && flagCassVersion.Minor == 1 {
					t.Skip("Doesn't work properly on Scylla 2024.1 due to https://github.com/scylladb/scylladb/issues/26761")
				}

				// Generate a unique keyspace name to avoid collisions under parallel execution.
				// Replace the fixed keyspace name in CQL input/golden files with the unique name.
				ks := fixtureKeyspace("_agg_") + strings.ReplaceAll(cql.MustRandomUUID().String(), "-", "")[:12]
				cleanup(t, session, ks)
				t.Cleanup(func() { cleanup(t, session, ks) })

				input := cassandraOverride(test.Input, test.CassandraInput)
				in, err := os.ReadFile(input)
				if err != nil {
					t.Fatal(err)
				}

				// Substitute the fixed keyspace name in the CQL input with the unique name.
				inStr := strings.ReplaceAll(string(in), test.FixedKeyspace, ks)

				queries := trimQueries(splitFixtureCQL(inStr))
				for _, q := range queries {
					_, err = recreateExec(t, session, q)
					if err != nil {
						break
					}
				}

				if tabletsAutoEnabled && test.FailWithTablets {
					if err == nil {
						t.Errorf("did not get expected error or tablets")
					} else if strings.Contains(err.Error(), "not supported") && strings.Contains(err.Error(), "tablets") {
						return
					} else {
						t.Fatal("query failed with unexpected error", err)
					}
				} else if err != nil {
					t.Fatal("invalid input query", err)
				}

				if err := session.AwaitSchemaAgreement(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := session.RefreshMetadata(context.Background()); err != nil {
					t.Fatal(err)
				}
				km, ok := session.Metadata().Keyspace(ks)
				if !ok {
					t.Fatal("keyspace missing from typed metadata snapshot:", ks)
				}
				dump, err := km.ToCQL()
				if err != nil {
					t.Fatal("recreate schema", err)
				}

				dump = trimSchema(dump)

				// Normalize the dump back to the fixed keyspace name for comparison with golden files.
				dump = strings.ReplaceAll(dump, ks, test.FixedKeyspace)

				var golden []byte
				if getStmtFromCluster {
					golden, err = getV2CreateStatements(context.Background(), newSchemaSource(session), ks)
					if err != nil {
						t.Fatal(err)
					}
					golden = []byte(trimSchema(string(golden)))
					// Normalize from the cluster's unique keyspace name back to fixed name.
					golden = []byte(strings.ReplaceAll(string(golden), ks, test.FixedKeyspace))
				} else {
					goldenPath := cassandraOverride(test.Golden, test.CassandraGolden)
					if *updateGolden {
						if err := os.WriteFile(goldenPath, []byte(dump), 0644); err != nil {
							t.Fatal(err)
						}
					}
					golden, err = os.ReadFile(goldenPath)
					if err != nil {
						t.Fatal(err)
					}
					golden = []byte(trimSchema(string(golden)))
				}

				goldenQueries := trimQueries(sortQueries(splitFixtureCQL(string(golden))))
				dumpQueries := trimQueries(sortQueries(splitFixtureCQL(dump)))

				if len(goldenQueries) != len(dumpQueries) {
					t.Fatalf("Expected len(dumpQueries) to be %d, got %d", len(goldenQueries), len(dumpQueries))
				}
				// Compare with golden
				for i, dq := range dumpQueries {
					gq := goldenQueries[i]

					if diff := cmp.Diff(gq, dq); diff != "" {
						t.Errorf("dumpQueries[%d] diff\n%s", i, diff)
					}
				}

				if test.Name == "Aggregates" {
					checkAggregate(t, session, ks)
				}

				// Exec dumped queries to check if they are CQL-correct.
				// Substitute the fixed keyspace name back to the unique name for execution.
				cleanup(t, session, ks)

				for _, q := range trimQueries(splitFixtureCQL(strings.ReplaceAll(dump, test.FixedKeyspace, ks))) {
					if _, err := recreateExec(t, session, q); err != nil {
						t.Fatal("invalid dump query", q, err)
					}
				}

				if test.Name == "Aggregates" {
					checkAggregate(t, session, ks)
				}

				// Check if new dump is the same as previous
				err = session.AwaitSchemaAgreement(context.Background())
				if err != nil {
					t.Fatal("failed to await for schema agreement", err)
				}
				err = session.RefreshMetadata(context.Background())
				if err != nil {
					t.Fatal("failed to read schema for keyspace", err)
				}
				km, ok = session.Metadata().Keyspace(ks)
				if !ok {
					t.Fatal("recreated keyspace missing from typed metadata snapshot:", ks)
				}
				secondDump, err := km.ToCQL()
				if err != nil {
					t.Fatal("recreate schema", err)
				}

				secondDump = trimSchema(secondDump)
				// Normalize the second dump back to fixed keyspace name for comparison.
				secondDump = strings.ReplaceAll(secondDump, ks, test.FixedKeyspace)

				secondDumpQueries := trimQueries(sortQueries(splitFixtureCQL(secondDump)))

				if !cmp.Equal(secondDumpQueries, dumpQueries) {
					t.Errorf("first dump and second one differs: %s", cmp.Diff(secondDumpQueries, dumpQueries))
				}
			})
		}
	})

	t.Run("Session", func(t *testing.T) {
		t.Parallel()

		session := createV2Session(t)
		defer session.Close()

		ks := fixtureKeyspace("")
		tabletsSupported, err := v2TabletsSupported(context.Background(), session)
		require.NoError(t, err)

		if _, err := cassandraKeyspace(session, ks); err != nil {
			t.Fatalf("failed to get initial keyspace metadata: %v", err)
		}

		waitForSchemaRefresh := func() {
			if err := session.AwaitSchemaAgreement(context.Background()); err != nil {
				t.Logf("schema agreement warning: %v", err)
			}
			require.NoError(t, session.RefreshMetadata(context.Background()))
		}

		t.Run("TableMetadata", func(t *testing.T) {
			t.Run("basic_table_after_create", func(t *testing.T) {
				table := testTableName(t)
				if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
					"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int)", ks, table)); err != nil {
					t.Fatalf("create table: %v", err)
				}
				defer v2Exec(context.Background(), session, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table))

				waitForSchemaRefresh()

				tm, err := cassandraTable(session, ks, table)
				if err != nil {
					t.Fatalf("TableMetadata failed: %v", err)
				}
				if tm.Name != table {
					t.Errorf("expected table name %q, got %q", table, tm.Name)
				}
				if tm.Keyspace != ks {
					t.Errorf("expected keyspace %q, got %q", ks, tm.Keyspace)
				}
			})

			t.Run("columns_and_partition_key", func(t *testing.T) {
				table := testTableName(t)
				if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
					"CREATE TABLE IF NOT EXISTS %s.%s (pk1 int, pk2 text, ck int, val blob, PRIMARY KEY ((pk1, pk2), ck))", ks, table)); err != nil {
					t.Fatalf("create table: %v", err)
				}
				defer v2Exec(context.Background(), session, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table))

				waitForSchemaRefresh()

				tm, err := cassandraTable(session, ks, table)
				if err != nil {
					t.Fatalf("TableMetadata failed: %v", err)
				}

				if len(tm.PartitionKey) != 2 {
					t.Fatalf("expected 2 partition key columns, got %d", len(tm.PartitionKey))
				}
				if tm.PartitionKey[0].Name != "pk1" || tm.PartitionKey[1].Name != "pk2" {
					t.Errorf("unexpected partition key columns: %v, %v", tm.PartitionKey[0].Name, tm.PartitionKey[1].Name)
				}

				if len(tm.ClusteringColumns) != 1 || tm.ClusteringColumns[0].Name != "ck" {
					t.Errorf("expected clustering column 'ck', got %v", tm.ClusteringColumns)
				}

				for _, col := range []string{"pk1", "pk2", "ck", "val"} {
					if _, ok := tm.Columns[col]; !ok {
						t.Errorf("expected column %q in metadata", col)
					}
				}
			})

			t.Run("with_secondary_index", func(t *testing.T) {
				if tabletsSupported {
					t.Skip("secondary indexes are not supported on tables with tablets")
				}

				table := testTableName(t)
				if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
					"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int)", ks, table)); err != nil {
					t.Fatalf("create table: %v", err)
				}
				defer v2Exec(context.Background(), session, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table))

				idxName := table + "_v_idx"
				if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
					"CREATE INDEX IF NOT EXISTS %s ON %s.%s (v)", idxName, ks, table)); err != nil {
					t.Fatalf("create index: %v", err)
				}

				waitForSchemaRefresh()

				require.NoError(t, session.RefreshMetadata(context.Background()))
				km, err := cassandraKeyspace(session, ks)
				if err != nil {
					t.Fatalf("KeyspaceMetadata failed: %v", err)
				}
				if _, ok := km.Indexes[idxName]; !ok {
					t.Errorf("expected index %q in keyspace metadata indexes", idxName)
				}
			})

			t.Run("with_materialized_view", func(t *testing.T) {
				if flagCassVersion.Before(3, 0, 0) {
					t.Skip("materialized views require Cassandra 3.0+")
				}
				if tabletsSupported {
					t.Skip("materialized views are not supported on tables with tablets")
				}

				baseTable := testTableName(t, "base")
				viewName := testTableName(t, "view")
				if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
					"CREATE TABLE IF NOT EXISTS %s.%s (pk int, ck int, v int, PRIMARY KEY (pk, ck))", ks, baseTable)); err != nil {
					t.Fatalf("create base table: %v", err)
				}
				defer v2Exec(context.Background(), session, fmt.Sprintf("DROP MATERIALIZED VIEW IF EXISTS %s.%s", ks, viewName))
				defer v2Exec(context.Background(), session, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, baseTable))

				if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
					"CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s AS SELECT pk, ck, v FROM %s.%s WHERE pk IS NOT NULL AND ck IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, pk, ck)",
					ks, viewName, ks, baseTable)); err != nil {
					t.Fatalf("create materialized view: %v", err)
				}

				waitForSchemaRefresh()

				tm, err := cassandraTable(session, ks, baseTable)
				if err != nil {
					t.Fatalf("TableMetadata for base table failed: %v", err)
				}
				if tm.Name != baseTable {
					t.Errorf("expected table name %q, got %q", baseTable, tm.Name)
				}

				require.NoError(t, session.RefreshMetadata(context.Background()))
				km, err := cassandraKeyspace(session, ks)
				if err != nil {
					t.Fatalf("KeyspaceMetadata failed: %v", err)
				}
				if _, ok := km.Views[viewName]; !ok {
					t.Errorf("expected view %q in keyspace metadata", viewName)
				}
				if km.Views[viewName].BaseTableName != baseTable {
					t.Errorf("expected view base table %q, got %q", baseTable, km.Views[viewName].BaseTableName)
				}
			})

			t.Run("after_alter_table", func(t *testing.T) {
				table := testTableName(t)
				if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
					"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int)", ks, table)); err != nil {
					t.Fatalf("create table: %v", err)
				}
				defer v2Exec(context.Background(), session, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table))

				if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
					"ALTER TABLE %s.%s ADD v2 text", ks, table)); err != nil {
					t.Fatalf("alter table: %v", err)
				}

				waitForSchemaRefresh()

				tm, err := cassandraTable(session, ks, table)
				if err != nil {
					t.Fatalf("TableMetadata failed: %v", err)
				}
				if _, ok := tm.Columns["v2"]; !ok {
					t.Errorf("expected column 'v2' after ALTER TABLE, got columns: %v", columnNames(tm.Columns))
				}
			})

			t.Run("after_drop_and_recreate", func(t *testing.T) {
				table := testTableName(t)
				if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
					"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int)", ks, table)); err != nil {
					t.Fatalf("create table: %v", err)
				}

				waitForSchemaRefresh()

				if _, err := cassandraTable(session, ks, table); err != nil {
					t.Fatalf("TableMetadata before drop failed: %v", err)
				}

				if err := v2CreateTable(context.Background(), session, fmt.Sprintf("DROP TABLE %s.%s", ks, table)); err != nil {
					t.Fatalf("drop table: %v", err)
				}
				if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
					"CREATE TABLE %s.%s (pk text PRIMARY KEY, new_col int)", ks, table)); err != nil {
					t.Fatalf("recreate table: %v", err)
				}
				defer v2Exec(context.Background(), session, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table))

				waitForSchemaRefresh()

				tm, err := cassandraTable(session, ks, table)
				if err != nil {
					t.Fatalf("TableMetadata after recreate failed: %v", err)
				}
				if _, ok := tm.Columns["new_col"]; !ok {
					t.Errorf("expected column 'new_col' after recreate, got columns: %v", columnNames(tm.Columns))
				}
				if _, ok := tm.Columns["v"]; ok {
					t.Errorf("old column 'v' should not exist after recreate")
				}
			})

			t.Run("nonexistent_table", func(t *testing.T) {
				km, found := session.Metadata().Keyspace(ks)
				require.True(t, found)
				_, found = km.Tables["does_not_exist_at_all"]
				require.False(t, found)
			})

			t.Run("empty_table_name", func(t *testing.T) {
				km, found := session.Metadata().Keyspace(ks)
				require.True(t, found)
				_, found = km.Tables[""]
				require.False(t, found)
			})

			t.Run("empty_keyspace", func(t *testing.T) { _, found := session.Metadata().Keyspace(""); require.False(t, found) })
		})

		t.Run("KeyspaceMetadata", func(t *testing.T) {
			t.Run("includes_new_table", func(t *testing.T) {
				table := testTableName(t)
				if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
					"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int)", ks, table)); err != nil {
					t.Fatalf("create table: %v", err)
				}
				defer v2Exec(context.Background(), session, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table))

				waitForSchemaRefresh()

				require.NoError(t, session.RefreshMetadata(context.Background()))
				km, err := cassandraKeyspace(session, ks)
				if err != nil {
					t.Fatalf("KeyspaceMetadata failed: %v", err)
				}
				if _, ok := km.Tables[table]; !ok {
					t.Fatalf("expected table %q in keyspace metadata, got tables: %v", table, tableNames(km.Tables))
				}
			})

			t.Run("excludes_dropped_table", func(t *testing.T) {
				table := testTableName(t)
				if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
					"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int)", ks, table)); err != nil {
					t.Fatalf("create table: %v", err)
				}

				waitForSchemaRefresh()

				require.NoError(t, session.RefreshMetadata(context.Background()))
				km, err := cassandraKeyspace(session, ks)
				if err != nil {
					t.Fatalf("KeyspaceMetadata before drop failed: %v", err)
				}
				if _, ok := km.Tables[table]; !ok {
					t.Fatalf("expected table %q before drop", table)
				}

				if err := v2CreateTable(context.Background(), session, fmt.Sprintf("DROP TABLE %s.%s", ks, table)); err != nil {
					t.Fatalf("drop table: %v", err)
				}

				waitForSchemaRefresh()

				require.NoError(t, session.RefreshMetadata(context.Background()))
				km, err = cassandraKeyspace(session, ks)
				if err != nil {
					t.Fatalf("KeyspaceMetadata after drop failed: %v", err)
				}
				if _, ok := km.Tables[table]; ok {
					t.Errorf("table %q should not appear after DROP", table)
				}
			})

			t.Run("multiple_tables", func(t *testing.T) {
				tables := []string{testTableName(t, "a"), testTableName(t, "b"), testTableName(t, "c")}
				for _, table := range tables {
					if err := v2CreateTable(context.Background(), session, fmt.Sprintf(
						"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY)", ks, table)); err != nil {
						t.Fatalf("create table %s: %v", table, err)
					}
					defer v2Exec(context.Background(), session, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table))
				}

				waitForSchemaRefresh()

				require.NoError(t, session.RefreshMetadata(context.Background()))
				km, err := cassandraKeyspace(session, ks)
				if err != nil {
					t.Fatalf("KeyspaceMetadata failed: %v", err)
				}
				for _, table := range tables {
					if _, ok := km.Tables[table]; !ok {
						t.Errorf("expected table %q in keyspace metadata", table)
					}
				}
			})

			t.Run("nonexistent_keyspace", func(t *testing.T) {
				_, found := session.Metadata().Keyspace("keyspace_that_does_not_exist_xyz")
				require.False(t, found)
			})

			t.Run("empty_keyspace", func(t *testing.T) { _, found := session.Metadata().Keyspace(""); require.False(t, found) })
		})
	})
}
