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
	"testing"

	"github.com/stretchr/testify/require"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cql/dynamic"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	"github.com/scylladb/gocql/v2/internal/tests"
	"github.com/scylladb/gocql/v2/routing"
)

func TestPrepared(t *testing.T) {
	t.Run("Cache", func(t *testing.T) {
		// Will make sure that the cache size is maintained
		t.Run("Eviction", func(t *testing.T) {
			t.Parallel()
			const maxPrepared = 4
			session := createV2Session(t, func(c *gocql.ConfigBuilder) { c.WithPreparedCacheSize(maxPrepared).WithIgnoreEvents(gocql.SchemaEvent) })
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,mod int)", table))
			runtime := runtimeFor(session)
			runtime.InternalMu.Lock()
			clear(runtime.InternalPrepared)
			runtime.InternalPrepareOrder = nil
			runtime.InternalMu.Unlock()
			insert := fmt.Sprintf("INSERT INTO %s(id,mod) VALUES(?,?)", table)
			for i := int32(0); i < 2; i++ {
				cassandraWrite(t, session, insert, dynamic.Int(i), dynamic.Int(10000%(i+1)))
			}
			selects := []string{fmt.Sprintf("SELECT id,mod FROM %s WHERE id=0", table), fmt.Sprintf("SELECT id,mod FROM %s WHERE id=1", table)}
			for _, stmt := range selects {
				prepared := cassandraPrepared(t, session, cql.Statement[cql.EmptyParams, dynamic.Row]{CQL: stmt, Params: cql.EmptyParamsEncoder, Rows: dynamic.Rows}, gocql.PrepareOptions{})
				cassandraReadBound(t, session, cassandraBound(t, prepared, cql.EmptyParams{}), gocql.Options{})
			}
			update := fmt.Sprintf("UPDATE %s SET mod=? WHERE id=?", table)
			cassandraWrite(t, session, update, dynamic.Int(1), dynamic.Int(11))
			del := fmt.Sprintf("DELETE FROM %s WHERE id=?", table)
			cassandraWrite(t, session, del, dynamic.Int(1))
			cassandraWrite(t, session, insert, dynamic.Int(3), dynamic.Int(11))
			runtime.InternalMu.Lock()
			defer runtime.InternalMu.Unlock()
			require.Len(t, runtime.InternalPrepared, maxPrepared)
			cached := map[string]bool{}
			for key := range runtime.InternalPrepared {
				cached[key.InternalCql] = true
			}
			require.False(t, cached[selects[0]])
			for _, stmt := range []string{selects[1], insert, update, del} {
				require.True(t, cached[stmt], "expected cached %q", stmt)
			}
		})
		t.Run("Key", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			ks := testKeyspaceName(t, "ks2")
			other := cassandraOtherSession(t, session, ks)
			defer other.Close()
			defer cassandraDDL(t, session, "DROP KEYSPACE "+ks)
			for _, s := range []*gocql.Session[testDialect]{session, other} {
				cassandraDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id varchar PRIMARY KEY,field varchar)", table))
			}
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,field) VALUES(?,?)", table), codecTestMust(dynamic.Text("key")), codecTestMust(dynamic.Text("one")))
			cassandraWrite(t, other, fmt.Sprintf("INSERT INTO %s(id,field) VALUES(?,?)", table), codecTestMust(dynamic.Text("key")), codecTestMust(dynamic.Text("two")))
			stmt := fmt.Sprintf("SELECT field FROM %s WHERE id=?", table)
			require.Equal(t, "one", cassandraText(t, cassandraRead(t, session, stmt, codecTestMust(dynamic.Text("key"))), "field"))
			require.Equal(t, "two", cassandraText(t, cassandraRead(t, other, stmt, codecTestMust(dynamic.Text("key"))), "field"))
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
			for i, keyspace := range []string{fixtureKeyspace(""), ks} {
				bound := cassandraWriteBound(t, session, insert, []dynamic.Value{dynamic.Int(int32(i + 1))}, gocql.PrepareOptions{Keyspace: keyspace})
				cassandraBatch(t, session, cql.LoggedBatch, []batchItem{cassandraItem(t, bound)}, gocql.Options{Keyspace: keyspace})
			}
			for i, keyspace := range []string{fixtureKeyspace(""), ks} {
				prepared := cassandraPrepared(t, session, cql.Statement[cql.EmptyParams, int32]{CQL: "SELECT * FROM " + table, Params: cql.EmptyParamsEncoder, Rows: cql.OneColumn(cql.Int32Codec)}, gocql.PrepareOptions{Keyspace: keyspace})
				require.Equal(t, keyspace, prepared.Info().Keyspace)
				require.Equal(t, int32(i+1), cassandraReadBound(t, session, cassandraBound(t, prepared, cql.EmptyParams{}), gocql.Options{Keyspace: keyspace}))
			}
		})
	})

	t.Run("Prepare", func(t *testing.T) {
		t.Parallel()

		// Checks that fully qualified prepared statements retain the
		// server's keyspace/table metadata independently of later configuration changes.
		t.Run("KeyspaceTable", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := createV2Cluster()
			v2ConfigureRouting(t, cluster, routing.Options{TokenAware: true})
			session, err := gocql.Connect[testDialect](ctx, mustBuild(t, cluster))
			if err != nil {
				t.Fatal("connect:", err)
			}
			defer session.Close()
			cluster.WithKeyspace("wrong_keyspace")
			keyspace, table := v2SchemaRoutingName(t), v2SchemaRoutingName(t)
			v2CreateRoutingKeyspace(t, session, keyspace, 1)
			defer cleanup(t, session, keyspace)
			if err := v2CreateTable(ctx, session, fmt.Sprintf(`CREATE TABLE %s.%s (pk int, ck int, v int, PRIMARY KEY (pk, ck))`, keyspace, table)); err != nil {
				t.Fatal(err)
			}
			if _, err := v2SchemaRoutingExec(ctx, session, fmt.Sprintf(`INSERT INTO %s.%s(pk, ck, v) VALUES (?, ?, ?)`, keyspace, table), gocql.Options{Consistency: cql.Option[cql.Consistency]{Value: cql.One, Set: true}}, dynamic.Int(1), dynamic.Int(2), dynamic.Int(3)); err != nil {
				t.Fatal(err)
			}
			prepared, err := session.Prepare(ctx, cql.Statement[[]dynamic.Value, dynamic.Row]{CQL: fmt.Sprintf(`SELECT pk FROM %s.%s WHERE pk = ? LIMIT 1`, keyspace, table), Params: dynamic.Params, Rows: dynamic.Rows}, gocql.PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			bound, err := BindOwned(prepared, []dynamic.Value{dynamic.Int(1)})
			if err != nil {
				t.Fatal(err)
			}
			defer bound.Close()
			rows, err := Run(ctx, session, bound, bindOptions(session, gocql.Options{Consistency: cql.Option[cql.Consistency]{Value: cql.One, Set: true}}))
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var row dynamic.Row
			if !rows.NextInto(&row) {
				t.Fatalf("missing row: %v", rows.Err())
			}
			tests.AssertEqual(t, "pk", int32(1), v2SchemaRoutingInt(t, row, "pk"))
			if rows.NextInto(&row) {
				t.Fatal("unexpected extra row")
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			info := prepared.Info()
			tests.AssertEqual(t, "prepared.Keyspace", keyspace, info.Keyspace)
			tests.AssertEqual(t, "prepared.Table", table, info.Table)
			tests.AssertEqual(t, "bound.Keyspace", keyspace, bound.Routing().Keyspace)
			tests.AssertEqual(t, "bound.Table", table, bound.Routing().Table)
			tests.AssertTrue(t, "partition-key token is available", bound.Routing().Token.Set)
			metadata, ok := session.Metadata().Keyspace(keyspace)
			tests.AssertTrue(t, "keyspace is present", ok)
			tableMetadata := metadata.Tables[table]
			tests.AssertTrue(t, "table is present", tableMetadata != nil)
			tests.AssertEqual(t, "partition key metadata", "pk", tableMetadata.PartitionKey[0].Name)
			tests.AssertEqual(t, "clustering key metadata", "ck", tableMetadata.ClusteringColumns[0].Name)
		})
		t.Run("Metadata", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			prepared := cassandraPrepared(t, session, cql.Statement[string, dynamic.Row]{CQL: "SELECT release_version,host_id FROM system.local WHERE key=?", Params: cql.OneParam(cql.StringCodec), Rows: dynamic.Rows}, gocql.PrepareOptions{})
			require.Equal(t, 1, prepared.Parameters().Len())
			require.Equal(t, 2, prepared.Columns().Len())
		})
		t.Run("MissingSchema", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			conn := getRandomConn(t, session)
			statement := cql.Statement[int32, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(val) VALUES(?)", table), Params: cql.OneParam(cql.Int32Codec), Rows: cql.EmptyRowDecoder}
			options := gocql.PrepareOptions{HostID: cql.Option[cql.UUID]{Value: implEndpoint.InternalHostInfoHostUUID(conn.InternalHost), Set: true}}
			_, err := session.Prepare(context.Background(), statement, options)
			require.Error(t, err)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(val int PRIMARY KEY)", table))
			prepared := cassandraPrepared(t, session, statement, options)
			_, err = Exec(context.Background(), session, cassandraBound(t, prepared, int32(5)), bindOptions(session, gocql.Options{HostID: options.HostID}))
			require.NoError(t, err)
		})
	})

	t.Run("Reprepare", func(t *testing.T) {
		t.Parallel()

		t.Run("AfterAlterTable", func(t *testing.T) {
			t.Parallel()
			if flagCassVersion.Major == 0 || flagCassVersion.Before(2, 1, 3) {
				t.Skipf("skipping TestPrepared/Reprepare/AfterAlterTable due to CASSANDRA-7910 in Cassandra <2.1.3 version=%v", flagCassVersion)
			}
			session := createV2Session(t, func(c *gocql.ConfigBuilder) { c.WithPoolSize(gocql.PerHost(1)) })
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id text PRIMARY KEY)", table))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id) VALUES(?)", table), codecTestMust(dynamic.Text("test")))
			selectCQL := fmt.Sprintf("SELECT * FROM %s WHERE id=?", table)
			before := cassandraDynamicBound(t, session, selectCQL, []dynamic.Value{codecTestMust(dynamic.Text("test"))}, gocql.PrepareOptions{})
			require.Equal(t, "test", cassandraText(t, cassandraReadBound(t, session, before, gocql.Options{}), "id"))
			cassandraDDL(t, session, fmt.Sprintf("ALTER TABLE %s ADD val text", table))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,val) VALUES(?,?)", table), codecTestMust(dynamic.Text("test")), codecTestMust(dynamic.Text("test-val")))
			row := cassandraReadBound(t, session, before, gocql.Options{})
			require.Equal(t, "test", cassandraText(t, row, "id"))
			require.Equal(t, "test-val", cassandraText(t, row, "val"))
		})
		t.Run("Batch", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			bound, conn := injectInvalidPreparedStatement(t, session, table)
			cassandraBatch(t, session, cql.UnloggedBatch, []batchItem{cassandraItem(t, bound)}, *func() *implSession.Options {
				opaqueStorageValuexxx := new(implSession.Options)
				opaqueStorageValuexxx.HostID = cql.Option[cql.UUID]{Value: implEndpoint.InternalHostInfoHostUUID(conn.InternalHost), Set: true}
				implSession.OptionsData(opaqueStorageValuexxx).InternalConnection = conn
				return opaqueStorageValuexxx
			}())
			implSession.BoundData(bound).InternalRequest.InternalPrepared().InternalMu.RLock()
			id := append([]byte(nil), implSession.BoundData(bound).InternalRequest.InternalPrepared().InternalHosts[implEndpoint.InternalHostInfoHostUUID(conn.InternalHost)].InternalId...)
			implSession.BoundData(bound).InternalRequest.InternalPrepared().InternalMu.RUnlock()
			require.NotEqual(t, []byte("foobar"), id)
			require.Equal(t, int64(7), cassandraInt(t, cassandraRead(t, session, "SELECT bar FROM "+table+" WHERE foo=?", codecTestMust(dynamic.Text("bar"))), "bar"))
		})
		t.Run("Statement", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			bound, conn := injectInvalidPreparedStatement(t, session, table)
			_, err := Exec(context.Background(), session, bound, bindOptions(session, *func() *implSession.Options {
				opaqueStorageValuexx := new(implSession.Options)
				opaqueStorageValuexx.HostID = cql.Option[cql.UUID]{Value: implEndpoint.InternalHostInfoHostUUID(conn.InternalHost), Set: true}
				implSession.OptionsData(opaqueStorageValuexx).InternalConnection = conn
				return opaqueStorageValuexx
			}()))
			require.NoError(t, err)
			implSession.BoundData(bound).InternalRequest.InternalPrepared().InternalMu.RLock()
			id := append([]byte(nil), implSession.BoundData(bound).InternalRequest.InternalPrepared().InternalHosts[implEndpoint.InternalHostInfoHostUUID(conn.InternalHost)].InternalId...)
			implSession.BoundData(bound).InternalRequest.InternalPrepared().InternalMu.RUnlock()
			require.NotEqual(t, []byte("foobar"), id)
			require.Equal(t, int64(7), cassandraInt(t, cassandraRead(t, session, "SELECT bar FROM "+table+" WHERE foo=?", codecTestMust(dynamic.Text("bar"))), "bar"))
		})
	})
}
