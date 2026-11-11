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
	"strings"
	"testing"
	"time"

	implSession "github.com/scylladb/gocql/v2/internal/session"

	"github.com/stretchr/testify/require"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cql/dynamic"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/retry"
)

func TestBatch(t *testing.T) {
	t.Run("Bind", func(t *testing.T) {
		t.Parallel()

		// Makes sure that the application can manually bind query parameters when executing in a batch
		t.Run("ManualParams", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int,cluster int,value text,PRIMARY KEY(id,cluster))", table))
			params := cql.Params(cql.Param("id", cql.Int32Codec, func(v ClusteredKeyValue) int32 { return v.Id }), cql.Param("cluster", cql.Int32Codec, func(v ClusteredKeyValue) int32 { return v.Cluster }), cql.Param("value", cql.StringCodec, func(v ClusteredKeyValue) string { return v.Value }))
			prepared := cassandraPrepared(t, session, cql.Statement[ClusteredKeyValue, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(id,cluster,value) VALUES(?,?,?)", table), Params: params, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
			require.Equal(t, 3, prepared.Parameters().Len())
			batch := &gocql.Batch{Kind: cql.LoggedBatch}
			cassandraAdd(t, batch, prepared, ClusteredKeyValue{4000, 5000, "bar"})
			cassandraBatch(t, session, batch, gocql.Options{})
			read := cassandraDynamicPrepared(t, session, fmt.Sprintf("SELECT id,cluster,value FROM %s WHERE id=? AND cluster=?", table), gocql.PrepareOptions{})
			row := cassandraReadOne(t, session, read, []dynamic.Value{dynamic.Int(4000), dynamic.Int(5000)}, gocql.Options{})
			require.Equal(t, "bar", cassandraText(t, row, "value"))
			require.Equal(t, int64(4000), cassandraInt(t, row, "id"))
			require.Equal(t, int64(5000), cassandraInt(t, row, "cluster"))
		})
		// Verify unset column will not replace a column in batch
		t.Run("UnsetColumn", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			if (*implSession.Session)(session).Data().InternalCfg.ProtoVersion < 4 {
				t.Skip("Unset Values are not supported in protocol < 4")
			}
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,my_int int,my_text text)", table))
			stmt := fmt.Sprintf("INSERT INTO %s(id,my_int,my_text) VALUES(?,?,?)", table)
			batch := &gocql.Batch{Kind: cql.LoggedBatch}
			for _, values := range [][]dynamic.Value{{dynamic.Int(1), dynamic.Int(1), dynamic.Unset(codecTestMust(dynamic.Text("")).Type())}, {dynamic.Int(1), dynamic.Unset(dynamic.Int(0).Type()), codecTestMust(dynamic.Text(""))}, {dynamic.Int(2), dynamic.Int(2), dynamic.Unset(codecTestMust(dynamic.Text("")).Type())}} {
				cassandraAdd(t, batch, cassandraWritePrepared(t, session, stmt, gocql.PrepareOptions{}), values)
			}
			info := cassandraBatch(t, session, batch, gocql.Options{})
			require.Positive(t, info.Attempts)
			require.Positive(t, info.Latency)
			require.Equal(t, int64(2), cassandraInt(t, cassandraRead(t, session, "SELECT count(*) FROM "+table), "count"))
			row := cassandraRead(t, session, "SELECT id,my_int,my_text FROM "+table+" WHERE id=1")
			require.Equal(t, int64(1), cassandraInt(t, row, "id"))
			require.Equal(t, int64(1), cassandraInt(t, row, "my_int"))
			require.Equal(t, "", cassandraText(t, row, "my_text"))
		})
	})

	t.Run("Exec", func(t *testing.T) {
		t.Parallel()

		t.Run("Basic", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			retryPolicy := sessionTestRetry{action: retry.Next, maxAttempts: 2}
			session := createV2SessionFromConfig(createV2ClusterWithout(fixtureRetry|fixtureConsistency, func(config *gocql.ConfigBuilder) {
				// The batch below writes at ONE, so the rows are read back at ALL:
				// a QUORUM read could miss the one replica that took the write.
				config.ConfigureDefaultProfile(func(p *gocql.ProfileBuilder) { p.WithRetry(retryPolicy).WithConsistency(cql.All) })
			}), t, false)
			defer session.Close()
			table := testTableName(t)
			require.NoError(t, v2CreateTable(ctx, session, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", table)))
			insert := cassandraWritePrepared(t, session, fmt.Sprintf("INSERT INTO %s(id) VALUES (?)", table), gocql.PrepareOptions{})
			inherited, err := resolveOptions(session, gocql.Options{})
			require.NoError(t, err)
			require.Equal(t, retryPolicy, inherited.InternalProfile.Retry)
			override := sessionTestRetry{action: retry.Next, maxAttempts: 4}
			observer := new(sessionTestObserver)
			options := gocql.Options{Consistency: cql.Option[cql.Consistency]{Value: cql.One, Set: true}, Retry: override, Trace: true, Observer: observer}
			resolved, err := resolveOptions(session, options)
			require.NoError(t, err)
			require.Equal(t, override, resolved.InternalProfile.Retry)
			require.Equal(t, cql.One, resolved.InternalProfile.Consistency)
			require.True(t, resolved.InternalOptions.Trace)
			// Native batch kind remains explicit on Batch.Kind.
			// Per-request Result/Observer replace attempts/latency on a mutable batch.
			require.Zero(t, (gocql.Result{}).Attempts)
			require.Zero(t, (gocql.Result{}).Latency)
			for _, kind := range []cql.BatchKind{cql.UnloggedBatch, cql.LoggedBatch} {
				batch := &gocql.Batch{Kind: kind}
				cassandraAdd(t, batch, insert, []dynamic.Value{dynamic.Int(1)})
				cassandraAdd(t, batch, insert, []dynamic.Value{dynamic.Int(2)})
				require.Equal(t, 2, batch.Len())
				result, err := session.ExecBatch(ctx, batch, bindOptions(session, options))
				require.NoError(t, err)
				require.Equal(t, 1, result.Attempts)
				require.Greater(t, result.Latency, time.Duration(0))
				require.Equal(t, cql.One, result.AchievedConsistency)
				require.NotEmpty(t, result.TraceIDs)
			}
			requests, attempts := observer.snapshot()
			require.Len(t, requests, 2)
			require.Len(t, attempts, 2)
			for _, request := range requests {
				require.Len(t, request.BatchStatements, 2)
			}
			for _, id := range []int32{1, 2} {
				row, err := v2ReadOne(ctx, session, fmt.Sprintf("SELECT id FROM %s WHERE id=?", table), dynamic.Int(id))
				require.NoError(t, err)
				require.Equal(t, int64(id), sessionTestInt(t, row, "id"))
			}
		})
		t.Run("BeginApplyStatement", func(t *testing.T) {
			t.Parallel()

			session := createV2Session(t)
			defer session.Close()

			table := testTableName(t)

			if err := v2CreateTable(context.Background(), session, fmt.Sprintf(("CREATE TABLE "+fixtureKeyspace("")+".%s (id int primary key)"), table)); err != nil {
				t.Fatal(err)
			}

			begin := "BEGIN BATCH"
			end := "APPLY BATCH"
			query := fmt.Sprintf("INSERT INTO %s (id) VALUES (?)", table)
			fullQuery := strings.Join([]string{begin, query, end}, "\n")
			prepared := cassandraWritePrepared(t, session, fullQuery, gocql.PrepareOptions{})
			if _, err := prepared.Exec(context.Background(), []dynamic.Value{dynamic.Int(5)}, bindOptions(session, gocql.Options{Consistency: cql.Option[cql.Consistency]{Value: cql.Quorum, Set: true}})); err != nil {
				t.Fatal(err)
			}
		})
		t.Run("Logged", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY)", table))
			prepared := cassandraPrepared(t, session, cql.Statement[int32, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(id) VALUES(?)", table), Params: cql.OneParam(cql.Int32Codec), Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
			batch := &gocql.Batch{Kind: cql.LoggedBatch}
			for i := int32(0); i < 100; i++ {
				cassandraAdd(t, batch, prepared, i)
			}
			cassandraBatch(t, session, batch, gocql.Options{})
			require.Equal(t, int64(100), cassandraInt(t, cassandraRead(t, session, "SELECT COUNT(*) FROM "+table), "count"))
		})
		t.Run("RejectsInvalidStatement", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			require.NoError(t, v2CreateTable(ctx, session, fmt.Sprintf((`CREATE TABLE `+fixtureKeyspace("")+`.%s (id int primary key, val inet)`), table)))

			// Keep the invalid SELECT inside the native BATCH, so the server rejects
			// the batch itself rather than a preceding PREPARE operation.
			inet := cql.InetType
			batch := &gocql.Batch{Kind: cql.LoggedBatch}
			require.NoError(t, session.UnpreparedAddToBatch(batch, cql.Statement[[]dynamic.Value, cql.EmptyRow]{
				CQL:    fmt.Sprintf(("SELECT * FROM " + fixtureKeyspace("") + ".%s WHERE id=2 AND val=?"), table),
				Params: dynamic.Params, Rows: cql.EmptyRowDecoder,
				BindMarkers: cql.NewColumns(cql.ColumnSpec{Name: "val", Type: inet}),
			}, []dynamic.Value{dynamic.Null(inet)}))
			_, err := session.ExecBatch(ctx, batch)
			require.Error(t, err, "expected to get error for invalid query in batch")
			var server cqlerr.ServerError
			require.ErrorAs(t, err, &server)
		})
		// Tests gocql to make sure batch operations larger than the maximum
		// statement limit are not submitted to a cassandra node.
		t.Run("RejectsOverStatementLimit", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY)", table))
			prepared := cassandraWritePrepared(t, session, fmt.Sprintf("INSERT INTO %s(id) VALUES(?)", table), gocql.PrepareOptions{})
			batch := &gocql.Batch{Kind: cql.LoggedBatch}
			for range 65537 {
				cassandraAdd(t, batch, prepared, []dynamic.Value{dynamic.Int(1)})
			}
			before := session.Metrics()
			_, err := session.ExecBatch(context.Background(), batch)
			require.Error(t, err)
			require.Contains(t, err.Error(), "65535")
			require.Equal(t, before.Requests, session.Metrics().Requests, "oversized batches must be rejected before submission")
		})
		t.Run("UnpreparedCounter", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,c counter)", table))
			batch := &gocql.Batch{Kind: cql.UnloggedBatch}
			for range 100 {
				require.NoError(t, session.UnpreparedAddToBatch(batch, cql.Command(fmt.Sprintf("UPDATE %s SET c=c+1 WHERE id=1", table)), cql.EmptyParams{}))
			}
			cassandraBatch(t, session, batch, gocql.Options{})
			require.Equal(t, int64(1), cassandraInt(t, cassandraRead(t, session, "SELECT COUNT(*) FROM "+table), "count"))
			require.Equal(t, int64(100), cassandraInt(t, cassandraRead(t, session, "SELECT c FROM "+table), "c"))
		})
	})

	t.Run("Options", func(t *testing.T) {
		t.Run("Keyspace", func(t *testing.T) {
			ctx := context.Background()
			session := createV2Session(t)
			defer session.Close()
			if (*implSession.Session)(session).Data().InternalCfg.ProtoVersion < protoVersion5 {
				t.Skip("keyspace for BATCH message is not supported in protocol < 5")
			}
			const keyspace = "gocql_keyspace_override_test"
			require.NoError(t, v2CreateTable(ctx, session, `CREATE KEYSPACE IF NOT EXISTS `+keyspace+` WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}`))
			require.NoError(t, v2CreateTable(ctx, session, "CREATE TABLE IF NOT EXISTS "+keyspace+".batch_keyspace(id int, value text, PRIMARY KEY (id))"))
			// Explicit prepare keyspace and execution keyspace must agree. Unqualified
			// member CQL still exercises the v5 BATCH keyspace flag.
			prepared, err := session.Prepare(ctx, cql.Statement[[]dynamic.Value, cql.EmptyRow]{CQL: "INSERT INTO batch_keyspace(id, value) VALUES (?, ?)", Params: dynamic.Params, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{Keyspace: keyspace})
			require.NoError(t, err)
			batch := &gocql.Batch{Kind: cql.LoggedBatch}
			cassandraAdd(t, batch, prepared, []dynamic.Value{dynamic.Int(1), codecTestMust(dynamic.Text("val1"))})
			cassandraAdd(t, batch, prepared, []dynamic.Value{dynamic.Int(2), codecTestMust(dynamic.Text("val2"))})
			_, err = session.ExecBatch(ctx, batch, bindOptions(session, gocql.Options{Keyspace: keyspace}))
			require.NoError(t, err)
			rows, err := v2Query(ctx, session, "SELECT id, value FROM "+keyspace+".batch_keyspace")
			require.NoError(t, err)
			defer rows.Close()
			got := map[int64]string{}
			var row dynamic.Row
			for rows.NextInto(&row) {
				value, err := row.Get("value")
				require.NoError(t, err)
				text, err := value.Text()
				require.NoError(t, err)
				got[sessionTestInt(t, row, "id")] = text.String()
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			require.Equal(t, map[int64]string{1: "val1", 2: "val2"}, got)
		})
		t.Run("NowInSeconds", func(t *testing.T) {
			ctx := context.Background()
			session := createV2Session(t)
			defer session.Close()
			if (*implSession.Session)(session).Data().InternalCfg.ProtoVersion < protoVersion5 {
				t.Skip("Batch now in seconds are only available on protocol >= 5")
			}
			table := testTableName(t)
			require.NoError(t, v2CreateTable(ctx, session, fmt.Sprintf(`CREATE TABLE %s (id int primary key, val text)`, table)))
			batch := &gocql.Batch{Kind: cql.LoggedBatch}
			cassandraAdd(t, batch, cassandraWritePrepared(t, session, fmt.Sprintf("INSERT INTO %s (id, val) VALUES (?, ?) USING TTL 20", table), gocql.PrepareOptions{}), []dynamic.Value{dynamic.Int(1), codecTestMust(dynamic.Text("val"))})
			_, err := session.ExecBatch(ctx, batch, bindOptions(session, gocql.Options{NowInSeconds: cql.Option[int32]{Value: 0, Set: true}}))
			require.NoError(t, err)

			prepared, err := session.Prepare(ctx, cql.Statement[int32, int32]{CQL: fmt.Sprintf(`SELECT TTL(val) FROM %s WHERE id = ?`, table), Params: cql.OneParam(cql.Int32Codec), Rows: cql.OneColumn(cql.Int32Codec)}, gocql.PrepareOptions{})
			require.NoError(t, err)
			rows, err := prepared.Run(ctx, int32(1), bindOptions(session, gocql.Options{NowInSeconds: cql.Option[int32]{Value: 10, Set: true}}))
			require.NoError(t, err)
			defer rows.Close()
			var ttl int32
			require.True(t, rows.NextInto(&ttl))
			require.Equal(t, int32(10), ttl)
			require.False(t, rows.NextInto(&ttl))
			require.NoError(t, rows.Err())
		})
		t.Run("Timestamp", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			require.NoError(t, v2CreateTable(ctx, session, fmt.Sprintf((`CREATE TABLE `+fixtureKeyspace("")+`.%s (id int primary key, val text)`), table)))
			micros := time.Now().UnixMicro() - 1000
			insert := cassandraWritePrepared(t, session, fmt.Sprintf(("INSERT INTO "+fixtureKeyspace("")+".%s (id, val) VALUES (?, ?)"), table), gocql.PrepareOptions{})
			batch := &gocql.Batch{Kind: cql.LoggedBatch}
			cassandraAdd(t, batch, insert, []dynamic.Value{dynamic.Int(1), codecTestMust(dynamic.Text("val"))})
			cassandraAdd(t, batch, insert, []dynamic.Value{dynamic.Int(2), codecTestMust(dynamic.Text("val"))})
			_, err := session.ExecBatch(ctx, batch, bindOptions(session, gocql.Options{Timestamp: cql.Option[int64]{Value: micros, Set: true}}))
			require.NoError(t, err)
			for _, id := range []int32{1, 2} {
				row, err := v2ReadOne(ctx, session, fmt.Sprintf((`SELECT writetime(val) AS written FROM `+fixtureKeyspace("")+`.%s WHERE id = ?`), table), dynamic.Int(id))
				require.NoError(t, err)
				require.Equal(t, micros, sessionTestInt(t, row, "written"))
			}
		})
	})

	// Confirms that the stats are returning valid data. Accuracy may be questionable.
	t.Run("Stats", func(t *testing.T) {
		t.Parallel()
		session := createV2Session(t)
		defer session.Close()
		table := testTableName(t)
		cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY)", table))
		batch := &gocql.Batch{Kind: cql.LoggedBatch}
		for _, id := range []int32{1, 2} {
			cassandraAdd(t, batch, cassandraWritePrepared(t, session, fmt.Sprintf("INSERT INTO %s(id) VALUES(?)", table), gocql.PrepareOptions{}), []dynamic.Value{dynamic.Int(id)})
		}
		info := cassandraBatch(t, session, batch, gocql.Options{})
		require.Positive(t, info.Attempts)
		require.Positive(t, info.Latency)
	})
}
