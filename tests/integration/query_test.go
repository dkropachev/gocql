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
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/logging"
	implSession "github.com/scylladb/gocql/v2/internal/session"

	"github.com/stretchr/testify/require"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cql/dynamic"
	"github.com/scylladb/gocql/v2/cql/stream"
	"github.com/scylladb/gocql/v2/cqlerr"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/retry"
)

func TestQuery(t *testing.T) {
	t.Run("Bind", func(t *testing.T) {
		t.Parallel()

		t.Run("DetectsUse", func(t *testing.T) {
			t.Parallel()

			testCases := []struct {
				input string
				exp   bool
			}{
				{"USE foo", true},
				{"USe foo", true},
				{"UsE foo", true},
				{"Use foo", true},
				{"uSE foo", true},
				{"uSe foo", true},
				{"usE foo", true},
				{"use foo", true},
				{"SELECT ", false},
				{"UPDATE ", false},
				{"INSERT ", false},
				{"", false},
			}

			// Adding to a batch validates the statement without a connection.
			session := (*gocql.Session[testDialect])(implSession.InternalWrapSession(new(implCluster.Core)))
			(*implSession.Session)(session).Data().InternalLogger = logging.Nop()
			defer session.Close()
			for _, tc := range testCases {
				err := session.UnpreparedAddToBatch(&gocql.Batch{}, cql.Command(tc.input), cql.EmptyParams{})
				v := errors.Is(err, cqlerr.ErrUseStmt)
				if v != tc.exp {
					t.Fatalf("expected %v but got %v for statement %q", tc.exp, v, tc.input)
				}
			}
		})
		t.Run("NamedValues", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,value text)", table))
			prepared := cassandraPrepared(t, session, cql.Statement[[]dynamic.Field, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(id,value) VALUES(:id,:value)", table), Params: dynamic.NamedParams, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
			_, err := prepared.Exec(context.Background(), []dynamic.Field{{Name: "value", Value: codecTestMust(dynamic.Text("i am a value"))}, {Name: "id", Value: dynamic.Int(1)}})
			require.NoError(t, err)
			read := cassandraPrepared(t, session, cql.Statement[[]dynamic.Field, cql.Text]{CQL: fmt.Sprintf("SELECT value FROM %s WHERE id=:id", table), Params: dynamic.NamedParams, Rows: cql.OneColumn(cql.TextCodec)}, gocql.PrepareOptions{})
			require.Equal(t, "i am a value", cassandraReadOne(t, session, read, []dynamic.Field{{Name: "id", Value: dynamic.Int(1)}}, gocql.Options{}).String())
		})
		// Tests to see that a nil value passed to a query is handled by Cassandra
		// TODO validate the nil value by reading back the nil. Need to fix Unmarshalling.
		t.Run("Nil", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,count int)", table))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,count) VALUES(?,?)", table), dynamic.Int(1), dynamic.Null(dynamic.Int(0).Type()))
			row := cassandraRead(t, session, "SELECT id,count FROM "+table)
			require.Equal(t, int64(1), cassandraInt(t, row, "id"))
			require.Equal(t, cql.Null, cassandraField(t, row, "count").State())
		})
		// Tests to make sure the library correctly handles the application level bug
		// whereby not enough query arguments are passed to a query
		t.Run("NotEnoughArgs", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int,cluster int,value int,PRIMARY KEY(id,cluster))", table))
			read := cassandraPrepared(t, session, cql.Statement[[]dynamic.Value, dynamic.Row]{CQL: fmt.Sprintf("SELECT * FROM %s WHERE id=? AND cluster=?", table), Params: dynamic.Params, Rows: dynamic.Rows}, gocql.PrepareOptions{})
			_, err := read.Run(context.Background(), []dynamic.Value{dynamic.Int(1)})
			require.Error(t, err)
			write := cassandraPrepared(t, session, cql.Statement[[]dynamic.Value, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(id,cluster,value) VALUES(?,?,?)", table), Params: dynamic.Params, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
			err = write.AddToBatch(&gocql.Batch{}, []dynamic.Value{dynamic.Int(1), dynamic.Int(2)})
			require.Error(t, err, "invalid batch entry must fail while it is added")
		})
		// Makes sure that the application can manually bind query parameters using the query meta data supplied at runtime
		t.Run("ParamsMetadata", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int,cluster int,value text,PRIMARY KEY(id,cluster))", table))
			params := cql.Params(cql.Param("value", cql.StringCodec, func(v ClusteredKeyValue) string { return v.Value }), cql.Param("cluster", cql.Int32Codec, func(v ClusteredKeyValue) int32 { return v.Cluster }), cql.Param("id", cql.Int32Codec, func(v ClusteredKeyValue) int32 { return v.Id }))
			prepared := cassandraPrepared(t, session, cql.Statement[ClusteredKeyValue, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(id,cluster,value) VALUES(?,?,?)", table), Params: params, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
			require.Equal(t, 3, prepared.Parameters().Len())
			_, err := prepared.Exec(context.Background(), ClusteredKeyValue{200, 300, "baz"})
			require.NoError(t, err)
			readParams := cql.Params(cql.Param("cluster", cql.Int32Codec, func(v ClusteredKeyValue) int32 { return v.Cluster }), cql.Param("id", cql.Int32Codec, func(v ClusteredKeyValue) int32 { return v.Id }))
			read := cassandraPrepared(t, session, cql.Statement[ClusteredKeyValue, dynamic.Row]{CQL: fmt.Sprintf("SELECT id,cluster,value FROM %s WHERE id=? AND cluster=?", table), Params: readParams, Rows: dynamic.Rows}, gocql.PrepareOptions{})
			row := cassandraReadOne(t, session, read, ClusteredKeyValue{Id: 200, Cluster: 300}, gocql.Options{})
			require.Equal(t, "baz", cassandraText(t, row, "value"))
			require.Equal(t, int64(200), cassandraInt(t, row, "id"))
			require.Equal(t, int64(300), cassandraInt(t, row, "cluster"))
		})
		t.Run("Rebind", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,value text)", table))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,value) VALUES(?,?)", table), dynamic.Int(23), codecTestMust(dynamic.Text("quux")))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,value) VALUES(?,?)", table), dynamic.Int(24), codecTestMust(dynamic.Text("w00t")))
			prepared := cassandraPrepared(t, session, cql.Statement[int32, cql.Text]{CQL: fmt.Sprintf("SELECT value FROM %s WHERE id=?", table), Params: cql.OneParam(cql.Int32Codec), Rows: cql.OneColumn(cql.TextCodec)}, gocql.PrepareOptions{})
			require.Equal(t, "quux", cassandraReadOne(t, session, prepared, int32(23), gocql.Options{}).String())
			require.Equal(t, "w00t", cassandraReadOne(t, session, prepared, int32(24), gocql.Options{}).String())
			require.Equal(t, "quux", cassandraReadOne(t, session, prepared, int32(23), gocql.Options{}).String())
		})
		// Checks to make sure the correct error is returned when the user tries to execute a use statement.
		t.Run("RejectsUse", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			_, err := v2Exec(context.Background(), session, ("USE " + fixtureKeyspace("")))
			require.Error(t, err)
			require.ErrorIs(t, err, cqlerr.ErrUseStmt)
		})
		// Makes sure that the application can manually bind query parameters using the simplest possible static binding strategy
		t.Run("Static", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,value text)", table))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,value) VALUES(?,?)", table), dynamic.Int(113), codecTestMust(dynamic.Text("foo")))
			prepared := cassandraPrepared(t, session, cql.Statement[int32, dynamic.Row]{CQL: fmt.Sprintf("SELECT id,value FROM %s WHERE id=?", table), Params: cql.OneParam(cql.Int32Codec), Rows: dynamic.Rows}, gocql.PrepareOptions{})
			require.Equal(t, 1, prepared.Parameters().Len())
			for i := 0; i < 2; i++ {
				row := cassandraReadOne(t, session, prepared, int32(113), gocql.Options{})
				require.Equal(t, int64(113), cassandraInt(t, row, "id"))
				require.Equal(t, "foo", cassandraText(t, row, "value"))
			}
		})
		// Tests to make sure the library correctly handles the application level bug
		// whereby too many query arguments are passed to a query
		t.Run("TooManyArgs", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,value int)", table))
			read := cassandraPrepared(t, session, cql.Statement[[]dynamic.Value, dynamic.Row]{CQL: fmt.Sprintf("SELECT * FROM %s WHERE id=?", table), Params: dynamic.Params, Rows: dynamic.Rows}, gocql.PrepareOptions{})
			_, err := read.Run(context.Background(), []dynamic.Value{dynamic.Int(1), dynamic.Int(2)})
			require.Error(t, err)
			write := cassandraPrepared(t, session, cql.Statement[[]dynamic.Value, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(id,value) VALUES(?,?)", table), Params: dynamic.Params, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
			err = write.AddToBatch(&gocql.Batch{}, []dynamic.Value{dynamic.Int(1), dynamic.Int(2), dynamic.Int(3)})
			require.Error(t, err, "invalid batch entry must fail while it is added")
		})
		// Verify unset column will not replace an existing column
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
			cassandraWrite(t, session, stmt, dynamic.Int(1), dynamic.Int(2), codecTestMust(dynamic.Text("3")))
			cassandraWrite(t, session, stmt, dynamic.Int(1), dynamic.Unset(dynamic.Int(0).Type()), dynamic.Unset(codecTestMust(dynamic.Text("")).Type()))
			row := cassandraRead(t, session, "SELECT id,my_int,my_text FROM "+table)
			require.Equal(t, int64(1), cassandraInt(t, row, "id"))
			require.Equal(t, int64(2), cassandraInt(t, row, "my_int"))
			require.Equal(t, "3", cassandraText(t, row, "my_text"))
		})
	})

	t.Run("ConsistencyString", func(t *testing.T) {
		t.Parallel()

		names := map[fmt.Stringer]string{
			cql.Any:         "ANY",
			cql.One:         "ONE",
			cql.Two:         "TWO",
			cql.Three:       "THREE",
			cql.Quorum:      "QUORUM",
			cql.All:         "ALL",
			cql.LocalQuorum: "LOCAL_QUORUM",
			cql.EachQuorum:  "EACH_QUORUM",
			cql.Serial:      "SERIAL",
			cql.LocalSerial: "LOCAL_SERIAL",
			cql.LocalOne:    "LOCAL_ONE",
		}

		for k, v := range names {
			if k.String() != v {
				t.Fatalf("expected '%v', got '%v'", v, k.String())
			}
		}
	})

	t.Run("CustomPayload", func(t *testing.T) {
		t.Parallel()

		t.Run("Messages", func(t *testing.T) {
			t.Parallel()

			requireCassandraFixtures(t)
			cluster := createV2Cluster()
			session := createV2SessionFromConfig(cluster, t, false)
			defer session.Close()

			table := testTableName(t, session.ID())

			if err := v2CreateTable(context.Background(), session, fmt.Sprintf("CREATE TABLE %s.%s (id int, value int, PRIMARY KEY (id))", (*implSession.Session)(session).Data().InternalCfg.Keyspace, table)); err != nil {
				t.Fatal(err)
			}

			customPayload := []cql.PayloadEntry{{Key: "a", Value: cql.Nullable[cql.Bytes]{Valid: true, Value: cql.BorrowBytes([]byte{10, 20})}}, {Key: "b", Value: cql.Nullable[cql.Bytes]{Valid: true, Value: cql.BorrowBytes([]byte{20, 30})}}}
			options := gocql.Options{Consistency: cql.Option[cql.Consistency]{Value: cql.One, Set: true}, CustomPayload: customPayload}

			// Prepared SELECT retains the EXECUTE custom-payload path.
			selectStatement := cql.Statement[[]dynamic.Value, dynamic.Row]{CQL: fmt.Sprintf("SELECT id FROM %s where id = ?", table), Params: dynamic.Params, Rows: dynamic.Rows}
			prepared, err := session.Prepare(context.Background(), selectStatement, gocql.PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			page, err := prepared.RunPage(context.Background(), []dynamic.Value{dynamic.Int(42)}, cql.StartPage, 0, bindOptions(session, options))
			if err != nil {
				t.Fatal(err)
			}
			assertV2CustomPayload(t, customPayload, page.Info().CustomPayload())
			if err := page.Close(); err != nil {
				t.Fatal(err)
			}

			// Unprepared INSERT retains the QUERY custom-payload path.
			insert := cql.Command(fmt.Sprintf("INSERT INTO %s(id,value) VALUES(1, 1)", table))
			result, err := session.UnpreparedExec(context.Background(), insert, cql.EmptyParams{}, options)
			if err != nil {
				t.Fatal(err)
			}
			assertV2CustomPayload(t, customPayload, result.CustomPayload())

			// Native BATCH retains its independent custom-payload frame path.
			batch := &gocql.Batch{Kind: cql.LoggedBatch}
			if err := session.UnpreparedAddToBatch(batch, insert, cql.EmptyParams{}); err != nil {
				t.Fatal(err)
			}
			result, err = session.ExecBatch(context.Background(), batch, bindOptions(session, options))
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			assertV2CustomPayload(t, customPayload, result.CustomPayload())
		})
		t.Run("Values", func(t *testing.T) {
			t.Parallel()
			requireCassandraFixtures(t)
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t, session.ID())
			if err := v2CreateTable(context.Background(), session, fmt.Sprintf("CREATE TABLE %s.%s (id int, value int, PRIMARY KEY (id))", (*implSession.Session)(session).Data().InternalCfg.Keyspace, table)); err != nil {
				t.Fatal(err)
			}

			values := [][]cql.PayloadEntry{
				{{Key: "a", Value: cql.Nullable[cql.Bytes]{Valid: true, Value: cql.BorrowBytes([]byte{10, 20})}}, {Key: "b", Value: cql.Nullable[cql.Bytes]{Valid: true, Value: cql.BorrowBytes([]byte{20, 30})}}},
				nil,
				{{Key: "a", Value: cql.Nullable[cql.Bytes]{Valid: true, Value: cql.BorrowBytes([]byte{10, 20})}}, {Key: "b", Value: cql.Nullable[cql.Bytes]{}}},
			}
			statement := cql.Statement[[]dynamic.Value, dynamic.Row]{CQL: fmt.Sprintf("SELECT id FROM %s where id = ?", table), Params: dynamic.Params, Rows: dynamic.Rows}
			prepared, err := session.Prepare(context.Background(), statement, gocql.PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			for _, customPayload := range values {
				page, err := prepared.RunPage(context.Background(), []dynamic.Value{dynamic.Int(42)}, cql.StartPage, 0, bindOptions(session, gocql.Options{Consistency: cql.Option[cql.Consistency]{Value: cql.One, Set: true}, CustomPayload: customPayload}))
				if err != nil {
					t.Fatal(err)
				}
				assertV2CustomPayload(t, customPayload, page.Info().CustomPayload())
				if err := page.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	})

	t.Run("Errors", func(t *testing.T) {
		t.Parallel()

		t.Run("AlreadyExists", func(t *testing.T) {
			t.Parallel()

			session := createV2Session(t)
			defer session.Close()

			table := testTableName(t, session.ID())

			if err := v2CreateTable(context.Background(), session, fmt.Sprintf((`CREATE TABLE `+fixtureKeyspace("")+`.%s (id int primary key)`), table)); err != nil {
				t.Fatal("create:", err)
			}

			if err := v2CreateTable(context.Background(), session, fmt.Sprintf((`CREATE TABLE `+fixtureKeyspace("")+`.%s (id int primary key)`), table)); err == nil {
				t.Fatal("Should have gotten already exists error from cassandra server.")
			} else {
				if e, ok := errors.AsType[*cqlerr.AlreadyExistsError](err); ok {
					if e.Table != table {
						t.Fatalf("expected error table to be %q but was %q", table, e.Table)
					}
				} else {
					t.Fatalf("expected to get AlreadyExistsError instead got %T", e)
				}
			}
		})
		t.Run("WriteFailure", func(t *testing.T) {
			t.Parallel()

			requireCassandraFixtures(t)
			cluster := createV2Cluster()
			session, err := gocql.Connect[testDialect](context.Background(), mustBuild(t, cluster))
			if err != nil {
				t.Fatal("create session:", err)
			}
			defer session.Close()

			if err := v2CreateTable(context.Background(), session, fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': %d}", fixtureKeyspace("_fail"), *flagRF)); err != nil {
				t.Fatal(err)
			}
			table := testTableName(t, session.ID())

			if err := v2CreateTable(context.Background(), session, fmt.Sprintf("CREATE TABLE %s.%s (id int,value int,PRIMARY KEY (id))", fixtureKeyspace("_fail"), table)); err != nil {
				t.Fatalf("failed to create table with error '%v'", err)
			}
			if _, err := v2Exec(context.Background(), session, fmt.Sprintf(`INSERT INTO %s.%s (id, value) VALUES (1, 1)`, fixtureKeyspace("_fail"), table)); err != nil {
				if errWrite, ok := errors.AsType[*cqlerr.WriteFailureError](err); ok {
					if (*implSession.Session)(session).Data().InternalCfg.ProtoVersion >= protoVersion5 {
						// ErrorMap should be filled with some hosts that should've errored
						if len(errWrite.ErrorMap) == 0 {
							t.Fatal("errWrite.ErrorMap should have some failed hosts but it didn't have any")
						}
					} else {
						// Map doesn't get filled for V4
						if len(errWrite.ErrorMap) != 0 {
							t.Fatal("errWrite.ErrorMap should have length 0, it's: ", len(errWrite.ErrorMap))
						}
					}
				} else {
					t.Fatalf("error (%s) should be WriteFailureError, it's: %T", err, err)
				}
			} else {
				t.Fatal("a write fail error should have happened when querying test keyspace")
			}

			if _, err = v2Exec(context.Background(), session, "DROP KEYSPACE "+fixtureKeyspace("_fail")); err != nil {
				t.Fatal(err)
			}
		})
	})

	t.Run("Exec", func(t *testing.T) {
		t.Parallel()

		t.Run("Basic", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			require.NoError(t, v2CreateTable(ctx, session, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", table)))
			prepared, err := session.Prepare(ctx, cql.Statement[int32, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(id) VALUES (?)", table), Params: cql.OneParam(cql.Int32Codec), Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
			require.NoError(t, err)
			require.Equal(t, 1, prepared.Parameters().Len())
			require.Equal(t, table, prepared.Info().Table)

			// Query setters become per-admission Options. Trace IDs and timing belong
			// to each execution result; a statement never accumulates mutable metrics.
			zero := gocql.Result{}
			require.Zero(t, zero.Attempts)
			require.Zero(t, zero.Latency)
			retryPolicy := sessionTestRetry{action: retry.Next, maxAttempts: 3}
			observer := new(sessionTestObserver)
			options := gocql.Options{Consistency: cql.Option[cql.Consistency]{Value: cql.All, Set: true}, SerialConsistency: gocql.SerialLocal, PageSize: cql.Option[int32]{Value: 10, Set: true}, Prefetch: cql.Option[int]{Value: 1, Set: true}, Retry: retryPolicy, Trace: true, Observer: observer}
			resolved, err := resolveOptions(session, options)
			require.NoError(t, err)
			require.Equal(t, cql.All, resolved.InternalProfile.Consistency)
			require.Equal(t, cql.LocalSerial, resolved.InternalProfile.SerialConsistency.Value)
			require.Equal(t, int32(10), resolved.InternalProfile.PageSize)
			require.Equal(t, 1, resolved.InternalProfile.Prefetch)
			require.Equal(t, retryPolicy, resolved.InternalProfile.Retry)
			serialValues := options
			serialValues.Consistency.Value = cql.LocalSerial
			serialRead := serialValues
			resolvedSerial, err := resolveOptions(session, serialRead)
			require.NoError(t, err)
			require.Equal(t, cql.LocalSerial, resolvedSerial.InternalProfile.Consistency)

			for _, id := range []int32{1, 2, 1} {
				result, err := prepared.Exec(ctx, id, bindOptions(session, options))
				require.NoError(t, err)
				require.Equal(t, 1, result.Attempts)
				require.Greater(t, result.Latency, time.Duration(0))
				require.Equal(t, cql.All, result.AchievedConsistency)
				require.NotEmpty(t, result.TraceIDs)
			}
			requests, attempts := observer.snapshot()
			require.Len(t, requests, 3)
			require.Len(t, attempts, 3)
			for i := range attempts {
				require.Equal(t, 1, attempts[i].Attempt)
				require.NoError(t, attempts[i].Err)
			}
			for _, id := range []int32{1, 2} {
				row, err := v2ReadOne(ctx, session, fmt.Sprintf("SELECT id FROM %s WHERE id=?", table), dynamic.Int(id))
				require.NoError(t, err)
				require.Equal(t, int64(id), sessionTestInt(t, row, "id"))
			}
		})
		t.Run("JSON", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			if (*implSession.Session)(session).Data().InternalCfg.ProtoVersion < 4 {
				t.Skip("skipping JSON support on proto < 4")
			}
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id text PRIMARY KEY,age int,state text)", table))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s JSON ?", table), codecTestMust(dynamic.Text(`{"id": "user123", "age": 42, "state": "TX"}`)))
			row := cassandraRead(t, session, fmt.Sprintf("SELECT id,age,state FROM %s WHERE id=?", table), codecTestMust(dynamic.Text("user123")))
			require.Equal(t, "user123", cassandraText(t, row, "id"))
			require.Equal(t, int64(42), cassandraInt(t, row, "age"))
			require.Equal(t, "TX", cassandraText(t, row, "state"))
		})
		t.Run("WhereIn", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int,cluster int,PRIMARY KEY(id,cluster))", table))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,cluster) VALUES(?,?)", table), dynamic.Int(100), dynamic.Int(200))
			row := cassandraRead(t, session, fmt.Sprintf("SELECT * FROM %s WHERE id=? AND cluster IN (?)", table), dynamic.Int(100), dynamic.Int(200))
			require.Equal(t, int64(100), cassandraInt(t, row, "id"))
			require.Equal(t, int64(200), cassandraInt(t, row, "cluster"))
		})
	})

	t.Run("Options", func(t *testing.T) {
		// Ensures that queries are sent to the specified host only.
		// WARNING: This test must NOT use t.Parallel(). It calls pool.host.setState(NodeDown)
		// which mutates shared HostInfo state visible to all concurrent sessions.
		//
		//nolint:paralleltest // mutates shared HostInfo state via setState(NodeDown)
		t.Run("HostID", func(t *testing.T) {
			session := createV2Session(t)
			defer session.Close()
			waitForHostConns(t, session)
			const hostIDQuery = "SELECT host_id FROM system.local"
			statement := cql.Statement[cql.EmptyParams, cql.UUID]{CQL: hostIDQuery, Params: cql.EmptyParamsEncoder, Rows: cql.OneColumn(cql.UUIDCodec)}
			runOn := func(options gocql.Options) error {
				var id cql.UUID
				_, err := session.UnpreparedEach(context.Background(), statement, cql.EmptyParams{}, &id, func(*cql.UUID) error { return nil }, options)
				return err
			}
			hosts := session.Metadata().Hosts()
			for _, host := range hosts {
				for i := 0; i < 5; i++ {
					require.Equal(t, host.ID, cassandraUnpreparedOne(t, session, hostIDQuery, cql.OneColumn(cql.UUIDCodec), gocql.Options{HostID: cql.Option[cql.UUID]{Value: host.ID, Set: true}}))
				}
			}
			_, err := cql.ParseUUID("[invalid]")
			require.Error(t, err, "invalid host IDs are rejected while constructing typed UUIDs")
			_, err = session.CompileOptions(gocql.Options{HostID: cql.Option[cql.UUID]{Value: cql.UUID{}, Set: true}})
			var invalidHost *cqlerr.ValidationError
			require.ErrorAs(t, err, &invalidHost, "an empty host ID is rejected when options are built")
			err = runOn(gocql.Options{HostID: cql.Option[cql.UUID]{Value: cql.RandomUUID(), Set: true}})
			require.ErrorIs(t, err, cqlerr.ErrNoConnections)
			host := hosts[0]
			pool, ok := (*implSession.Session)(session).Data().InternalPool.InternalGetPoolByHostID(host.ID.String())
			require.True(t, ok)
			implEndpoint.InternalHostInfoSetState(pool.InternalHost, NodeDown)
			defer implEndpoint.InternalHostInfoSetState(pool.InternalHost, NodeUp)
			err = runOn(gocql.Options{HostID: cql.Option[cql.UUID]{Value: host.ID, Set: true}})
			require.ErrorIs(t, err, cqlerr.ErrNoConnections)
		})
		t.Run("Keyspace", func(t *testing.T) {
			session := createV2Session(t)
			defer session.Close()
			if (*implSession.Session)(session).Data().InternalCfg.ProtoVersion < 5 {
				t.Skip("keyspace for QUERY message is not supported in protocol < 5")
			}
			ks := testKeyspaceName(t)
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE KEYSPACE %s WITH replication={'class':'SimpleStrategy','replication_factor':'1'}", ks))
			defer cassandraDDL(t, session, "DROP KEYSPACE "+ks)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s.%s(id int PRIMARY KEY,value text)", ks, table))
			write := cassandraWritePrepared(t, session, fmt.Sprintf("INSERT INTO %s(id,value) VALUES(?,?)", table), gocql.PrepareOptions{Keyspace: ks})
			_, err := write.Exec(context.Background(), []dynamic.Value{dynamic.Int(1), codecTestMust(dynamic.Text("text"))}, bindOptions(session, gocql.Options{Keyspace: ks}))
			require.NoError(t, err)
			prepared := cassandraPrepared(t, session, cql.Statement[cql.EmptyParams, dynamic.Row]{CQL: "SELECT * FROM " + table, Params: cql.EmptyParamsEncoder, Rows: dynamic.Rows}, gocql.PrepareOptions{Keyspace: ks})
			for _, row := range []dynamic.Row{cassandraReadOne(t, session, prepared, cql.EmptyParams{}, gocql.Options{Keyspace: ks}), cassandraUnpreparedOne(t, session, "SELECT * FROM "+table, dynamic.Rows, gocql.Options{Keyspace: ks})} {
				require.Equal(t, int64(1), cassandraInt(t, row, "id"))
				require.Equal(t, "text", cassandraText(t, row, "value"))
			}
		})
		t.Run("NowInSeconds", func(t *testing.T) {
			session := createV2Session(t)
			defer session.Close()
			if (*implSession.Session)(session).Data().InternalCfg.ProtoVersion < 5 {
				t.Skip("Query now in seconds are only available on protocol >= 5")
			}
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,val text)", table))
			write := cassandraWritePrepared(t, session, fmt.Sprintf("INSERT INTO %s(id,val) VALUES(?,?) USING TTL 20", table), gocql.PrepareOptions{})
			_, err := write.Exec(context.Background(), []dynamic.Value{dynamic.Int(1), codecTestMust(dynamic.Text("val"))}, bindOptions(session, gocql.Options{NowInSeconds: cql.Option[int32]{Value: 0, Set: true}}))
			require.NoError(t, err)
			read := cassandraPrepared(t, session, cql.Statement[int32, int32]{CQL: fmt.Sprintf("SELECT TTL(val) FROM %s WHERE id=?", table), Params: cql.OneParam(cql.Int32Codec), Rows: cql.OneColumn(cql.Int32Codec)}, gocql.PrepareOptions{})
			require.Equal(t, int32(10), cassandraReadOne(t, session, read, int32(1), gocql.Options{NowInSeconds: cql.Option[int32]{Value: 10, Set: true}}))
		})
	})

	t.Run("Result", func(t *testing.T) {
		t.Parallel()

		// Confirms that host is added to Iter when the query succeeds.
		t.Run("HostID", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			rows, err := v2Query(context.Background(), session, "SELECT * FROM system.peers")
			require.NoError(t, err)
			defer rows.Close()
			require.NotEqual(t, cql.UUID{}, rows.Info().HostID)
		})
		// Confirms that the stats are returning valid data. Accuracy may be questionable.
		t.Run("Stats", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			before := session.Metrics()
			rows, err := v2Query(context.Background(), session, "SELECT * FROM system.peers")
			require.NoError(t, err)
			var row dynamic.Row
			for rows.NextInto(&row) {
			}
			require.NoError(t, rows.Close())
			info := rows.Info()
			require.Positive(t, info.Attempts)
			require.Positive(t, info.Latency)
			require.Greater(t, session.Metrics().Requests, before.Requests)
		})
	})

	t.Run("Retry", func(t *testing.T) {
		t.Parallel()

		t.Run("Idempotence", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			for _, tc := range []struct {
				name       string
				idempotent bool
				attempts   int
			}{{"with retry", true, 6}, {"without retry", false, 1}} {
				t.Run(tc.name, func(t *testing.T) {
					observer := &cassandraObserver{}
					insert := cql.Command("INSERT INTO " + fixtureKeyspace("") + ".not_existing_table(event_id,time,args) VALUES(4,now(),'test')")
					_, err := session.UnpreparedExec(context.Background(), insert, cql.EmptyParams{}, gocql.Options{Idempotent: tc.idempotent, Retry: cassandraRetry{}, Consistency: cql.Option[cql.Consistency]{Value: cql.All, Set: true}, Observer: observer})
					require.Error(t, err)
					events, attempts := observer.snapshot()
					require.Len(t, attempts, tc.attempts)
					require.Len(t, events, 1)
				})
			}
		})
		// The old Ignore mode stopped retries but preserved errors. Its v2 equivalent
		// is retry.Stop; retry.IgnoreWrite is a separate explicit error-suppression policy.
		t.Run("IgnoreRethrow", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			ctx := context.Background()
			insert := cql.Command("INSERT INTO " + fixtureKeyspace("") + ".invalid_table(value) VALUES(1)")
			for _, mode := range []string{"Ignore", "Rethrow"} {
				for _, retries := range []int{0, 1} {
					t.Run(fmt.Sprintf("%s/%d", mode, retries), func(t *testing.T) {
						observer := new(sessionTestObserver)
						retryPolicy := sessionTestRetry{action: retry.Stop, maxAttempts: retries + 1}
						_, err := session.UnpreparedExec(ctx, insert, cql.EmptyParams{}, gocql.Options{Idempotent: true, Retry: retryPolicy, Observer: observer})
						require.Error(t, err)
						var server cqlerr.ServerError
						require.ErrorAs(t, err, &server)
						requests, attempts := observer.snapshot()
						require.Len(t, requests, 1)
						require.Len(t, attempts, 1)
						require.Error(t, requests[0].Err)
						require.Error(t, attempts[0].Err)
						require.Equal(t, 1, requests[0].Result.Attempts)
					})
				}
			}
		})
	})

	t.Run("Reuse", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		config := createV2Cluster()
		session, err := gocql.Connect[testDialect](ctx, mustBuild(t, config))
		if err != nil {
			t.Fatal(err)
		}
		defer session.Close()
		namespace := fmt.Sprintf("gocql_query_%x", time.Now().UnixNano())
		exec := func(ctx context.Context, stmt string) error {
			_, err := session.UnpreparedExec(ctx, cql.Command(stmt), cql.EmptyParams{})
			return err
		}
		if err := exec(ctx, fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class':'NetworkTopologyStrategy','replication_factor':%d}", namespace, *flagRF)); err != nil {
			t.Fatal(err)
		}
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			dropErr := exec(cleanup, "DROP KEYSPACE "+namespace)
			// DDL acknowledgement and its automatic metadata refresh have distinct
			// outcomes. Verify the actual cleanup without replaying successful DDL.
			verify, finish := context.WithTimeout(context.Background(), 30*time.Second)
			defer finish()
			if err := session.AwaitSchemaAgreement(verify); err != nil {
				t.Error(err)
				return
			}
			typ := cql.TextType
			var keyspaceName string
			found, _, err := session.UnpreparedOne(verify, cql.Statement[string, string]{Kind: cql.OrdinaryStatement, CQL: "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name=?", Params: cql.OneParam(cql.StringCodec), Rows: cql.OneColumn(cql.StringCodec), BindMarkers: cql.NewColumns(cql.ColumnSpec{Name: "keyspace_name", Type: typ})}, namespace, &keyspaceName)
			if err != nil {
				t.Error(err)
				return
			}
			if found {
				t.Error("owned namespace survived cleanup", namespace, dropErr)
			}
			if dropErr != nil {
				t.Logf("drop verified after post-DDL error: %v", dropErr)
			}
		}()
		table := namespace + ".records"
		if err := exec(ctx, "CREATE TABLE "+table+"(k int,c int,p blob,PRIMARY KEY(k,c))"); err != nil {
			t.Fatal(err)
		}
		if err := session.AwaitSchemaAgreement(ctx); err != nil {
			t.Fatal(err)
		}
		for attempt := 0; ; attempt++ {
			err := session.RefreshMetadata(ctx)
			if err == nil {
				break
			}
			if partial, ok := errors.AsType[*cqlerr.PartialRefreshError](err); attempt >= 2 || !ok || len(partial.Keyspaces) != 1 || partial.Keyspaces["<publication>"] == nil {
				t.Fatal(err)
			}
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				t.Fatal(ctx.Err())
			case <-timer.C:
			}
		}
		type record struct {
			K, C int32
			P    []byte
		}
		write := codecTestMust(session.Prepare(ctx, cql.Statement[record, cql.EmptyRow]{Kind: cql.OrdinaryStatement, CQL: "INSERT INTO " + table + "(k,c,p)VALUES(?,?,?)", Params: cql.Params(cql.Param("k", cql.Int32Codec, func(r record) int32 { return r.K }), cql.Param("c", cql.Int32Codec, func(r record) int32 { return r.C }), cql.Param("p", cql.ByteSliceCodec, func(r record) []byte { return r.P })), Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{}))
		w := codecTestMust(write.Query(gocql.Options{}))
		defer w.Close()
		payload := bytes.Repeat([]byte{42}, 4096)
		for i := int32(0); i < 6; i++ {
			if _, err := w.Exec(ctx, record{K: 1, C: i, P: payload}); err != nil {
				t.Fatal(err)
			}
		}
		decoder := cql.RowInto(func(*record) {}, cql.Column("c", cql.Int32Codec, func(r *record) *int32 { return &r.C }), cql.Column("p", cql.ByteSliceCodec, func(r *record) *[]byte { return &r.P }))
		read := codecTestMust(session.Prepare(ctx, cql.Statement[int32, record]{Kind: cql.OrdinaryStatement, CQL: "SELECT c,p FROM " + table + " WHERE k=?", Params: cql.OneParam(cql.Int32Codec), Rows: decoder}, gocql.PrepareOptions{}))
		q := codecTestMust(read.Query(gocql.Options{PageSize: cql.Option[int32]{Set: true, Value: 2}, Prefetch: cql.Option[int]{Set: true, Value: 0}}))
		defer q.Close()
		var previous gocql.Rows[record]
		for range 2 {
			rows := codecTestMust(q.Run(ctx, int32(1)))
			// A stale handle from the previous execution must not touch this one.
			if err := previous.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := q.Run(ctx, int32(1)); !errors.Is(err, cqlerr.ErrBusy) {
				t.Fatal(err)
			}
			dst := record{P: make([]byte, 0, len(payload))}
			backing := &dst.P[:cap(dst.P)][0]
			count := 0
			for rows.NextInto(&dst) {
				if dst.C != int32(count) || !bytes.Equal(dst.P, payload) || &dst.P[0] != backing {
					t.Fatal("owned destination mismatch", count)
				}
				count++
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			if count != 6 {
				t.Fatal("row count", count)
			}
			previous = rows
		}
		state := cql.StartPage
		pageRows := 0
		var stalePage *gocql.Page[record]
		for {
			page := codecTestMust(q.RunPage(ctx, int32(1), state, 0))
			if stalePage != nil {
				_ = stalePage.Close()
			}
			var dst record
			for page.NextInto(&dst) {
				if dst.C != int32(pageRows) {
					t.Fatal("explicit page order")
				}
				pageRows++
			}
			state = page.State()
			if err := page.Close(); err != nil {
				t.Fatal(err)
			}
			stalePage = page
			if state.Complete() {
				break
			}
		}
		if pageRows != 6 {
			t.Fatal("explicit paging count", pageRows)
		}
		t.Log("query reuse, stale cursors, owned destinations and explicit paging passed")
		unpaged := codecTestMust(read.Query(gocql.Options{PageSize: cql.Some(cql.Unpaged), Prefetch: cql.Option[int]{Set: true, Value: 0}}))
		defer unpaged.Close()
		page := codecTestMust(unpaged.RunPage(ctx, int32(1), cql.StartPage, 0))
		n := 0
		var unpagedRow record
		for page.NextInto(&unpagedRow) {
			n++
		}
		if err := page.Close(); err != nil || n != 6 {
			t.Fatal("unpaged", err, n)
		}
		for _, incremental := range []bool{false, true} {
			streamDecoder := stream.RowInto(func(*record) {}, stream.Column("c", stream.Int32, func(r *record) *int32 { return &r.C }), stream.Column("p", stream.Blob, func(r *record) *[]byte { return &r.P }))
			readStream := codecTestMust(session.PrepareStream(ctx, stream.Statement[int32, record]{Kind: cql.OrdinaryStatement, CQL: "SELECT c,p FROM " + table + " WHERE k=?", Params: cql.OneParam(cql.Int32Codec), Rows: streamDecoder}))
			streamQuery := codecTestMust(readStream.Query(gocql.StreamOptions{Incremental: incremental, Options: gocql.Options{PageSize: cql.Option[int32]{Set: true, Value: 2}}}))
			cursor, err := streamQuery.Run(ctx, int32(1))
			if incremental && *flagProto < 5 && fixtureCompressed() {
				if _, ok := errors.AsType[*cqlerr.StreamCapabilityError](err); !ok {
					t.Fatal(err)
				}
				streamQuery.Close()
				if err := streamQuery.Wait(ctx); err != nil {
					t.Fatal(err)
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			dst := record{P: make([]byte, 0, len(payload))}
			count := 0
			for cursor.NextInto(&dst) {
				if dst.C != int32(count) || !bytes.Equal(dst.P, payload) {
					t.Fatal("stream mismatch", incremental, count)
				}
				count++
			}
			if err := cursor.Close(); err != nil {
				t.Fatal(err)
			}
			streamQuery.Close()
			if err := streamQuery.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			if count != 6 {
				t.Fatal("stream count", count)
			}
			t.Logf("stream receive incremental=%t passed", incremental)
		}
		batch := gocql.Batch{Kind: cql.UnloggedBatch}
		for i := int32(0); i < 3; i++ {
			if err := w.AddToBatch(&batch, record{K: 2, C: i, P: payload}); err != nil {
				t.Fatal(err)
			}
		}
		payload[0] = 99
		if _, err := session.ExecBatch(ctx, &batch); err != nil {
			t.Fatal(err)
		}
		rows := codecTestMust(q.Run(ctx, int32(2)))
		defer rows.Close()
		var dst record
		count := 0
		for rows.NextInto(&dst) {
			if len(dst.P) != 4096 || dst.P[0] != 42 {
				t.Fatal("batch snapshot mutated")
			}
			count++
		}
		if rows.Err() != nil || count != 3 {
			t.Fatal(rows.Err(), count)
		}
		t.Log("owned batch entry snapshots passed")
	})

	t.Run("ShouldPrepare", func(t *testing.T) {
		t.Parallel()

		t.Run("Keywords", func(t *testing.T) {
			t.Parallel()
			toPrepare := []string{"select * ", "INSERT INTO", "update table", "delete from", "begin batch"}
			cantPrepare := []string{"create table", "USE table", "LIST keyspaces", "alter table", "drop table", "grant user", "revoke user"}
			// Preparation is explicit in v2. Preserve every old classification input:
			// non-USE statements are accepted unprepared without keyword guessing, and explicit Prepare
			// reaches connection selection even for formerly unprepared DDL.
			session := func() *gocql.Session[testDialect] {
				opaqueStorageValuex := (*gocql.Session[testDialect])(implSession.InternalWrapSession(new(implCluster.Core)))
				(*implSession.Session)(opaqueStorageValuex).Data().InternalLogger = logging.Nop()
				return opaqueStorageValuex
			}()
			defer session.Close()
			for _, stmt := range append(toPrepare, cantPrepare...) {
				t.Run(stmt, func(t *testing.T) {
					statement := cql.Command(stmt)
					err := session.UnpreparedAddToBatch(&gocql.Batch{}, statement, cql.EmptyParams{})
					if firstCQLWord(stmt) == "USE" {
						require.ErrorIs(t, err, cqlerr.ErrUseStmt)
						_, err = session.Prepare(context.Background(), statement, gocql.PrepareOptions{})
						require.ErrorIs(t, err, cqlerr.ErrUseStmt)
						return
					}
					require.NoError(t, err)
					_, err = session.Prepare(context.Background(), statement, gocql.PrepareOptions{})
					require.ErrorIs(t, err, cqlerr.ErrNoConnections)
				})
			}
		})
		t.Run("MultilineStatements", func(t *testing.T) {
			t.Parallel()

			var shouldPrepareTests = []struct {
				Stmt   string
				Result bool
			}{
				{`
      BEGIN BATCH
        INSERT INTO users (userID, password)
        VALUES ('smith', 'secret')
      APPLY BATCH
    ;
      `, true},
				{`INSERT INTO users (userID, password, name) VALUES ('user2', 'ch@ngem3b', 'second user')`, true},
				{`BEGIN COUNTER BATCH UPDATE stats SET views = views + 1 WHERE pageid = 1 APPLY BATCH`, true},
				{`delete name from users where userID = 'smith';`, true},
				{`  UPDATE users SET password = 'secret' WHERE userID = 'smith'   `, true},
				{`CREATE TABLE users (
        user_name varchar PRIMARY KEY,
        password varchar,
        gender varchar,
        session_token varchar,
        state varchar,
        birth_year bigint
      );`, false},
			}

			// Automatic keyword-based preparation was removed: every original CQL
			// case can now be bound explicitly as QUERY, and Prepare explicitly asks
			// the server for metadata, including DDL previously classified false.
			session := func() *gocql.Session[testDialect] {
				opaqueStorageValue := (*gocql.Session[testDialect])(implSession.InternalWrapSession(new(implCluster.Core)))
				(*implSession.Session)(opaqueStorageValue).Data().InternalLogger = logging.Nop()
				return opaqueStorageValue
			}()
			defer session.Close()
			for _, test := range shouldPrepareTests {
				t.Run(fmt.Sprintf("formerly_prepared_%t_%s", test.Result, firstCQLWord(test.Stmt)), func(t *testing.T) {
					statement := cql.Command(test.Stmt)
					if err := session.UnpreparedAddToBatch(&gocql.Batch{}, statement, cql.EmptyParams{}); err != nil {
						t.Fatal(err)
					}
					if _, err := session.Prepare(context.Background(), statement, gocql.PrepareOptions{}); !errors.Is(err, cqlerr.ErrNoConnections) {
						t.Fatalf("explicit Prepare must attempt a connection for %q: %v", test.Stmt, err)
					}
				})
			}
		})
	})
}
