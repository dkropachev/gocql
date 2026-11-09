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

	"github.com/stretchr/testify/require"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
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

			for _, tc := range testCases {
				bound, err := BindUnpreparedOwned(cql.Command(tc.input), cql.NewColumns(), cql.EmptyParams{})
				if bound != nil {
					bound.Close()
				}
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
			prepared := cassandraPrepared(t, session, cql.Statement[[]cql.ValueField, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(id,value) VALUES(:id,:value)", table), Params: cql.NamedDynamicParams, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
			bound := cassandraBound(t, prepared, []cql.ValueField{{Name: "value", Value: codecTestMust(cql.TextValue("i am a value"))}, {Name: "id", Value: cql.IntValue(1)}})
			_, err := Exec(context.Background(), session, bound, gocql.SessionOptions{})
			require.NoError(t, err)
			read := cassandraPrepared(t, session, cql.Statement[[]cql.ValueField, cql.Text]{CQL: fmt.Sprintf("SELECT value FROM %s WHERE id=:id", table), Params: cql.NamedDynamicParams, Rows: cql.OneColumn(cql.TextCodec)}, gocql.PrepareOptions{})
			require.Equal(t, "i am a value", cassandraReadBound(t, session, cassandraBound(t, read, []cql.ValueField{{Name: "id", Value: cql.IntValue(1)}}), gocql.Options{}).String())
		})
		// Tests to see that a nil value passed to a query is handled by Cassandra
		// TODO validate the nil value by reading back the nil. Need to fix Unmarshalling.
		t.Run("Nil", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,count int)", table))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,count) VALUES(?,?)", table), cql.IntValue(1), cql.NullValue(cql.IntValue(0).Type()))
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
			read := cassandraPrepared(t, session, cql.Statement[[]cql.Value, cql.DynamicRow]{CQL: fmt.Sprintf("SELECT * FROM %s WHERE id=? AND cluster=?", table), Params: cql.DynamicParams, Rows: cql.DynamicRows}, gocql.PrepareOptions{})
			_, err := BindOwned(read, []cql.Value{cql.IntValue(1)})
			require.Error(t, err)
			write := cassandraPrepared(t, session, cql.Statement[[]cql.Value, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(id,cluster,value) VALUES(?,?,?)", table), Params: cql.DynamicParams, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
			_, err = BindOwned(write, []cql.Value{cql.IntValue(1), cql.IntValue(2)})
			require.Error(t, err, "invalid batch entry must fail during binding")
		})
		// Makes sure that the application can manually bind query parameters using the query meta data supplied at runtime
		t.Run("ParamsMetadata", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int,cluster int,value text,PRIMARY KEY(id,cluster))", table))
			params := cql.Params(cql.Param("value", cql.StringEncoder, func(v ClusteredKeyValue) string { return v.Value }), cql.Param("cluster", cql.Int32, func(v ClusteredKeyValue) int32 { return v.Cluster }), cql.Param("id", cql.Int32, func(v ClusteredKeyValue) int32 { return v.Id }))
			prepared := cassandraPrepared(t, session, cql.Statement[ClusteredKeyValue, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(id,cluster,value) VALUES(?,?,?)", table), Params: params, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
			require.Equal(t, 3, prepared.Parameters().Len())
			_, err := Exec(context.Background(), session, cassandraBound(t, prepared, ClusteredKeyValue{200, 300, "baz"}), gocql.SessionOptions{})
			require.NoError(t, err)
			readParams := cql.Params(cql.Param("cluster", cql.Int32, func(v ClusteredKeyValue) int32 { return v.Cluster }), cql.Param("id", cql.Int32, func(v ClusteredKeyValue) int32 { return v.Id }))
			read := cassandraPrepared(t, session, cql.Statement[ClusteredKeyValue, cql.DynamicRow]{CQL: fmt.Sprintf("SELECT id,cluster,value FROM %s WHERE id=? AND cluster=?", table), Params: readParams, Rows: cql.DynamicRows}, gocql.PrepareOptions{})
			row := cassandraReadBound(t, session, cassandraBound(t, read, ClusteredKeyValue{Id: 200, Cluster: 300}), gocql.Options{})
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
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,value) VALUES(?,?)", table), cql.IntValue(23), codecTestMust(cql.TextValue("quux")))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,value) VALUES(?,?)", table), cql.IntValue(24), codecTestMust(cql.TextValue("w00t")))
			prepared := cassandraPrepared(t, session, cql.Statement[int32, cql.Text]{CQL: fmt.Sprintf("SELECT value FROM %s WHERE id=?", table), Params: cql.OneParam(cql.Int32), Rows: cql.OneColumn(cql.TextCodec)}, gocql.PrepareOptions{})
			first := cassandraBound(t, prepared, int32(23))
			second := cassandraBound(t, prepared, int32(24))
			require.Equal(t, "quux", cassandraReadBound(t, session, first, gocql.Options{}).String())
			require.Equal(t, "w00t", cassandraReadBound(t, session, second, gocql.Options{}).String())
			require.Equal(t, "quux", cassandraReadBound(t, session, first, gocql.Options{}).String())
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
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,value) VALUES(?,?)", table), cql.IntValue(113), codecTestMust(cql.TextValue("foo")))
			prepared := cassandraPrepared(t, session, cql.Statement[int32, cql.DynamicRow]{CQL: fmt.Sprintf("SELECT id,value FROM %s WHERE id=?", table), Params: cql.OneParam(cql.Int32), Rows: cql.DynamicRows}, gocql.PrepareOptions{})
			require.Equal(t, 1, prepared.Parameters().Len())
			bound := cassandraBound(t, prepared, int32(113))
			for i := 0; i < 2; i++ {
				row := cassandraReadBound(t, session, bound, gocql.Options{})
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
			read := cassandraPrepared(t, session, cql.Statement[[]cql.Value, cql.DynamicRow]{CQL: fmt.Sprintf("SELECT * FROM %s WHERE id=?", table), Params: cql.DynamicParams, Rows: cql.DynamicRows}, gocql.PrepareOptions{})
			_, err := BindOwned(read, []cql.Value{cql.IntValue(1), cql.IntValue(2)})
			require.Error(t, err)
			write := cassandraPrepared(t, session, cql.Statement[[]cql.Value, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(id,value) VALUES(?,?)", table), Params: cql.DynamicParams, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
			_, err = BindOwned(write, []cql.Value{cql.IntValue(1), cql.IntValue(2), cql.IntValue(3)})
			require.Error(t, err, "invalid batch entry must fail during binding")
		})
		// Verify unset column will not replace an existing column
		t.Run("UnsetColumn", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			if implCluster.SessionData(session).InternalCfg.ProtoVersion < 4 {
				t.Skip("Unset Values are not supported in protocol < 4")
			}
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,my_int int,my_text text)", table))
			stmt := fmt.Sprintf("INSERT INTO %s(id,my_int,my_text) VALUES(?,?,?)", table)
			cassandraWrite(t, session, stmt, cql.IntValue(1), cql.IntValue(2), codecTestMust(cql.TextValue("3")))
			cassandraWrite(t, session, stmt, cql.IntValue(1), cql.UnsetDynamic(cql.IntValue(0).Type()), cql.UnsetDynamic(codecTestMust(cql.TextValue("")).Type()))
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

			if err := v2CreateTable(context.Background(), session, fmt.Sprintf("CREATE TABLE %s.%s (id int, value int, PRIMARY KEY (id))", implCluster.SessionData(session).InternalCfg.Keyspace, table)); err != nil {
				t.Fatal(err)
			}

			customPayload := []cql.PayloadEntry{{Key: "a", Value: cql.Nullable[cql.Bytes]{Valid: true, Value: cql.BorrowBytes([]byte{10, 20})}}, {Key: "b", Value: cql.Nullable[cql.Bytes]{Valid: true, Value: cql.BorrowBytes([]byte{20, 30})}}}
			options := gocql.Options{Consistency: cql.Option[cql.Consistency]{Value: cql.One, Set: true}, CustomPayload: customPayload}

			// Prepared SELECT retains the EXECUTE custom-payload path.
			selectStatement := cql.Statement[[]cql.Value, cql.DynamicRow]{CQL: fmt.Sprintf("SELECT id FROM %s where id = ?", table), Params: cql.DynamicParams, Rows: cql.DynamicRows}
			prepared, err := gocql.Prepare(context.Background(), session, selectStatement, gocql.PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			bound, err := BindOwned(prepared, []cql.Value{cql.IntValue(42)})
			if err != nil {
				t.Fatal(err)
			}
			defer bound.Close()
			page, err := RunPage(context.Background(), session, bound, gocql.StartPage, bindOptions(session, options))
			if err != nil {
				t.Fatal(err)
			}
			assertV2CustomPayload(t, customPayload, page.Info().CustomPayload())
			if err := page.Close(); err != nil {
				t.Fatal(err)
			}

			// Unprepared INSERT retains the QUERY custom-payload path.
			insert, err := BindUnpreparedOwned(cql.Command(fmt.Sprintf("INSERT INTO %s(id,value) VALUES(1, 1)", table)), cql.NewColumns(), cql.EmptyParams{})
			if err != nil {
				t.Fatal(err)
			}
			defer insert.Close()
			result, err := Exec(context.Background(), session, insert, bindOptions(session, options))
			if err != nil {
				t.Fatal(err)
			}
			assertV2CustomPayload(t, customPayload, result.CustomPayload())

			// Native BATCH retains its independent custom-payload frame path.
			item, err := BatchEntry(insert)
			if err != nil {
				t.Fatal(err)
			}
			result, err = execBatch(context.Background(), session, cql.LoggedBatch, []batchItem{item}, bindOptions(session, options))
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
			if err := v2CreateTable(context.Background(), session, fmt.Sprintf("CREATE TABLE %s.%s (id int, value int, PRIMARY KEY (id))", implCluster.SessionData(session).InternalCfg.Keyspace, table)); err != nil {
				t.Fatal(err)
			}

			values := [][]cql.PayloadEntry{
				{{Key: "a", Value: cql.Nullable[cql.Bytes]{Valid: true, Value: cql.BorrowBytes([]byte{10, 20})}}, {Key: "b", Value: cql.Nullable[cql.Bytes]{Valid: true, Value: cql.BorrowBytes([]byte{20, 30})}}},
				nil,
				{{Key: "a", Value: cql.Nullable[cql.Bytes]{Valid: true, Value: cql.BorrowBytes([]byte{10, 20})}}, {Key: "b", Value: cql.Nullable[cql.Bytes]{}}},
			}
			statement := cql.Statement[[]cql.Value, cql.DynamicRow]{CQL: fmt.Sprintf("SELECT id FROM %s where id = ?", table), Params: cql.DynamicParams, Rows: cql.DynamicRows}
			prepared, err := gocql.Prepare(context.Background(), session, statement, gocql.PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			bound, err := BindOwned(prepared, []cql.Value{cql.IntValue(42)})
			if err != nil {
				t.Fatal(err)
			}
			defer bound.Close()
			for _, customPayload := range values {
				page, err := RunPage(context.Background(), session, bound, gocql.StartPage, bindOptions(session, gocql.Options{Consistency: cql.Option[cql.Consistency]{Value: cql.One, Set: true}, CustomPayload: customPayload}))
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
				if e, ok := errors.AsType[*cqlerr.RequestErrAlreadyExists](err); ok {
					if e.Table != table {
						t.Fatalf("expected error table to be %q but was %q", table, e.Table)
					}
				} else {
					t.Fatalf("expected to get RequestErrAlreadyExists instead got %T", e)
				}
			}
		})
		t.Run("WriteFailure", func(t *testing.T) {
			t.Parallel()

			requireCassandraFixtures(t)
			cluster := createV2Cluster()
			session, err := gocql.Connect(context.Background(), mustBuild(t, cluster))
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
				if errWrite, ok := errors.AsType[*cqlerr.RequestErrWriteFailure](err); ok {
					if implCluster.SessionData(session).InternalCfg.ProtoVersion >= protoVersion5 {
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
					t.Fatalf("error (%s) should be RequestErrWriteFailure, it's: %T", err, err)
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
			prepared, err := gocql.Prepare(ctx, session, cql.Statement[int32, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(id) VALUES (?)", table), Params: cql.OneParam(cql.Int32), Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
			require.NoError(t, err)
			require.Equal(t, 1, prepared.Parameters().Len())
			require.Equal(t, table, prepared.Info().Table)
			original, err := BindOwned(prepared, int32(1))
			require.NoError(t, err)
			defer original.Close()
			rebound, err := BindOwned(prepared, int32(2))
			require.NoError(t, err)
			defer rebound.Close()

			// Query setters become per-admission Options. Trace IDs and timing belong
			// to each execution result; a Bound never accumulates mutable metrics.
			zero := gocql.Result{}
			require.Zero(t, zero.Attempts)
			require.Zero(t, zero.Latency)
			retryPolicy := sessionTestRetry{action: retry.Next, maxAttempts: 3}
			observer := new(sessionTestObserver)
			options := gocql.Options{Consistency: cql.Option[cql.Consistency]{Value: cql.All, Set: true}, SerialConsistency: cql.Some(cql.LocalSerial), PageSize: cql.Option[int32]{Value: 10, Set: true}, Prefetch: cql.Option[int]{Value: 1, Set: true}, Retry: retryPolicy, Trace: true, Observer: observer}
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

			for _, bound := range []*Bound[cql.EmptyRow]{original, rebound, original} {
				result, err := Exec(ctx, session, bound, bindOptions(session, options))
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
				row, err := v2ReadOne(ctx, session, fmt.Sprintf("SELECT id FROM %s WHERE id=?", table), cql.IntValue(id))
				require.NoError(t, err)
				require.Equal(t, int64(id), sessionTestInt(t, row, "id"))
			}
			// Admission copies the profile, so a request keeps the values it was
			// admitted with.
			profile := *runtimeFor(session).InternalProfile
			profile.PageSize = 10
			_, cancel, admitted, err := admitExecution(ctx, session, original, bindOptions(session, gocql.Options{Profile: &profile}))
			require.NoError(t, err)
			profile.PageSize = 100
			require.Equal(t, int32(10), admitted.InternalOptions.InternalProfile.PageSize)
			admitted.InternalLogical.InternalFinish(gocql.Result{}, nil)
			admitted.InternalOwner.InternalRelease()
			cancel()
			original.Close()
			require.NoError(t, original.Wait(ctx))
			_, err = Exec(ctx, session, original, gocql.SessionOptions{})
			require.ErrorIs(t, err, cqlerr.ErrClosed)
		})
		t.Run("JSON", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			if implCluster.SessionData(session).InternalCfg.ProtoVersion < 4 {
				t.Skip("skipping JSON support on proto < 4")
			}
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id text PRIMARY KEY,age int,state text)", table))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s JSON ?", table), codecTestMust(cql.TextValue(`{"id": "user123", "age": 42, "state": "TX"}`)))
			row := cassandraRead(t, session, fmt.Sprintf("SELECT id,age,state FROM %s WHERE id=?", table), codecTestMust(cql.TextValue("user123")))
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
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,cluster) VALUES(?,?)", table), cql.IntValue(100), cql.IntValue(200))
			row := cassandraRead(t, session, fmt.Sprintf("SELECT * FROM %s WHERE id=? AND cluster IN (?)", table), cql.IntValue(100), cql.IntValue(200))
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
			bound := cassandraUnprepared(t, "SELECT host_id FROM system.local", cql.OneColumn(cql.UUIDCodec))
			hosts := session.Metadata().Hosts()
			for _, host := range hosts {
				for i := 0; i < 5; i++ {
					require.Equal(t, host.ID, cassandraReadBound(t, session, bound, gocql.Options{HostID: cql.Option[cql.UUID]{Value: host.ID, Set: true}}))
				}
			}
			_, err := cql.ParseUUID("[invalid]")
			require.Error(t, err, "invalid host IDs are rejected while constructing typed UUIDs")
			_, err = gocql.BindOptions(session, gocql.Options{HostID: cql.Option[cql.UUID]{Value: cql.UUID{}, Set: true}})
			var invalidHost *cqlerr.ValidationError
			require.ErrorAs(t, err, &invalidHost, "an empty host ID is rejected when options are built")
			_, err = Run(context.Background(), session, bound, bindOptions(session, gocql.Options{HostID: cql.Option[cql.UUID]{Value: codecTestMust(cql.RandomUUID()), Set: true}}))
			require.ErrorIs(t, err, cqlerr.ErrNoConnections)
			host := hosts[0]
			pool, ok := implCluster.SessionData(session).InternalPool.InternalGetPoolByHostID(host.ID.String())
			require.True(t, ok)
			implEndpoint.InternalHostInfoSetState(pool.InternalHost, NodeDown)
			defer implEndpoint.InternalHostInfoSetState(pool.InternalHost, NodeUp)
			_, err = Run(context.Background(), session, bound, bindOptions(session, gocql.Options{HostID: cql.Option[cql.UUID]{Value: host.ID, Set: true}}))
			require.ErrorIs(t, err, cqlerr.ErrNoConnections)
		})
		t.Run("Keyspace", func(t *testing.T) {
			session := createV2Session(t)
			defer session.Close()
			if implCluster.SessionData(session).InternalCfg.ProtoVersion < 5 {
				t.Skip("keyspace for QUERY message is not supported in protocol < 5")
			}
			ks := testKeyspaceName(t)
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE KEYSPACE %s WITH replication={'class':'SimpleStrategy','replication_factor':'1'}", ks))
			defer cassandraDDL(t, session, "DROP KEYSPACE "+ks)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s.%s(id int PRIMARY KEY,value text)", ks, table))
			write := cassandraWriteBound(t, session, fmt.Sprintf("INSERT INTO %s(id,value) VALUES(?,?)", table), []cql.Value{cql.IntValue(1), codecTestMust(cql.TextValue("text"))}, gocql.PrepareOptions{Keyspace: ks})
			_, err := Exec(context.Background(), session, write, bindOptions(session, gocql.Options{Keyspace: ks}))
			require.NoError(t, err)
			prepared := cassandraPrepared(t, session, cql.Statement[cql.EmptyParams, cql.DynamicRow]{CQL: "SELECT * FROM " + table, Params: cql.EmptyParamsEncoder, Rows: cql.DynamicRows}, gocql.PrepareOptions{Keyspace: ks})
			for _, bound := range []*Bound[cql.DynamicRow]{cassandraBound(t, prepared, cql.EmptyParams{}), cassandraUnprepared(t, "SELECT * FROM "+table, cql.DynamicRows)} {
				row := cassandraReadBound(t, session, bound, gocql.Options{Keyspace: ks})
				require.Equal(t, int64(1), cassandraInt(t, row, "id"))
				require.Equal(t, "text", cassandraText(t, row, "value"))
			}
		})
		t.Run("NowInSeconds", func(t *testing.T) {
			session := createV2Session(t)
			defer session.Close()
			if implCluster.SessionData(session).InternalCfg.ProtoVersion < 5 {
				t.Skip("Query now in seconds are only available on protocol >= 5")
			}
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,val text)", table))
			write := cassandraWriteBound(t, session, fmt.Sprintf("INSERT INTO %s(id,val) VALUES(?,?) USING TTL 20", table), []cql.Value{cql.IntValue(1), codecTestMust(cql.TextValue("val"))}, gocql.PrepareOptions{})
			_, err := Exec(context.Background(), session, write, bindOptions(session, gocql.Options{NowInSeconds: cql.Option[int32]{Value: 0, Set: true}}))
			require.NoError(t, err)
			read := cassandraPrepared(t, session, cql.Statement[int32, int32]{CQL: fmt.Sprintf("SELECT TTL(val) FROM %s WHERE id=?", table), Params: cql.OneParam(cql.Int32), Rows: cql.OneColumn(cql.Int32)}, gocql.PrepareOptions{})
			require.Equal(t, int32(10), cassandraReadBound(t, session, cassandraBound(t, read, int32(1)), gocql.Options{NowInSeconds: cql.Option[int32]{Value: 10, Set: true}}))
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
			var row cql.DynamicRow
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
					bound := cassandraUnprepared(t, ("INSERT INTO " + fixtureKeyspace("") + ".not_existing_table(event_id,time,args) VALUES(4,now(),'test')"), cql.EmptyRowDecoder)
					_, err := Exec(context.Background(), session, bound, bindOptions(session, gocql.Options{Idempotent: tc.idempotent, Retry: cassandraRetry{}, Consistency: cql.Option[cql.Consistency]{Value: cql.All, Set: true}, Observer: observer}))
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
			bound, err := BindUnpreparedOwned(cql.Command(("INSERT INTO " + fixtureKeyspace("") + ".invalid_table(value) VALUES(1)")), cql.NewColumns(), cql.EmptyParams{})
			require.NoError(t, err)
			defer bound.Close()
			for _, mode := range []string{"Ignore", "Rethrow"} {
				for _, retries := range []int{0, 1} {
					t.Run(fmt.Sprintf("%s/%d", mode, retries), func(t *testing.T) {
						observer := new(sessionTestObserver)
						retryPolicy := sessionTestRetry{action: retry.Stop, maxAttempts: retries + 1}
						_, err := Exec(ctx, session, bound, bindOptions(session, gocql.Options{Idempotent: true, Retry: retryPolicy, Observer: observer}))
						require.Error(t, err)
						var server cqlerr.RequestError
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
		session, err := gocql.Connect(ctx, mustBuild(t, config))
		if err != nil {
			t.Fatal(err)
		}
		defer session.Close()
		namespace := fmt.Sprintf("gocql_query_%x", time.Now().UnixNano())
		exec := func(ctx context.Context, stmt string) error {
			q, err := gocql.NewUnpreparedQuery(session, cql.Command(stmt), gocql.Options{Timeout: cql.Option[time.Duration]{Set: true, Value: 30 * time.Second}})
			if err != nil {
				return err
			}
			defer q.Close()
			err = q.Exec(ctx, cql.EmptyParams{})
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
			query, err := gocql.NewUnpreparedQuery(session, cql.Statement[string, cql.Text]{Kind: cql.OrdinaryStatement, CQL: "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name=?", Params: cql.OneParam(cql.StringEncoder), Rows: cql.OneColumn(cql.TextCodec), BindMarkers: cql.NewColumns(cql.ColumnSpec{Name: "keyspace_name", Type: typ})}, gocql.Options{Timeout: cql.Option[time.Duration]{Set: true, Value: 10 * time.Second}})
			if err != nil {
				t.Error(err)
				return
			}
			defer query.Close()
			rows, err := query.Run(verify, namespace)
			if err != nil {
				t.Error(err)
				return
			}
			defer rows.Close()
			var keyspaceName cql.Text
			if rows.NextInto(&keyspaceName) {
				t.Error("owned namespace survived cleanup", namespace, dropErr)
			}
			if err := rows.Err(); err != nil {
				t.Error(err)
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
			if partial, ok := errors.AsType[*gocql.PartialRefreshError](err); attempt >= 2 || !ok || len(partial.Keyspaces) != 1 || partial.Keyspaces["<publication>"] == nil {
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
		write := codecTestMust(gocql.Prepare(ctx, session, cql.Statement[record, cql.EmptyRow]{Kind: cql.OrdinaryStatement, CQL: "INSERT INTO " + table + "(k,c,p)VALUES(?,?,?)", Params: cql.Params(cql.Param("k", cql.Int32, func(r record) int32 { return r.K }), cql.Param("c", cql.Int32, func(r record) int32 { return r.C }), cql.Param("p", cql.BytesEncoder, func(r record) []byte { return r.P })), Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{}))
		w := codecTestMust(write.Query(gocql.Options{}))
		defer w.Close()
		payload := bytes.Repeat([]byte{42}, 4096)
		for i := int32(0); i < 6; i++ {
			if err := w.Exec(ctx, record{K: 1, C: i, P: payload}); err != nil {
				t.Fatal(err)
			}
			if err := w.Wait(ctx); err != nil {
				t.Fatal(err)
			}
		}
		decoder := cql.RowInto(func(*record) {}, cql.Column("c", cql.Int32, func(r *record) *int32 { return &r.C }), cql.Column("p", cql.OwnedBlobCodec, func(r *record) *[]byte { return &r.P }))
		read := codecTestMust(gocql.Prepare(ctx, session, cql.Statement[int32, record]{Kind: cql.OrdinaryStatement, CQL: "SELECT c,p FROM " + table + " WHERE k=?", Params: cql.OneParam(cql.Int32), Rows: decoder}, gocql.PrepareOptions{}))
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
			if err := q.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			previous = rows
		}
		state := gocql.StartPage
		pageRows := 0
		var stalePage *gocql.Page[record]
		for {
			page := codecTestMust(q.RunPage(ctx, int32(1), state))
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
			if err := q.Wait(ctx); err != nil {
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
		unpaged := codecTestMust(read.Query(gocql.Options{Prefetch: cql.Option[int]{Set: true, Value: 0}}))
		defer unpaged.Close()
		page := codecTestMust(unpaged.RunUnpaged(ctx, int32(1)))
		n := 0
		var unpagedRow record
		for page.NextInto(&unpagedRow) {
			n++
		}
		if err := page.Close(); err != nil || n != 6 {
			t.Fatal("unpaged", err, n)
		}
		for _, incremental := range []bool{false, true} {
			streamDecoder := cql.StreamRow(func(*record) {}, cql.StreamColumn("c", cql.StreamInt32, func(r *record) *int32 { return &r.C }), cql.StreamColumn("p", cql.StreamBlob, func(r *record) *[]byte { return &r.P }))
			streamQuery := codecTestMust(gocql.NewStreamQuery(read, streamDecoder, gocql.StreamOptions{Incremental: incremental, Options: gocql.Options{PageSize: cql.Option[int32]{Set: true, Value: 2}}}))
			cursor, err := streamQuery.Run(ctx, int32(1))
			if incremental && *flagProto < 5 && fixtureCompressed() {
				if _, ok := errors.AsType[*cqlerr.StreamCapabilityError](err); !ok {
					t.Fatal(err)
				}
				streamQuery.Close()
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
			if err := streamQuery.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			streamQuery.Close()
			if count != 6 {
				t.Fatal("stream count", count)
			}
			t.Logf("stream receive incremental=%t passed", incremental)
		}
		var batch gocql.Batch
		for i := int32(0); i < 3; i++ {
			if err := w.AddToBatch(&batch, record{K: 2, C: i, P: payload}); err != nil {
				t.Fatal(err)
			}
		}
		payload[0] = 99
		if _, err := w.ExecBatch(ctx, cql.UnloggedBatch, &batch, gocql.SessionOptions{}); err != nil {
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
			// non-USE statements bind without keyword guessing, and explicit Prepare
			// reaches connection selection even for formerly unprepared DDL.
			session := func() *gocql.Session {
				opaqueStorageValuex := new(gocql.Session)
				implCluster.SessionData(opaqueStorageValuex).InternalLogger = nopLogger{}
				return opaqueStorageValuex
			}()
			defer session.Close()
			for _, stmt := range append(toPrepare, cantPrepare...) {
				t.Run(stmt, func(t *testing.T) {
					statement := cql.Command(stmt)
					bound, err := BindUnpreparedOwned(statement, cql.NewColumns(), cql.EmptyParams{})
					if firstCQLWord(stmt) == "USE" {
						require.ErrorIs(t, err, cqlerr.ErrUseStmt)
						_, err = gocql.Prepare(context.Background(), session, statement, gocql.PrepareOptions{})
						require.ErrorIs(t, err, cqlerr.ErrUseStmt)
						return
					}
					require.NoError(t, err)
					bound.Close()
					_, err = gocql.Prepare(context.Background(), session, statement, gocql.PrepareOptions{})
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
			session := func() *gocql.Session {
				opaqueStorageValue := new(gocql.Session)
				implCluster.SessionData(opaqueStorageValue).InternalLogger = nopLogger{}
				return opaqueStorageValue
			}()
			defer session.Close()
			for _, test := range shouldPrepareTests {
				t.Run(fmt.Sprintf("formerly_prepared_%t_%s", test.Result, firstCQLWord(test.Stmt)), func(t *testing.T) {
					statement := cql.Command(test.Stmt)
					bound, err := BindUnpreparedOwned(statement, cql.NewColumns(), cql.EmptyParams{})
					if err != nil {
						t.Fatal(err)
					}
					bound.Close()
					if _, err := gocql.Prepare(context.Background(), session, statement, gocql.PrepareOptions{}); !errors.Is(err, cqlerr.ErrNoConnections) {
						t.Fatalf("explicit Prepare must attempt a connection for %q: %v", test.Stmt, err)
					}
				})
			}
		})
	})
}
