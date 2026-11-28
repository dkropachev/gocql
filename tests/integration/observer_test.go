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

	"github.com/stretchr/testify/require"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqltrace"
	"github.com/scylladb/gocql/v2/execopt"
)

func TestObserver(t *testing.T) {
	t.Parallel()

	t.Run("Batch", func(t *testing.T) {
		t.Parallel()
		session := createV2Session(t)
		defer session.Close()
		table := testTableName(t)
		cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,other int)", table))
		observer := &cassandraObserver{}
		batch := &gocql.Batch{Kind: cql.LoggedBatch}
		var cqls []string
		for i := int32(0); i < 100; i++ {
			stmt := fmt.Sprintf("INSERT INTO %s(id,other) VALUES(?,%d)", table, i)
			cqls = append(cqls, stmt)
			cassandraAdd(t, batch, cassandraWritePrepared(t, session, stmt, execopt.PrepareOptions{}), []cql.Value{cql.IntValue(i)})
		}
		cassandraBatch(t, session, batch, execopt.Options{Observer: hooksOf(observer)})
		events, _ := observer.snapshot()
		require.Len(t, events, 1)
		require.NoError(t, events[0].Err)
		require.Equal(t, 1, events[0].Pages)
		starts, values := observer.started()
		require.Len(t, starts, 1)
		require.Equal(t, fixtureKeyspace(""), starts[0].Routing.Keyspace)
		var statements []string
		for _, statement := range starts[0].BatchStatements {
			statements = append(statements, statement.Statement)
		}
		require.Equal(t, cqls, statements)
		// The Request hook read each statement's bound value while it was
		// valid.
		require.True(t, strings.HasPrefix(values[0], "id=0; id=1; id=2; "), values[0])
		// Verify each bound value by reading the resulting rows.
		rows := cassandraUnpreparedAll(t, session, "SELECT id,other FROM "+table, cql.DynamicRows, execopt.Options{})
		require.Len(t, rows, 100)
		for _, row := range rows {
			require.Equal(t, cassandraInt(t, row, "id"), cassandraInt(t, row, "other"))
		}
	})

	t.Run("Paging", func(t *testing.T) {
		t.Parallel()
		session := createV2Session(t)
		defer session.Close()
		table := testTableName(t)
		cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", table))
		for i := int32(0); i < 50; i++ {
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s (id) VALUES (?)", table), cql.IntValue(i))
		}
		observer := &cassandraObserver{}
		statement := cql.Statement[cql.EmptyParams, cql.DynamicRow]{CQL: fmt.Sprintf("SELECT id FROM %s LIMIT 100", table), Rows: cql.DynamicRows}
		// The typed observer reports each transport attempt and one completed logical
		// request; row counts belong to Result instead of mutable per-page callbacks.
		var row cql.DynamicRow
		i := 0
		_, err := session.UnpreparedEach(context.Background(), statement, cql.EmptyParams{}, &row, func(*cql.DynamicRow) error {
			_, attempts := observer.snapshot()
			require.Equal(t, i/10+1, len(attempts), "row %d", i)
			i++
			return nil
		}, bindOptions(session, execopt.Options{PageSize: 10, Prefetch: cql.Some(false), Observer: hooksOf(observer)}))
		require.NoError(t, err)
		require.Equal(t, 50, i)
		events, attempts := observer.snapshot()
		require.Len(t, events, 1)
		require.GreaterOrEqual(t, len(attempts), 5)
		require.Equal(t, int64(50), events[0].Result.FetchedRows)
		// Each page's attempts are numbered from 1 within the page, and the
		// pages from 1 within the request; the request reports how many
		// pages it fetched (a sixth, empty one when the fifth still had a
		// paging state).
		require.GreaterOrEqual(t, events[0].Pages, 5)
		require.Len(t, attempts, events[0].Pages)
		for i, attempt := range attempts {
			require.Equal(t, i+1, attempt.Page, "attempt %d", i)
			require.Equal(t, 1, attempt.Attempt, "attempt %d", i)
			require.NoError(t, attempt.Err)
		}
	})

	t.Run("Query", func(t *testing.T) {
		t.Parallel()
		session := createV2Session(t)
		defer session.Close()
		table := testTableName(t)
		cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", table))
		observer := &cassandraObserver{}
		check := func(stmt string, wantErr bool) {
			t.Helper()
			events, attempts := observer.snapshot()
			starts, _ := observer.started()
			require.Len(t, events, 1)
			require.Len(t, starts, 1)
			require.NotEmpty(t, attempts)
			require.Equal(t, stmt, starts[0].Statement)
			require.Equal(t, fixtureKeyspace(""), starts[0].Routing.Keyspace)
			require.Equal(t, wantErr, events[0].Err != nil)
		}
		selectCQL := fmt.Sprintf("SELECT id FROM %s WHERE id = ?", table)
		read := cassandraDynamicPrepared(t, session, selectCQL, execopt.PrepareOptions{})
		rows, err := read.Run(context.Background(), []cql.Value{cql.IntValue(43)}, bindOptions(session, execopt.Options{Observer: hooksOf(observer)}))
		require.NoError(t, err)
		defer rows.Close()
		var row cql.DynamicRow
		require.False(t, rows.NextInto(&row))
		require.NoError(t, rows.Close())
		check(selectCQL, false)
		observer.reset()
		insertCQL := fmt.Sprintf("INSERT INTO %s (id) VALUES (?)", table)
		write := cassandraWritePrepared(t, session, insertCQL, execopt.PrepareOptions{})
		_, err = write.Exec(context.Background(), []cql.Value{cql.IntValue(42)}, bindOptions(session, execopt.Options{Observer: hooksOf(observer)}))
		require.NoError(t, err)
		check(insertCQL, false)
		observer.reset()
		require.Equal(t, int64(42), cassandraInt(t, cassandraReadOne(t, session, read, []cql.Value{cql.IntValue(42)}, execopt.Options{Observer: hooksOf(observer)}), "id"))
		check(selectCQL, false)
		observer.reset()
		other := createV2Session(t, func(c *config.ConfigBuilder) { c.WithObserver(hooksOf(observer)) })
		defer other.Close()
		observer.reset()
		read = cassandraDynamicPrepared(t, other, selectCQL, execopt.PrepareOptions{})
		require.Equal(t, int64(42), cassandraInt(t, cassandraReadOne(t, other, read, []cql.Value{cql.IntValue(42)}, execopt.Options{}), "id"))
		check(selectCQL, false)
		observer.reset()
		bad := cql.Statement[cql.EmptyParams, cql.DynamicRow]{CQL: "SELECT id FROM unknown_table WHERE id = 42", Rows: cql.DynamicRows}
		var badRow cql.DynamicRow
		_, err = session.UnpreparedEach(context.Background(), bad, cql.EmptyParams{}, &badRow, func(*cql.DynamicRow) error { return nil }, bindOptions(session, execopt.Options{Observer: hooksOf(observer)}))
		require.Error(t, err)
		check("SELECT id FROM unknown_table WHERE id = 42", true)
	})

	t.Run("Trace", func(t *testing.T) {
		t.Parallel()

		t.Run("IDs", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", table))
			write := cassandraWritePrepared(t, session, fmt.Sprintf("INSERT INTO %s (id) VALUES (?)", table), execopt.PrepareOptions{})
			result, err := write.Exec(context.Background(), []cql.Value{cql.IntValue(42)}, bindOptions(session, execopt.Options{CQLTrace: true}))
			require.NoError(t, err)
			require.NotEmpty(t, result.CQLTraces())
			checkTrace := func(refs []cql.CQLTraceRef) {
				t.Helper()
				require.NotEmpty(t, refs)
				for _, ref := range refs {
					require.False(t, ref.Failed)
					require.Equal(t, 1, ref.Attempt)
					require.NotZero(t, ref.HostID)
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					trace, err := cqltrace.Fetch(ctx, session, ref)
					cancel()
					require.NoError(t, err)
					require.Equal(t, ref.ID, trace.ID)
					require.NotEmpty(t, trace.Events)
					require.True(t, trace.Settled)
				}
			}
			checkTrace(result.CQLTraces())
			read := cassandraDynamicPrepared(t, session, fmt.Sprintf("SELECT id FROM %s WHERE id = ?", table), execopt.PrepareOptions{})
			// Options are explicit per execution; the same statement can be traced repeatedly.
			for i := 0; i < 2; i++ {
				rows, err := read.Run(context.Background(), []cql.Value{cql.IntValue(42)}, bindOptions(session, execopt.Options{CQLTrace: true}))
				require.NoError(t, err)
				defer rows.Close()
				var row cql.DynamicRow
				require.True(t, rows.NextInto(&row))
				require.Equal(t, int64(42), cassandraInt(t, row, "id"))
				require.False(t, rows.NextInto(&row))
				require.NoError(t, rows.Close())
				checkTrace(rows.Result().CQLTraces())
			}
		})
		t.Run("Uncounted", func(t *testing.T) {
			t.Parallel()
			// Reading trace sessions is neither observed nor counted in the
			// session's metrics.
			observer := &cassandraObserver{}
			session := createV2Session(t, func(c *config.ConfigBuilder) { c.WithObserver(hooksOf(observer)) })
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", table))
			write := cassandraWritePrepared(t, session, fmt.Sprintf("INSERT INTO %s (id) VALUES (?)", table), execopt.PrepareOptions{})
			result, err := write.Exec(context.Background(), []cql.Value{cql.IntValue(42)}, bindOptions(session, execopt.Options{CQLTrace: true}))
			require.NoError(t, err)
			require.NotEmpty(t, result.CQLTraces())
			observer.reset()
			before := session.Metrics()
			hostAttempts := func() uint64 {
				var n uint64
				for _, h := range session.HostMetrics(nil) {
					n += h.Attempts
				}
				return n
			}
			hostsBefore := hostAttempts()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			infos, err := cqltrace.FetchAll(ctx, session, result.CQLTraces())
			require.NoError(t, err)
			require.NotEmpty(t, infos[0].Events)
			after := session.Metrics()
			require.Equal(t, before.Requests, after.Requests)
			require.Equal(t, before.Pages, after.Pages)
			require.Equal(t, before.Attempts, after.Attempts)
			require.Equal(t, before.Errors, after.Errors)
			require.Equal(t, hostsBefore, hostAttempts())
			events, attempts := observer.snapshot()
			require.Empty(t, events)
			require.Empty(t, attempts)
		})
		t.Run("Tracer", func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			require.NoError(t, v2CreateTable(ctx, session, fmt.Sprintf((`CREATE TABLE `+fixtureKeyspace("")+`.%s (id int primary key)`), table)))
			write := cassandraWritePrepared(t, session, fmt.Sprintf(`INSERT INTO %s (id) VALUES (?)`, table), execopt.PrepareOptions{})
			inserted, err := write.Exec(ctx, []cql.Value{cql.IntValue(42)}, bindOptions(session, execopt.Options{CQLTrace: true}))
			require.NoError(t, err)
			require.NotEmpty(t, inserted.CQLTraces())
			prepared, err := session.Prepare(ctx, cql.Statement[int32, int32]{CQL: fmt.Sprintf(`SELECT id FROM %s WHERE id = ?`, table), Params: cql.OneParam(cql.Int32Codec), Rows: cql.OneColumn(cql.Int32Codec)}, execopt.PrepareOptions{})
			require.NoError(t, err)
			rows, err := prepared.Run(ctx, int32(42), bindOptions(session, execopt.Options{CQLTrace: true}))
			require.NoError(t, err)
			defer rows.Close()
			var value int32
			require.True(t, rows.NextInto(&value))
			require.Equal(t, int32(42), value)
			require.False(t, rows.NextInto(&value))
			require.NoError(t, rows.Err())
			selected := rows.Result()
			require.NoError(t, rows.Close())
			require.NotEmpty(t, selected.CQLTraces())
			// Trace polls readiness under the caller context and returns owned typed
			// events; the legacy Tracer's internal table queries are no longer used.
			for _, ref := range append(inserted.CQLTraces(), selected.CQLTraces()...) {
				info, err := cqltrace.Fetch(ctx, session, ref)
				require.NoError(t, err)
				require.Equal(t, ref.ID, info.ID)
				require.True(t, info.Coordinator.IsValid(), "trace coordinator missing")
				require.False(t, info.Started.IsZero(), "trace start missing")
				require.NotEmpty(t, info.Request, "trace request missing")
				require.NotEmpty(t, info.Parameters, "trace parameters missing")
				require.Contains(t, info.Nodes(), info.Coordinator)
				require.NotEmpty(t, info.Events, "trace activities missing")
				for _, event := range info.Events {
					require.NotEmpty(t, event.Activity)
					require.True(t, event.Source.IsValid())
				}
			}
		})
	})
}
