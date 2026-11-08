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
	"time"

	"github.com/stretchr/testify/require"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
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
		var items []batchItem
		var cqls []string
		for i := int32(0); i < 100; i++ {
			stmt := fmt.Sprintf("INSERT INTO %s(id,other) VALUES(?,%d)", table, i)
			cqls = append(cqls, stmt)
			items = append(items, cassandraItem(t, cassandraWriteBound(t, session, stmt, []cql.Value{cql.IntValue(i)}, gocql.PrepareOptions{})))
		}
		cassandraBatch(t, session, cql.LoggedBatch, items, gocql.Options{Observer: observer})
		events, _ := observer.snapshot()
		require.Len(t, events, 1)
		require.NoError(t, events[0].Err)
		require.Equal(t, fixtureKeyspace(""), events[0].Routing.Keyspace)
		require.Equal(t, cqls, events[0].BatchStatements)
		// Observations expose typed parameter descriptors, never retained input
		// payloads. Verify each bound value by reading the resulting rows.
		rows := cassandraRows(t, session, cassandraUnprepared(t, "SELECT id,other FROM "+table, cql.DynamicRows), gocql.Options{})
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
		bound := cassandraUnprepared(t, fmt.Sprintf("SELECT id FROM %s LIMIT 100", table), cql.DynamicRows)
		// The typed observer reports each transport attempt and one completed logical
		// request; row counts belong to Result instead of mutable per-page callbacks.
		rows, err := Run(context.Background(), session, bound, bindOptions(session, gocql.Options{PageSize: cql.Option[int32]{Value: 10, Set: true}, Prefetch: cql.Option[int]{Value: 0, Set: true}, Observer: observer}))
		require.NoError(t, err)
		defer rows.Close()
		var row cql.DynamicRow
		for i := 0; i < 50; i++ {
			require.True(t, rows.NextInto(&row), "row %d: %v", i, rows.Err())
			_, attempts := observer.snapshot()
			require.Equal(t, i/10+1, len(attempts))
		}
		require.False(t, rows.NextInto(&row))
		require.NoError(t, rows.Close())
		events, attempts := observer.snapshot()
		require.Len(t, events, 1)
		require.GreaterOrEqual(t, len(attempts), 5)
		require.Equal(t, int64(50), events[0].Result.Rows)
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
			require.Len(t, events, 1)
			require.NotEmpty(t, attempts)
			require.Equal(t, stmt, events[0].Statement)
			require.Equal(t, fixtureKeyspace(""), events[0].Routing.Keyspace)
			require.Equal(t, wantErr, events[0].Err != nil)
		}
		selectCQL := fmt.Sprintf("SELECT id FROM %s WHERE id = ?", table)
		read := cassandraDynamicBound(t, session, selectCQL, []cql.Value{cql.IntValue(43)}, gocql.PrepareOptions{})
		rows, err := Run(context.Background(), session, read, bindOptions(session, gocql.Options{Observer: observer}))
		require.NoError(t, err)
		defer rows.Close()
		var row cql.DynamicRow
		require.False(t, rows.NextInto(&row))
		require.NoError(t, rows.Close())
		check(selectCQL, false)
		observer.reset()
		insertCQL := fmt.Sprintf("INSERT INTO %s (id) VALUES (?)", table)
		write := cassandraWriteBound(t, session, insertCQL, []cql.Value{cql.IntValue(42)}, gocql.PrepareOptions{})
		_, err = Exec(context.Background(), session, write, bindOptions(session, gocql.Options{Observer: observer}))
		require.NoError(t, err)
		check(insertCQL, false)
		observer.reset()
		read = cassandraDynamicBound(t, session, selectCQL, []cql.Value{cql.IntValue(42)}, gocql.PrepareOptions{})
		require.Equal(t, int64(42), cassandraInt(t, cassandraReadBound(t, session, read, gocql.Options{Observer: observer}), "id"))
		check(selectCQL, false)
		observer.reset()
		other := createV2Session(t, func(c *gocql.ConfigBuilder) { c.Observer(observer) })
		defer other.Close()
		observer.reset()
		read = cassandraDynamicBound(t, other, selectCQL, []cql.Value{cql.IntValue(42)}, gocql.PrepareOptions{})
		require.Equal(t, int64(42), cassandraInt(t, cassandraReadBound(t, other, read, gocql.Options{}), "id"))
		check(selectCQL, false)
		observer.reset()
		bad := cassandraUnprepared(t, "SELECT id FROM unknown_table WHERE id = 42", cql.DynamicRows)
		_, err = Run(context.Background(), session, bad, bindOptions(session, gocql.Options{Observer: observer}))
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
			write := cassandraWriteBound(t, session, fmt.Sprintf("INSERT INTO %s (id) VALUES (?)", table), []cql.Value{cql.IntValue(42)}, gocql.PrepareOptions{})
			result, err := Exec(context.Background(), session, write, bindOptions(session, gocql.Options{Trace: true}))
			require.NoError(t, err)
			require.NotEmpty(t, result.TraceIDs)
			checkTrace := func(ids []cql.UUID) {
				t.Helper()
				require.NotEmpty(t, ids)
				for _, id := range ids {
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					trace, err := session.Trace(ctx, id)
					cancel()
					require.NoError(t, err)
					require.Equal(t, id, trace.ID)
					require.NotEmpty(t, trace.Events)
				}
			}
			checkTrace(result.TraceIDs())
			read := cassandraDynamicBound(t, session, fmt.Sprintf("SELECT id FROM %s WHERE id = ?", table), []cql.Value{cql.IntValue(42)}, gocql.PrepareOptions{})
			// Options are explicit per execution; the same binding can be traced repeatedly.
			for i := 0; i < 2; i++ {
				rows, err := Run(context.Background(), session, read, bindOptions(session, gocql.Options{Trace: true}))
				require.NoError(t, err)
				defer rows.Close()
				var row cql.DynamicRow
				require.True(t, rows.NextInto(&row))
				require.Equal(t, int64(42), cassandraInt(t, row, "id"))
				require.False(t, rows.NextInto(&row))
				require.NoError(t, rows.Close())
				checkTrace(rows.Info().TraceIDs())
			}
		})
		t.Run("Tracer", func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			require.NoError(t, v2CreateTable(ctx, session, fmt.Sprintf((`CREATE TABLE `+fixtureKeyspace("")+`.%s (id int primary key)`), table)))
			write := sessionTestBind(t, session, fmt.Sprintf(`INSERT INTO %s (id) VALUES (?)`, table), cql.IntValue(42))
			defer write.Close()
			inserted, err := Exec(ctx, session, write, bindOptions(session, gocql.Options{Trace: true}))
			require.NoError(t, err)
			require.NotEmpty(t, inserted.TraceIDs)
			prepared, err := gocql.Prepare(ctx, session, cql.Statement[int32, int32]{CQL: fmt.Sprintf(`SELECT id FROM %s WHERE id = ?`, table), Params: cql.OneParam(cql.Int32), Rows: cql.OneColumn(cql.Int32)}, gocql.PrepareOptions{})
			require.NoError(t, err)
			read, err := BindOwned(prepared, int32(42))
			require.NoError(t, err)
			defer read.Close()
			rows, err := Run(ctx, session, read, bindOptions(session, gocql.Options{Trace: true}))
			require.NoError(t, err)
			defer rows.Close()
			var value int32
			require.True(t, rows.NextInto(&value))
			require.Equal(t, int32(42), value)
			require.False(t, rows.NextInto(&value))
			require.NoError(t, rows.Err())
			selected := rows.Info()
			require.NoError(t, rows.Close())
			require.NotEmpty(t, selected.TraceIDs)
			// Trace polls readiness under the caller context and returns owned typed
			// events; the legacy Tracer's internal table queries are no longer used.
			for _, id := range append(inserted.TraceIDs(), selected.TraceIDs()...) {
				info, err := session.Trace(ctx, id)
				require.NoError(t, err)
				require.Equal(t, id, info.ID)
				require.True(t, info.Coordinator.IsValid(), "trace coordinator missing")
				require.False(t, info.Started.IsZero(), "trace start missing")
				require.NotEmpty(t, info.Events, "trace activities missing")
				for _, event := range info.Events {
					require.NotEmpty(t, event.Activity)
					require.True(t, event.Source.IsValid())
				}
			}
		})
	})
}
