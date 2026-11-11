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
	implSession "github.com/scylladb/gocql/v2/internal/session"
)

func TestConditional(t *testing.T) {
	t.Parallel()

	t.Run("Exec", func(t *testing.T) {
		t.Parallel()
		session := createV2SessionFromConfig(createV2Cluster(), t, true)
		defer session.Close()
		table := testTableName(t)
		cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(title varchar,revid timeuuid,last_modified timestamp,PRIMARY KEY(title,revid))", table))
		title := "baz"
		rev := cql.NewTimeUUID()
		modified := cql.Timestamp(time.Now().UnixMilli())
		revValue := cassandraValue(cql.TimeUUIDValue(cql.TimeUUID(rev)))
		insertCQL := fmt.Sprintf("INSERT INTO %s(title,revid,last_modified) VALUES (?,?,?) IF NOT EXISTS", table)
		values := []cql.Value{codecTestMust(cql.TextValue(title)), revValue, cql.TimestampValue(modified)}
		insert := cassandraCASBound(t, session, insertCQL, values, cql.DynamicRows)
		first := cassandraReadBound(t, session, insert, gocql.Options{})
		require.True(t, first.Applied)
		second := cassandraReadBound(t, session, insert, gocql.Options{})
		require.False(t, second.Applied)
		require.True(t, second.Previous.Set)
		require.Equal(t, title, cassandraText(t, second.Previous.Value, "title"))
		gotRev, err := cassandraField(t, second.Previous.Value, "revid").UUID()
		require.NoError(t, err)
		require.Equal(t, cql.UUID(rev), gotRev)
		deleteCQL := fmt.Sprintf("DELETE FROM %s WHERE title=? AND revid=? IF last_modified=?", table)
		failedDelete := cassandraCASBound(t, session, deleteCQL, []cql.Value{codecTestMust(cql.TextValue(title)), revValue, cql.TimestampValue(modified + 10000)}, cql.DynamicRows)
		deleted := cassandraReadBound(t, session, failedDelete, gocql.Options{})
		require.False(t, deleted.Applied)
		require.Equal(t, int64(modified), cassandraInt(t, deleted.Previous.Value, "last_modified"))
		// Missing or incompatible previous-value decoders fail explicitly.
		invalid, err := session.Prepare(context.Background(), cql.Statement[[]cql.Value, cql.CASResult[cql.EmptyRow]]{CQL: deleteCQL, Params: cql.DynamicParams, Rows: cql.CASRows(cql.EmptyRowDecoder)}, gocql.PrepareOptions{})
		if err == nil {
			bound := cassandraBound(t, invalid, []cql.Value{codecTestMust(cql.TextValue(title)), revValue, cql.TimestampValue(modified + 10000)})
			rows, e := Run(context.Background(), session, bound, gocql.CompiledOptions{})
			defer rows.Close()
			err = e
		}
		require.Error(t, err)
		successDelete := cassandraCASBound(t, session, deleteCQL, values, cql.DynamicRows)
		require.True(t, cassandraReadBound(t, session, successDelete, gocql.Options{}).Applied)
		if err := truncateTable(t, session, table); err != nil {
			t.Fatal(err)
		}
		runCASBatch := func(items []batchItem, wantApplied bool, wantRows int) {
			t.Helper()
			page, err := runBatch(context.Background(), session, cql.LoggedBatch, items, cql.CASRows(cql.DynamicRows), gocql.CompiledOptions{})
			require.NoError(t, err)
			defer page.Close()
			count := 0
			var row cql.CASResult[cql.DynamicRow]
			for page.NextInto(&row) {
				require.Equal(t, wantApplied, row.Applied)
				if !wantApplied {
					require.True(t, row.Previous.Set)
				}
				count++
			}
			require.NoError(t, page.Close())
			require.Equal(t, wantRows, count)
		}
		runCASBatch([]batchItem{cassandraItem(t, insert)}, true, 1)
		other := cassandraCASBound(t, session, insertCQL, []cql.Value{codecTestMust(cql.TextValue(title + "_foo")), revValue, cql.TimestampValue(modified)}, cql.DynamicRows)
		runCASBatch([]batchItem{cassandraItem(t, other)}, true, 1)
		runCASBatch([]batchItem{cassandraItem(t, other)}, false, 1)
		var seed, conditional []batchItem
		for _, id := range []string{"2c3af400-73a4-11e5-9381-29463d90c3f0", "3e4ad2f1-73a4-11e5-9381-29463d90c3f0"} {
			r, e := cql.ParseUUID(id)
			require.NoError(t, e)
			rv := cassandraValue(cql.TimeUUIDValue(cql.TimeUUID(r)))
			seed = append(seed, cassandraItem(t, cassandraWriteBound(t, session, fmt.Sprintf("INSERT INTO %s(title,revid,last_modified) VALUES (?,?,?)", table), []cql.Value{codecTestMust(cql.TextValue("_foo")), rv, cql.TimestampValue(modified)}, gocql.PrepareOptions{})))
			conditional = append(conditional, cassandraItem(t, cassandraCASBound(t, session, fmt.Sprintf("UPDATE %s SET last_modified=? WHERE title=? AND revid=? IF last_modified=?", table), []cql.Value{cql.TimestampValue(modified + 10000), codecTestMust(cql.TextValue("_foo")), rv, cql.TimestampValue(modified + 20000)}, cql.DynamicRows)))
		}
		cassandraBatch(t, session, cql.LoggedBatch, seed, gocql.Options{})
		runCASBatch(conditional, false, 2)
		// CASRows rejects ordinary SELECTs and unconditional batches; the former
		// MapScanCAS silent false result is replaced by validated result metadata.
		_, err = session.Prepare(context.Background(), cql.Statement[[]cql.Value, cql.CASResult[cql.DynamicRow]]{CQL: fmt.Sprintf("SELECT revid FROM %s WHERE title=?", table), Params: cql.DynamicParams, Rows: cql.CASRows(cql.DynamicRows)}, gocql.PrepareOptions{})
		require.Error(t, err)
		for i := 0; i < 2; i++ {
			page, err := runBatch(context.Background(), session, cql.LoggedBatch, seed, cql.CASRows(cql.DynamicRows), gocql.CompiledOptions{})
			if page != nil {
				page.Close()
			}
			require.Error(t, err)
		}
		for _, batch := range []bool{false, true} {
			badDecoder := cql.CASRows(cql.OneColumn(cql.BoolCodec))
			bound := cassandraDynamicBound(t, session, fmt.Sprintf("UPDATE %s SET last_modified=? WHERE title=? AND revid=? IF last_modified=?", table), []cql.Value{cql.TimestampValue(modified + 10000), codecTestMust(cql.TextValue("_foo")), cassandraValue(cql.TimeUUIDValue(cql.TimeUUID(cassandraUUID("3e4ad2f1-73a4-11e5-9381-29463d90c3f0")))), cql.TimestampValue(modified + 20000)}, gocql.PrepareOptions{})
			if batch {
				page, e := runBatch(context.Background(), session, cql.LoggedBatch, []batchItem{cassandraItem(t, bound)}, badDecoder, gocql.CompiledOptions{})
				if page != nil {
					page.Close()
				}
				require.Error(t, e)
			} else {
				bad := func() *Bound[cql.CASResult[bool]] {
					opaqueStorageValue := new(Bound[cql.CASResult[bool]])
					implSession.BoundData(opaqueStorageValue).InternalRequest = implSession.BoundData(bound).InternalRequest
					implSession.BoundData(opaqueStorageValue).InternalRows = badDecoder
					return opaqueStorageValue
				}()
				rows, e := Run(context.Background(), session, bad, gocql.CompiledOptions{})
				rows.Close()
				require.Error(t, e)
			}
		}
	})

	t.Run("MapScan", func(t *testing.T) {
		t.Parallel()
		session := createV2SessionFromConfig(createV2Cluster(), t, true)
		defer session.Close()
		table := testTableName(t)
		cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(title varchar,revid timeuuid,last_modified timestamp,deleted boolean,PRIMARY KEY(title,revid))", table))
		rev := cql.NewTimeUUID()
		modified := cql.Timestamp(time.Now().UnixMilli())
		bound := cassandraCASBound(t, session, fmt.Sprintf("INSERT INTO %s(title,revid,last_modified,deleted) VALUES(?,?,?,?) IF NOT EXISTS", table), []cql.Value{codecTestMust(cql.TextValue("baz")), cassandraValue(cql.TimeUUIDValue(cql.TimeUUID(rev))), cql.TimestampValue(modified), cql.BoolValue(false)}, cql.DynamicRows)
		require.True(t, cassandraReadBound(t, session, bound, gocql.Options{}).Applied)
		result := cassandraReadBound(t, session, bound, gocql.Options{})
		require.False(t, result.Applied)
		require.True(t, result.Previous.Set)
		row := result.Previous.Value
		require.Equal(t, "baz", cassandraText(t, row, "title"))
		got, e := cassandraField(t, row, "revid").UUID()
		require.NoError(t, e)
		require.Equal(t, cql.UUID(rev), got)
		require.False(t, cassandraBool(t, row, "deleted"))
		require.Equal(t, int64(modified), cassandraInt(t, row, "last_modified"))
	})

	t.Run("ScanIgnoredColumns", func(t *testing.T) {
		t.Parallel()
		session := createV2SessionFromConfig(createV2Cluster(), t, true)
		defer session.Close()
		table := testTableName(t)
		cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(foo varchar,bar varchar,PRIMARY KEY(foo,bar))", table))
		stmt := fmt.Sprintf("INSERT INTO %s(foo,bar) VALUES(?,?) IF NOT EXISTS", table)
		values := []cql.Value{codecTestMust(cql.TextValue("baz")), codecTestMust(cql.TextValue("baz"))}
		ignored := cql.Row(cql.IgnoreColumn[cql.EmptyRow]("foo"), cql.IgnoreColumn[cql.EmptyRow]("bar"))
		require.True(t, cassandraReadBound(t, session, cassandraCASBound(t, session, stmt, values, ignored), gocql.Options{}).Applied)
		type result struct{ Text cql.Text }
		for _, selected := range []string{"foo", "bar"} {
			other := "foo"
			if selected == "foo" {
				other = "bar"
			}
			decoder := cql.Row(cql.Column(selected, cql.TextCodec, func(v *result) *cql.Text { return &v.Text }), cql.IgnoreColumn[result](other))
			got := cassandraReadBound(t, session, cassandraCASBound(t, session, stmt, values, decoder), gocql.Options{})
			require.False(t, got.Applied)
			require.True(t, got.Previous.Set)
			require.Equal(t, "baz", got.Previous.Value.Text.String())
		}
	})

	t.Run("SerialConsistency", func(t *testing.T) {
		t.Parallel()
		session := createV2Session(t)
		defer session.Close()
		table := testTableName(t)
		cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY)", table))
		for i, tc := range []struct {
			name        string
			consistency cql.Consistency
		}{{"Any", cql.Any}, {"One", cql.One}, {"Two", cql.Two}, {"Three", cql.Three}, {"Quorum", cql.Quorum}, {"LocalQuorum", cql.LocalQuorum}, {"EachQuorum", cql.EachQuorum}, {"Serial", cql.Serial}, {"LocalSerial", cql.LocalSerial}, {"LocalOne", cql.LocalOne}} {
			t.Run(tc.name, func(t *testing.T) {
				bound := cassandraWriteBound(t, session, fmt.Sprintf("INSERT INTO %s(id) VALUES (?)", table), []cql.Value{cql.IntValue(int32(i + 1))}, gocql.PrepareOptions{})
				// Serial consistency is checked when the options are built.
				options, err := session.CompileOptions(gocql.Options{SerialConsistency: cql.Some(tc.consistency)})
				if err == nil {
					_, err = Exec(context.Background(), session, bound, options)
				}
				if tc.consistency == cql.Serial || tc.consistency == cql.LocalSerial {
					require.NoError(t, err)
					require.Equal(t, int64(i+1), cassandraInt(t, cassandraRead(t, session, fmt.Sprintf("SELECT id FROM %s WHERE id=?", table), cql.IntValue(int32(i+1))), "id"))
				} else {
					require.Error(t, err)
					require.Contains(t, err.Error(), "serial consistency")
				}
			})
		}
	})
}
