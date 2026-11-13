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
	"github.com/scylladb/gocql/v2/cql/dynamic"
	"github.com/scylladb/gocql/v2/exec"
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
		revValue := cassandraValue(dynamic.TimeUUID(cql.TimeUUID(rev)))
		insertCQL := fmt.Sprintf("INSERT INTO %s(title,revid,last_modified) VALUES (?,?,?) IF NOT EXISTS", table)
		values := []dynamic.Value{codecTestMust(dynamic.Text(title)), revValue, dynamic.Timestamp(modified)}
		insert := cassandraCASPrepared(t, session, insertCQL, dynamic.Rows)
		first := cassandraReadOne(t, session, insert, values, exec.Options{})
		require.True(t, first.Applied)
		second := cassandraReadOne(t, session, insert, values, exec.Options{})
		require.False(t, second.Applied)
		require.True(t, second.Previous.Set)
		require.Equal(t, title, cassandraText(t, second.Previous.Value, "title"))
		gotRev, err := cassandraField(t, second.Previous.Value, "revid").UUID()
		require.NoError(t, err)
		require.Equal(t, cql.UUID(rev), gotRev)
		deleteCQL := fmt.Sprintf("DELETE FROM %s WHERE title=? AND revid=? IF last_modified=?", table)
		failedDelete := cassandraCASPrepared(t, session, deleteCQL, dynamic.Rows)
		deleted := cassandraReadOne(t, session, failedDelete, []dynamic.Value{codecTestMust(dynamic.Text(title)), revValue, dynamic.Timestamp(modified + 10000)}, exec.Options{})
		require.False(t, deleted.Applied)
		require.Equal(t, int64(modified), cassandraInt(t, deleted.Previous.Value, "last_modified"))
		// A previous-value decoder whose field type does not match its column
		// fails explicitly. Columns without a field are skipped, so only the
		// type mismatch can fail.
		type modifiedAsInt struct{ Modified int32 }
		incompatible := cql.Row(cql.Column("last_modified", cql.Int32Codec, func(r *modifiedAsInt) *int32 { return &r.Modified }))
		invalid, err := session.Prepare(context.Background(), cql.Statement[[]dynamic.Value, cql.CASResult[modifiedAsInt]]{CQL: deleteCQL, Params: dynamic.Params, Rows: cql.CASRows(incompatible)}, exec.PrepareOptions{})
		if err == nil {
			rows, e := invalid.Run(context.Background(), []dynamic.Value{codecTestMust(dynamic.Text(title)), revValue, dynamic.Timestamp(modified + 10000)})
			defer rows.Close()
			err = e
		}
		require.Error(t, err)
		require.True(t, cassandraReadOne(t, session, failedDelete, values, exec.Options{}).Applied)
		if err := truncateTable(t, session, table); err != nil {
			t.Fatal(err)
		}
		runCASBatch := func(batch *gocql.Batch, wantApplied bool, wantRows int) {
			t.Helper()
			rows, err := session.RunBatch(context.Background(), batch, cql.CASRows(dynamic.Rows))
			require.NoError(t, err)
			defer rows.Close()
			count := 0
			var row cql.CASResult[dynamic.Row]
			for rows.NextInto(&row) {
				require.Equal(t, wantApplied, row.Applied)
				if !wantApplied {
					require.True(t, row.Previous.Set)
				}
				count++
			}
			require.NoError(t, rows.Close())
			require.Equal(t, wantRows, count)
		}
		casBatch := func(p *gocql.Prepared[[]dynamic.Value, cql.CASResult[dynamic.Row]], value []dynamic.Value) *gocql.Batch {
			batch := &gocql.Batch{Kind: cql.LoggedBatch}
			cassandraAdd(t, batch, p, value)
			return batch
		}
		runCASBatch(casBatch(insert, values), true, 1)
		otherValues := []dynamic.Value{codecTestMust(dynamic.Text(title + "_foo")), revValue, dynamic.Timestamp(modified)}
		runCASBatch(casBatch(insert, otherValues), true, 1)
		runCASBatch(casBatch(insert, otherValues), false, 1)
		seed := &gocql.Batch{Kind: cql.LoggedBatch}
		conditional := &gocql.Batch{Kind: cql.LoggedBatch}
		seedInsert := cassandraWritePrepared(t, session, fmt.Sprintf("INSERT INTO %s(title,revid,last_modified) VALUES (?,?,?)", table), exec.PrepareOptions{})
		conditionalUpdate := cassandraCASPrepared(t, session, fmt.Sprintf("UPDATE %s SET last_modified=? WHERE title=? AND revid=? IF last_modified=?", table), dynamic.Rows)
		for _, id := range []string{"2c3af400-73a4-11e5-9381-29463d90c3f0", "3e4ad2f1-73a4-11e5-9381-29463d90c3f0"} {
			r, e := cql.ParseUUID(id)
			require.NoError(t, e)
			rv := cassandraValue(dynamic.TimeUUID(cql.TimeUUID(r)))
			cassandraAdd(t, seed, seedInsert, []dynamic.Value{codecTestMust(dynamic.Text("_foo")), rv, dynamic.Timestamp(modified)})
			cassandraAdd(t, conditional, conditionalUpdate, []dynamic.Value{dynamic.Timestamp(modified + 10000), codecTestMust(dynamic.Text("_foo")), rv, dynamic.Timestamp(modified + 20000)})
		}
		cassandraBatch(t, session, seed, exec.Options{})
		runCASBatch(conditional, false, 2)
		// CASRows rejects ordinary SELECTs and unconditional batches; the former
		// MapScanCAS silent false result is replaced by validated result metadata.
		_, err = session.Prepare(context.Background(), cql.Statement[[]dynamic.Value, cql.CASResult[dynamic.Row]]{CQL: fmt.Sprintf("SELECT revid FROM %s WHERE title=?", table), Params: dynamic.Params, Rows: cql.CASRows(dynamic.Rows)}, exec.PrepareOptions{})
		require.Error(t, err)
		for i := 0; i < 2; i++ {
			rows, err := session.RunBatch(context.Background(), seed, cql.CASRows(dynamic.Rows))
			rows.Close()
			require.Error(t, err)
		}
		updateCQL := fmt.Sprintf("UPDATE %s SET last_modified=? WHERE title=? AND revid=? IF last_modified=?", table)
		updateValues := []dynamic.Value{dynamic.Timestamp(modified + 10000), codecTestMust(dynamic.Text("_foo")), cassandraValue(dynamic.TimeUUID(cql.TimeUUID(cassandraUUID("3e4ad2f1-73a4-11e5-9381-29463d90c3f0")))), dynamic.Timestamp(modified + 20000)}
		badDecoder := cql.CASRows(cql.OneColumn(cql.BoolCodec))
		for _, batch := range []bool{false, true} {
			if batch {
				b := &gocql.Batch{Kind: cql.LoggedBatch}
				cassandraAdd(t, b, cassandraDynamicPrepared(t, session, updateCQL, exec.PrepareOptions{}), updateValues)
				rows, e := session.RunBatch(context.Background(), b, badDecoder)
				rows.Close()
				require.Error(t, e)
			} else {
				// CAS result columns are known only at execution, so preparing with
				// a mismatched previous-value decoder succeeds and Run fails.
				bad, err := session.Prepare(context.Background(), cql.Statement[[]dynamic.Value, cql.CASResult[bool]]{CQL: updateCQL, Params: dynamic.Params, Rows: badDecoder}, exec.PrepareOptions{})
				require.NoError(t, err)
				rows, e := bad.Run(context.Background(), updateValues)
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
		insert := cassandraCASPrepared(t, session, fmt.Sprintf("INSERT INTO %s(title,revid,last_modified,deleted) VALUES(?,?,?,?) IF NOT EXISTS", table), dynamic.Rows)
		values := []dynamic.Value{codecTestMust(dynamic.Text("baz")), cassandraValue(dynamic.TimeUUID(cql.TimeUUID(rev))), dynamic.Timestamp(modified), dynamic.Bool(false)}
		require.True(t, cassandraReadOne(t, session, insert, values, exec.Options{}).Applied)
		result := cassandraReadOne(t, session, insert, values, exec.Options{})
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
		values := []dynamic.Value{codecTestMust(dynamic.Text("baz")), codecTestMust(dynamic.Text("baz"))}
		ignored := cql.Row(cql.IgnoreColumn[cql.EmptyRow]("foo"), cql.IgnoreColumn[cql.EmptyRow]("bar"))
		require.True(t, cassandraReadOne(t, session, cassandraCASPrepared(t, session, stmt, ignored), values, exec.Options{}).Applied)
		type result struct{ Text cql.Text }
		for _, selected := range []string{"foo", "bar"} {
			other := "foo"
			if selected == "foo" {
				other = "bar"
			}
			decoder := cql.Row(cql.Column(selected, cql.TextCodec, func(v *result) *cql.Text { return &v.Text }), cql.IgnoreColumn[result](other))
			got := cassandraReadOne(t, session, cassandraCASPrepared(t, session, stmt, decoder), values, exec.Options{})
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
			name     string
			override exec.SerialOverride
		}{{"Inherit", exec.SerialInherit}, {"Serial", exec.SerialSerial}, {"Local", exec.SerialLocal}, {"Omit", exec.SerialOmit}, {"Unknown", exec.SerialOmit + 1}} {
			t.Run(tc.name, func(t *testing.T) {
				insert := cassandraWritePrepared(t, session, fmt.Sprintf("INSERT INTO %s(id) VALUES (?)", table), exec.PrepareOptions{})
				// Serial consistency is checked when the options are built.
				options, err := session.CompileOptions(exec.Options{SerialConsistency: tc.override})
				if err == nil {
					_, err = insert.Exec(context.Background(), []dynamic.Value{dynamic.Int(int32(i + 1))}, options)
				}
				if tc.override <= exec.SerialOmit {
					require.NoError(t, err)
					require.Equal(t, int64(i+1), cassandraInt(t, cassandraRead(t, session, fmt.Sprintf("SELECT id FROM %s WHERE id=?", table), dynamic.Int(int32(i+1))), "id"))
				} else {
					require.Error(t, err)
					require.Contains(t, err.Error(), "serial consistency")
				}
			})
		}
	})
}
