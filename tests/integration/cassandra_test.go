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
	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
	"github.com/scylladb/gocql/v2/metadata"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"

	"context"
	"fmt"
	"math"
	"math/big"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

	"github.com/stretchr/testify/require"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/murmur"
)

func TestEmptyHosts(t *testing.T) {
	t.Parallel()
	config := createV2Cluster()
	config.ContactPoints = nil
	session, err := gocql.Connect(context.Background(), config)
	if err == nil {
		session.Close()
		t.Fatal("expected error for empty contact points")
	}
}

func TestInvalidPeerEntry(t *testing.T) {
	t.Parallel()
	if *flagDistribution != "cassandra" {
		t.Skip("incomplete-peer wire fixture uses Cassandra's native v4 metadata")
	}
	fixture := &incompletePeerDialer{}
	config := createV2Cluster(func(c *gocql.Config) {
		c.Protocol = 4
		c.Compression = gocql.NoCompression
		c.TLS = nil
		c.Dialer = fixture
	})
	session, err := gocql.Connect(context.Background(), config)
	require.NoError(t, err)
	defer session.Close()
	require.Positive(t, fixture.injected.Load(), "discovery must consume injected incomplete peer")
	hosts := session.Metadata().Hosts()
	require.Len(t, hosts, *clusterSize)
	for _, host := range hosts {
		require.NotEqual(t, "35c0ec48-5109-40fd-9281-9e9d4add2f1e", host.ID.String())
	}
	require.Equal(t, "local", cassandraText(t, cassandraRead(t, session, "SELECT key FROM system.local"), "key"))
}

// TestUseStatementError checks to make sure the correct error is returned when the user tries to execute a use statement.
func TestUseStatementError(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	_, err := v2Exec(context.Background(), session, ("USE " + fixtureKeyspace("")))
	require.Error(t, err)
	require.ErrorIs(t, err, gocql.ErrUseStmt)
}

// TestInvalidKeyspace checks that an invalid keyspace will return promptly and without a flood of connections
func TestInvalidKeyspace(t *testing.T) {
	t.Parallel()
	config := createV2Cluster()
	config.Keyspace = "invalidKeyspace"
	session, err := gocql.Connect(context.Background(), config)
	if err == nil {
		session.Close()
		t.Fatal("expected invalid keyspace to fail")
	}
	require.Error(t, err)
}

func TestTracing(t *testing.T) {
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
}

func TestObserve(t *testing.T) {
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
	other := createV2Session(t, func(c *gocql.Config) { c.Observer = observer })
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
}

func TestObserve_Pagination(t *testing.T) {
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
}

func TestPaging(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", table))
	for i := int32(0); i < 100; i++ {
		cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s (id) VALUES (?)", table), cql.IntValue(i))
	}
	bound := cassandraUnprepared(t, fmt.Sprintf("SELECT id FROM %s", table), cql.DynamicRows)
	rows := cassandraRows(t, session, bound, gocql.Options{PageSize: cql.Option[int32]{Value: 10, Set: true}})
	require.Len(t, rows, 100)
}

func TestPagingWithAllowFiltering(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	const targetP1, targetP2, totalExpectedResults, pageSize, deletedRangeStart, deletedRangeEnd = 50, 50, 30, 5, 10, 20
	selectCQL := fmt.Sprintf("SELECT c1,f1 FROM %s WHERE p1=50 AND p2=50 AND f1<30 ALLOW FILTERING", table)
	validate := func(t *testing.T, rows []cql.DynamicRow) {
		t.Helper()
		var got []int
		for _, row := range rows {
			c1, f1 := cassandraInt(t, row, "c1"), cassandraInt(t, row, "f1")
			require.Equal(t, c1, f1)
			got = append(got, int(f1))
		}
		sort.Ints(got)
		var want []int
		for i := 0; i < totalExpectedResults; i++ {
			if i < deletedRangeStart || i >= deletedRangeEnd {
				want = append(want, i)
			}
		}
		require.Equal(t, want, got)
	}
	t.Run("Prepare", func(t *testing.T) {
		cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s (p1 int,p2 int,c1 int,f1 int,PRIMARY KEY ((p1,p2),c1)) WITH CLUSTERING ORDER BY (c1 DESC)", table))
		for i := int32(0); i < 100; i++ {
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s (p1,p2,c1,f1) VALUES (?,?,?,?)", table), cql.IntValue(i), cql.IntValue(i), cql.IntValue(i), cql.IntValue(i))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s (p1,p2,c1,f1) VALUES (?,?,?,?)", table), cql.IntValue(targetP1), cql.IntValue(targetP2), cql.IntValue(i), cql.IntValue(i))
		}
		cassandraWrite(t, session, fmt.Sprintf("DELETE FROM %s WHERE p1=? AND p2=? AND c1>=? AND c1<?", table), cql.IntValue(targetP1), cql.IntValue(targetP2), cql.IntValue(deletedRangeStart), cql.IntValue(deletedRangeEnd))
	})
	bound := cassandraUnprepared(t, selectCQL, cql.DynamicRows)
	t.Run("AutoPagination", func(t *testing.T) {
		for _, c := range []cql.Consistency{cql.One, cql.Quorum} {
			t.Run(c.String(), func(t *testing.T) {
				validate(t, cassandraRows(t, session, bound, gocql.Options{Consistency: cql.Option[cql.Consistency]{Value: c, Set: true}, PageSize: cql.Option[int32]{Value: pageSize, Set: true}}))
			})
		}
	})
	t.Run("ManualPagination", func(t *testing.T) {
		for _, c := range []cql.Consistency{cql.One, cql.Quorum} {
			t.Run(c.String(), func(t *testing.T) {
				var result []cql.DynamicRow
				state := gocql.StartPage()
				for {
					page, err := RunPage(context.Background(), session, bound, state, bindOptions(session, gocql.Options{Consistency: cql.Option[cql.Consistency]{Value: c, Set: true}, PageSize: cql.Option[int32]{Value: pageSize, Set: true}}))
					require.NoError(t, err)
					defer page.Close()
					next := page.State()
					// RunPage exposes empty tombstone/filter pages. Their continuation
					// must still advance, and their reported count must match decoded rows.
					pageRows := int64(0)
					var cur cql.DynamicRow
					for page.NextInto(&cur) {
						pageRows++
						row, err := page.Clone()
						require.NoError(t, err)
						result = append(result, row)
					}
					require.Equal(t, page.Info().Rows, pageRows)
					require.NoError(t, page.Close())
					if next.Complete() {
						break
					}
					require.NotEqual(t, state.Bytes(), next.Bytes())
					state = gocql.RestorePageState(next.Bytes())
				}
				validate(t, result)
			})
		}
	})
	cassandraDDL(t, session, "DROP TABLE "+table)
}

func TestPagingWithBind(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s (id int,val int,PRIMARY KEY(id,val))", table))
	for i := int32(0); i < 100; i++ {
		cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,val) VALUES (?,?)", table), cql.IntValue(1), cql.IntValue(i))
	}
	prepared := cassandraPrepared(t, session, cql.Statement[[]cql.Value, cql.DynamicRow]{CQL: fmt.Sprintf("SELECT val FROM %s WHERE id=? AND val<?", table), Params: cql.DynamicParams, Rows: cql.DynamicRows}, gocql.PrepareOptions{})
	first := cassandraBound(t, prepared, []cql.Value{cql.IntValue(1), cql.IntValue(50)})
	second := cassandraBound(t, prepared, []cql.Value{cql.IntValue(1), cql.IntValue(20)})
	options := gocql.Options{PageSize: cql.Option[int32]{Value: 10, Set: true}}
	require.Len(t, cassandraRows(t, session, first, options), 50)
	require.Len(t, cassandraRows(t, session, second, options), 20)
	require.Len(t, cassandraRows(t, session, first, options), 50, "new binding must not mutate old binding")
}

func TestCAS(t *testing.T) {
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
	require.Equal(t, rev, gotRev)
	deleteCQL := fmt.Sprintf("DELETE FROM %s WHERE title=? AND revid=? IF last_modified=?", table)
	failedDelete := cassandraCASBound(t, session, deleteCQL, []cql.Value{codecTestMust(cql.TextValue(title)), revValue, cql.TimestampValue(modified + 10000)}, cql.DynamicRows)
	deleted := cassandraReadBound(t, session, failedDelete, gocql.Options{})
	require.False(t, deleted.Applied)
	require.Equal(t, int64(modified), cassandraInt(t, deleted.Previous.Value, "last_modified"))
	// Missing or incompatible previous-value decoders fail explicitly.
	invalid, err := gocql.Prepare(context.Background(), session, cql.Statement[[]cql.Value, cql.CASResult[cql.EmptyRow]]{CQL: deleteCQL, Params: cql.DynamicParams, Rows: cql.CASRows(cql.EmptyRowDecoder)}, gocql.PrepareOptions{})
	if err == nil {
		bound := cassandraBound(t, invalid, []cql.Value{codecTestMust(cql.TextValue(title)), revValue, cql.TimestampValue(modified + 10000)})
		rows, e := Run(context.Background(), session, bound, gocql.SessionOptions{})
		defer rows.Close()
		err = e
	}
	require.Error(t, err)
	successDelete := cassandraCASBound(t, session, deleteCQL, values, cql.DynamicRows)
	require.True(t, cassandraReadBound(t, session, successDelete, gocql.Options{}).Applied)
	cassandraWrite(t, session, "TRUNCATE "+table)
	runCASBatch := func(items []batchItem, wantApplied bool, wantRows int) {
		t.Helper()
		page, err := runBatch(context.Background(), session, cql.LoggedBatch, items, cql.CASRows(cql.DynamicRows), gocql.SessionOptions{})
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
	_, err = gocql.Prepare(context.Background(), session, cql.Statement[[]cql.Value, cql.CASResult[cql.DynamicRow]]{CQL: fmt.Sprintf("SELECT revid FROM %s WHERE title=?", table), Params: cql.DynamicParams, Rows: cql.CASRows(cql.DynamicRows)}, gocql.PrepareOptions{})
	require.Error(t, err)
	for i := 0; i < 2; i++ {
		page, err := runBatch(context.Background(), session, cql.LoggedBatch, seed, cql.CASRows(cql.DynamicRows), gocql.SessionOptions{})
		if page != nil {
			page.Close()
		}
		require.Error(t, err)
	}
	for _, batch := range []bool{false, true} {
		badDecoder := cql.CASRows(cql.OneColumn(cql.Bool))
		bound := cassandraDynamicBound(t, session, fmt.Sprintf("UPDATE %s SET last_modified=? WHERE title=? AND revid=? IF last_modified=?", table), []cql.Value{cql.TimestampValue(modified + 10000), codecTestMust(cql.TextValue("_foo")), cassandraValue(cql.TimeUUIDValue(cql.TimeUUID(cassandraUUID("3e4ad2f1-73a4-11e5-9381-29463d90c3f0")))), cql.TimestampValue(modified + 20000)}, gocql.PrepareOptions{})
		if batch {
			page, e := runBatch(context.Background(), session, cql.LoggedBatch, []batchItem{cassandraItem(t, bound)}, badDecoder, gocql.SessionOptions{})
			if page != nil {
				page.Close()
			}
			require.Error(t, e)
		} else {
			bad := func() *Bound[cql.CASResult[bool]] {
				opaqueStorageValue := new(Bound[cql.CASResult[bool]])
				implExecution.BoundData(opaqueStorageValue).InternalRequest = implExecution.BoundData(bound).InternalRequest
				implExecution.BoundData(opaqueStorageValue).InternalRows = badDecoder
				return opaqueStorageValue
			}()
			rows, e := Run(context.Background(), session, bad, gocql.SessionOptions{})
			rows.Close()
			require.Error(t, e)
		}
	}
}

func TestConsistencySerial(t *testing.T) {
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
			options, err := gocql.BindOptions(session, gocql.Options{SerialConsistency: cql.Some(tc.consistency)})
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
}

func TestDurationType(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	if implCluster.SessionData(session).InternalCfg.ProtoVersion < 5 {
		t.Skip("skipped below protocol 5 by this test's own guard, not by a limit of the duration type")
	}
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(k int PRIMARY KEY,v duration)", table))
	for _, want := range []cql.Duration{{Months: 250, Days: 500, Nanoseconds: 300010001}, {Months: -250, Days: -500, Nanoseconds: -300010001}, {Days: 128, Nanoseconds: 127}, {Months: 0x7fffffff, Days: 0x7fffffff, Nanoseconds: 0x7fffffffffffffff}} {
		cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(k,v) VALUES(1,?)", table), cassandraValue(cql.DurationValue(want)))
		row := cassandraRead(t, session, "SELECT k,v FROM "+table)
		require.Equal(t, int64(1), cassandraInt(t, row, "k"))
		got, e := cassandraField(t, row, "v").Duration()
		require.NoError(t, e)
		require.Equal(t, want, got)
	}
}

func TestMapScanCAS(t *testing.T) {
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
	require.Equal(t, rev, got)
	require.False(t, cassandraBool(t, row, "deleted"))
	require.Equal(t, int64(modified), cassandraInt(t, row, "last_modified"))
}

func TestBatch(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY)", table))
	prepared := cassandraPrepared(t, session, cql.Statement[int32, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(id) VALUES(?)", table), Params: cql.OneParam(cql.Int32), Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
	var items []batchItem
	for i := int32(0); i < 100; i++ {
		items = append(items, cassandraItem(t, cassandraBound(t, prepared, i)))
	}
	cassandraBatch(t, session, cql.LoggedBatch, items, gocql.Options{})
	require.Equal(t, int64(100), cassandraInt(t, cassandraRead(t, session, "SELECT COUNT(*) FROM "+table), "count"))
}

func TestUnpreparedBatch(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,c counter)", table))
	bound := cassandraUnprepared(t, fmt.Sprintf("UPDATE %s SET c=c+1 WHERE id=1", table), cql.EmptyRowDecoder)
	items := make([]batchItem, 100)
	for i := range items {
		items[i] = cassandraItem(t, bound)
	}
	cassandraBatch(t, session, cql.UnloggedBatch, items, gocql.Options{})
	require.Equal(t, int64(1), cassandraInt(t, cassandraRead(t, session, "SELECT COUNT(*) FROM "+table), "count"))
	require.Equal(t, int64(100), cassandraInt(t, cassandraRead(t, session, "SELECT c FROM "+table), "c"))
}

// TestBatchLimit tests gocql to make sure batch operations larger than the maximum
// statement limit are not submitted to a cassandra node.
func TestBatchLimit(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY)", table))
	bound := cassandraWriteBound(t, session, fmt.Sprintf("INSERT INTO %s(id) VALUES(?)", table), []cql.Value{cql.IntValue(1)}, gocql.PrepareOptions{})
	items := make([]batchItem, 65537)
	for i := range items {
		items[i] = cassandraItem(t, bound)
	}
	before := session.Metrics()
	_, err := execBatch(context.Background(), session, cql.LoggedBatch, items, gocql.SessionOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "65535")
	require.Equal(t, before.Requests, session.Metrics().Requests, "oversized batches must be rejected before submission")
}

func TestWhereIn(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int,cluster int,PRIMARY KEY(id,cluster))", table))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,cluster) VALUES(?,?)", table), cql.IntValue(100), cql.IntValue(200))
	row := cassandraRead(t, session, fmt.Sprintf("SELECT * FROM %s WHERE id=? AND cluster IN (?)", table), cql.IntValue(100), cql.IntValue(200))
	require.Equal(t, int64(100), cassandraInt(t, row, "id"))
	require.Equal(t, int64(200), cassandraInt(t, row, "cluster"))
}

// TestTooManyQueryArgs tests to make sure the library correctly handles the application level bug
// whereby too many query arguments are passed to a query
func TestTooManyQueryArgs(t *testing.T) {
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
}

// TestNotEnoughQueryArgs tests to make sure the library correctly handles the application level bug
// whereby not enough query arguments are passed to a query
func TestNotEnoughQueryArgs(t *testing.T) {
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
}

// TestCreateSessionTimeout tests to make sure the CreateSession function timeouts out correctly
// and prevents an infinite loop of connection retries.
func TestCreateSessionTimeout(t *testing.T) {
	t.Parallel()
	config := createV2Cluster()
	config.ContactPoints = []string{"127.0.0.1:1"}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	session, err := gocql.Connect(ctx, config)
	if err == nil {
		session.Close()
		t.Fatal("expected startup failure")
	}
	require.Less(t, time.Since(start), 2*time.Second)
}

// TestReconnection verifies that a node marked down is eventually reconnected.
// WARNING: This test must NOT use t.Parallel(). It calls session.handleNodeDown()
// which mutates shared HostInfo state visible to all concurrent sessions.
//
//nolint:paralleltest // mutates shared HostInfo state via handleNodeDown()
func TestReconnection(t *testing.T) {
	config := createV2Cluster(func(c *gocql.Config) { c.Reconnect = cassandraReconnect{} })
	session := createV2SessionFromConfig(config, t, false)
	defer session.Close()
	host := implCluster.SessionData(session).InternalHostSource.InternalGetHostsList()[0]
	implCluster.InternalSessionHandleNodeDown(session, host.ConnectAddress(), host.Port())
	defer implEndpoint.InternalHostInfoSetState(host, NodeUp)
	require.Equal(t, NodeDown, host.State())
	require.True(t, pollUntil(31*time.Second, func() bool { return host.State() == NodeUp }), "host did not reconnect")
}

type FullName struct {
	FirstName string
	LastName  string
}

func TestMapScanWithRefMap(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(testtext text PRIMARY KEY,testfullname text,testint int)", table))
	type input struct {
		Text   string
		Name   FullName
		Number int32
	}
	params := cql.Params(cql.Param("testtext", cql.StringEncoder, func(v input) string { return v.Text }), cql.Param("testfullname", fullNameCodec{}, func(v input) FullName { return v.Name }), cql.Param("testint", cql.Int32, func(v input) int32 { return v.Number }))
	prepared := cassandraPrepared(t, session, cql.Statement[input, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(testtext,testfullname,testint) VALUES(?,?,?)", table), Params: params, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
	_, err := Exec(context.Background(), session, cassandraBound(t, prepared, input{"testtext", FullName{"John", "Doe"}, 100}), gocql.SessionOptions{})
	require.NoError(t, err)
	type output struct {
		Text   cql.Text
		Name   FullName
		Number int32
	}
	decoder := cql.Row(cql.Column("testtext", cql.TextCodec, func(v *output) *cql.Text { return &v.Text }), cql.Column("testfullname", fullNameCodec{}, func(v *output) *FullName { return &v.Name }), cql.Column("testint", cql.Int32, func(v *output) *int32 { return &v.Number }))
	got := cassandraReadBound(t, session, cassandraUnprepared(t, "SELECT * FROM "+table, decoder), gocql.Options{})
	require.Equal(t, "testtext", got.Text.String())
	require.Equal(t, FullName{"John", "Doe"}, got.Name)
	require.Equal(t, int32(100), got.Number)
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(testtext,testint) VALUES(?,?)", table), codecTestMust(cql.TextValue("null-int")), cql.NullValue(cql.IntValue(0).Type()))
	nullable := cassandraPrepared(t, session, cql.Statement[string, cql.Nullable[int32]]{CQL: fmt.Sprintf("SELECT testint FROM %s WHERE testtext=?", table), Params: cql.OneParam(cql.StringEncoder), Rows: cql.OneColumn(cql.NullableCodec(cql.Int32))}, gocql.PrepareOptions{})
	require.False(t, cassandraReadBound(t, session, cassandraBound(t, nullable, "null-int"), gocql.Options{}).Valid)
}

func TestMapScan(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(fullname text PRIMARY KEY,age int,address inet,data blob)", table))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(fullname,age,address) VALUES(?,?,?)", table), codecTestMust(cql.TextValue("Grace Hopper")), cql.IntValue(31), cassandraValue(cql.InetValue(netip.MustParseAddr("10.0.0.1"))))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(fullname,age,address,data) VALUES(?,?,?,?)", table), codecTestMust(cql.TextValue("Ada Lovelace")), cql.IntValue(30), cassandraValue(cql.InetValue(netip.MustParseAddr("10.0.0.2"))), cql.BlobValue([]byte(`{"foo": "bar"}`)))
	rows := cassandraRows(t, session, cassandraUnprepared(t, "SELECT * FROM "+table, cql.DynamicRows), gocql.Options{})
	require.Len(t, rows, 2)
	for i, want := range []struct {
		name    string
		age     int64
		address string
	}{{"Ada Lovelace", 30, "10.0.0.2"}, {"Grace Hopper", 31, "10.0.0.1"}} {
		row := rows[i]
		require.Equal(t, want.name, cassandraText(t, row, "fullname"))
		require.Equal(t, want.age, cassandraInt(t, row, "age"))
		ip, err := cassandraField(t, row, "address").Inet()
		require.NoError(t, err)
		require.Equal(t, want.address, ip.String())
		data := cassandraField(t, row, "data")
		if i == 0 {
			b, e := data.Bytes()
			require.NoError(t, e)
			require.Equal(t, []byte(`{"foo": "bar"}`), b.OwnedBytes())
		} else {
			require.Equal(t, cql.Null, data.State())
		}
	}
}

func TestSliceMap(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(testuuid timeuuid PRIMARY KEY,testtimestamp timestamp,testvarchar varchar,testbigint bigint,testblob blob,testbool boolean,testfloat float,testdouble double,testint int,testdecimal decimal,testlist list<text>,testset set<int>,testmap map<varchar,varchar>,testvarint varint,testinet inet)", table))
	textType, err := cql.ScalarType(cql.TypeVarchar)
	require.NoError(t, err)
	intType := cql.IntValue(0).Type()
	listType, err := cql.ListType(textType)
	require.NoError(t, err)
	setType, err := cql.SetType(intType)
	require.NoError(t, err)
	varcharType, err := cql.ScalarType(cql.TypeVarchar)
	require.NoError(t, err)
	mapType, err := cql.MapType(varcharType, varcharType)
	require.NoError(t, err)
	bigInt, ok := new(big.Int).SetString("830169365738487321165427203929228", 10)
	require.True(t, ok)
	expected := []cql.ValueField{{Name: "testuuid", Value: cassandraValue(cql.TimeUUIDValue(cql.TimeUUID(cql.NewTimeUUID())))}, {Name: "testtimestamp", Value: cql.TimestampValue(cql.Timestamp(time.Now().UnixMilli()))}, {Name: "testvarchar", Value: codecTestMust(cql.TextValue("Test VarChar"))}, {Name: "testbigint", Value: cql.BigIntValue(time.Now().Unix())}, {Name: "testblob", Value: cql.BlobValue([]byte("test blob"))}, {Name: "testbool", Value: cql.BoolValue(true)}, {Name: "testfloat", Value: cql.FloatValue(4.564)}, {Name: "testdouble", Value: cql.DoubleValue(4.815162342)}, {Name: "testint", Value: cql.IntValue(2343)}, {Name: "testdecimal", Value: cql.DecimalValue(cql.Decimal{Unscaled: cql.VarintFromBigInt(big.NewInt(100))})}, {Name: "testlist", Value: cassandraValue(cql.ListValue(listType, []cql.Value{codecTestMust(cql.TextValue("quux")), codecTestMust(cql.TextValue("foo")), codecTestMust(cql.TextValue("bar")), codecTestMust(cql.TextValue("baz")), codecTestMust(cql.TextValue("quux"))}))}, {Name: "testset", Value: cassandraValue(cql.SetValue(setType, []cql.Value{cql.IntValue(1), cql.IntValue(2), cql.IntValue(3), cql.IntValue(4), cql.IntValue(5), cql.IntValue(6), cql.IntValue(7), cql.IntValue(8), cql.IntValue(9)}))}, {Name: "testmap", Value: cassandraValue(cql.MapValue(mapType, []cql.ValueEntry{{Key: codecTestMust(cql.TextValue("field1")), Value: codecTestMust(cql.TextValue("val1"))}, {Key: codecTestMust(cql.TextValue("field2")), Value: codecTestMust(cql.TextValue("val2"))}, {Key: codecTestMust(cql.TextValue("field3")), Value: codecTestMust(cql.TextValue("val3"))}}))}, {Name: "testvarint", Value: cql.VarintValue(cql.VarintFromBigInt(bigInt))}, {Name: "testinet", Value: cassandraValue(cql.InetValue(netip.MustParseAddr("213.212.2.19")))}}
	var values []cql.Value
	for _, field := range expected {
		values = append(values, field.Value)
	}
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(testuuid,testtimestamp,testvarchar,testbigint,testblob,testbool,testfloat,testdouble,testint,testdecimal,testlist,testset,testmap,testvarint,testinet) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", table), values...)
	check := func(row cql.DynamicRow) {
		t.Helper()
		require.Equal(t, len(expected), row.Columns().Len())
		for _, field := range expected {
			actual := cassandraField(t, row, field.Name)
			want, err := cql.EncodeValueOwned(actual.Type(), cql.DynamicCodec, field.Value)
			require.NoError(t, err, field.Name)
			got, err := cql.EncodeValueOwned(actual.Type(), cql.DynamicCodec, actual)
			require.NoError(t, err, field.Name)
			require.Equal(t, want.State(), got.State(), field.Name)
			require.Equal(t, want.Bytes().OwnedBytes(), got.Bytes().OwnedBytes(), field.Name)
		}
	}
	// Preserve the three result-consumption forms as cloned slices, a cursor,
	// and an exactly-one-row read, all with explicit dynamic values.
	bound := cassandraUnprepared(t, "SELECT * FROM "+table, cql.DynamicRows)
	rows := cassandraRows(t, session, bound, gocql.Options{})
	require.Len(t, rows, 1)
	check(rows[0])
	cursor, err := Run(context.Background(), session, bound, gocql.SessionOptions{})
	require.NoError(t, err)
	defer cursor.Close()
	var row cql.DynamicRow
	require.True(t, cursor.NextInto(&row))
	check(row)
	require.NoError(t, cursor.Close())
	check(cassandraRead(t, session, "SELECT * FROM "+table))
}

func Test_RetryPolicyIdempotence(t *testing.T) {
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
}

func TestSmallInt(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(testsmallint smallint PRIMARY KEY)", table))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(testsmallint) VALUES(?)", table), cql.SmallIntValue(2))
	require.Equal(t, int64(2), cassandraInt(t, cassandraRead(t, session, "SELECT * FROM "+table), "testsmallint"))
}

func TestScanWithNilArguments(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(foo varchar,bar int,PRIMARY KEY(foo,bar))", table))
	for i := int32(1); i <= 20; i++ {
		cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(foo,bar) VALUES(?,?)", table), codecTestMust(cql.TextValue("squares")), cql.IntValue(i*i))
	}
	type result struct{ N int32 }
	decoder := cql.Row(cql.IgnoreColumn[result]("foo"), cql.Column("bar", cql.Int32, func(r *result) *int32 { return &r.N }))
	prepared := cassandraPrepared(t, session, cql.Statement[string, result]{CQL: fmt.Sprintf("SELECT * FROM %s WHERE foo=?", table), Params: cql.OneParam(cql.StringEncoder), Rows: decoder}, gocql.PrepareOptions{})
	rows, err := Run(context.Background(), session, cassandraBound(t, prepared, "squares"), gocql.SessionOptions{})
	require.NoError(t, err)
	defer rows.Close()
	var sum int32
	var row result
	for rows.NextInto(&row) {
		sum += row.N
	}
	require.NoError(t, rows.Close())
	require.Equal(t, int32(2870), sum)
}

func TestScanCASWithNilArguments(t *testing.T) {
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
}

func TestRebindQueryInfo(t *testing.T) {
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
}

// TestStaticQueryInfo makes sure that the application can manually bind query parameters using the simplest possible static binding strategy
func TestStaticQueryInfo(t *testing.T) {
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
}

type ClusteredKeyValue struct {
	Id      int32
	Cluster int32
	Value   string
}

// TestBoundQueryInfo makes sure that the application can manually bind query parameters using the query meta data supplied at runtime
func TestBoundQueryInfo(t *testing.T) {
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
}

// TestBatchQueryInfo makes sure that the application can manually bind query parameters when executing in a batch
func TestBatchQueryInfo(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int,cluster int,value text,PRIMARY KEY(id,cluster))", table))
	params := cql.Params(cql.Param("id", cql.Int32, func(v ClusteredKeyValue) int32 { return v.Id }), cql.Param("cluster", cql.Int32, func(v ClusteredKeyValue) int32 { return v.Cluster }), cql.Param("value", cql.StringEncoder, func(v ClusteredKeyValue) string { return v.Value }))
	prepared := cassandraPrepared(t, session, cql.Statement[ClusteredKeyValue, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(id,cluster,value) VALUES(?,?,?)", table), Params: params, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
	require.Equal(t, 3, prepared.Parameters().Len())
	bound := cassandraBound(t, prepared, ClusteredKeyValue{4000, 5000, "bar"})
	cassandraBatch(t, session, cql.LoggedBatch, []batchItem{cassandraItem(t, bound)}, gocql.Options{})
	read := cassandraDynamicBound(t, session, fmt.Sprintf("SELECT id,cluster,value FROM %s WHERE id=? AND cluster=?", table), []cql.Value{cql.IntValue(4000), cql.IntValue(5000)}, gocql.PrepareOptions{})
	row := cassandraReadBound(t, session, read, gocql.Options{})
	require.Equal(t, "bar", cassandraText(t, row, "value"))
	require.Equal(t, int64(4000), cassandraInt(t, row, "id"))
	require.Equal(t, int64(5000), cassandraInt(t, row, "cluster"))
}

// hostConnWaitTimeout bounds the waits below. Generous on purpose: exceeding it means
// the pools never came up, which is a failure worth reporting rather than a slow start.
const hostConnWaitTimeout = 15 * time.Second

// pollUntil calls cond every 50ms until it holds or timeout elapses.
func pollUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.After(timeout)
	for {
		if cond() {
			return true
		}

		select {
		case <-deadline:
			return false
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// waitForHostConns returns one pooled connection per host the session considers up,
// waiting until every one of them has yielded a connection.
//
// createV2Session waits for schema agreement, not for connections, and hostConnPool.pickable
// reports false while a pool is still empty -- it only schedules the fill. A host walk run
// straight after createV2Session therefore skips whichever pools have not come up yet, which
// silently narrows a per-host assertion down to the node that answered first.
//
// The host list is re-read on every attempt, so a host marked up while this waits is
// included and one marked down stops being required.
func waitForHostConns(t *testing.T, session *gocql.Session) map[string]*driverConn {
	t.Helper()

	conns := make(map[string]*driverConn)
	var pending, down []string

	ok := pollUntil(hostConnWaitTimeout, func() bool {
		clear(conns)
		pending = pending[:0]
		down = down[:0]

		for _, host := range implCluster.SessionData(session).InternalHostSource.InternalGetHostsList() {
			// Collected rather than merely skipped: a host that stays down keeps the
			// count below *clusterSize while contributing nothing to pending, so the
			// failure below would otherwise report "2 of 3 connected, 0 still without
			// a connection: []" and name no address to go and look at.
			if !host.IsUp() {
				down = append(down, host.ConnectAddressAndPort())
				continue
			}

			addr := host.ConnectAddressAndPort()
			pool, found := implCluster.SessionData(session).InternalPool.InternalGetPool(host)
			if !found {
				pending = append(pending, addr)
				continue
			}

			if conn := pool.Pick(nil); conn != nil {
				conns[addr] = conn
			} else {
				pending = append(pending, addr)
			}
		}

		// Against *clusterSize rather than "at least one": hostSource discovers peers
		// asynchronously too, so early on the list itself is short and every host in it
		// can have a connection while two nodes are not represented at all. Waiting for
		// len(pending) == 0 alone would be satisfied by that, and reintroduce one level
		// up exactly the "passed having checked one of three" hole this closes.
		//
		// >= rather than ==, so a session that has discovered more up hosts than
		// -clusterSize claims still converges instead of polling to the deadline.
		return len(pending) == 0 && len(conns) >= *clusterSize
	})

	if !ok {
		t.Fatalf("after %s the session had %d of %d hosts connected; up host(s) still without a connection: %v; host(s) marked down: %v",
			hostConnWaitTimeout, len(conns), *clusterSize, pending, down)
	}

	return conns
}

// getRandomConn returns a pooled connection, waiting for one rather than failing the
// moment none is pickable. See waitForHostConns: an empty pool straight after
// createV2Session is a pool still filling, not an anomaly.
func getRandomConn(t *testing.T, session *gocql.Session) *driverConn {
	t.Helper()

	var conn *driverConn
	if !pollUntil(hostConnWaitTimeout, func() bool {
		conn = implCluster.InternalSessionGetConn(session)
		return conn != nil
	}) {
		t.Fatal("unable to get a connection")
	}
	return conn
}

func injectInvalidPreparedStatement(t *testing.T, session *gocql.Session, table string) (*Bound[cql.EmptyRow], *driverConn) {
	t.Helper()
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(foo varchar,bar int,PRIMARY KEY(foo,bar))", table))
	conn := getRandomConn(t, session)
	prepared := cassandraPrepared(t, session, cql.Statement[string, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(foo,bar) VALUES(?,7)", table), Params: cql.OneParam(cql.StringEncoder), Rows: cql.EmptyRowDecoder}, *func() *gocql.PrepareOptions {
		opaqueStorageValuex := new(gocql.PrepareOptions)
		opaqueStorageValuex.HostID = cql.Option[cql.UUID]{Value: implEndpoint.InternalHostInfoHostUUID(conn.InternalHost), Set: true}
		implExecution.PrepareOptionsData(opaqueStorageValuex).InternalConnection = conn
		return opaqueStorageValuex
	}())
	implExecution.PreparedData(prepared).InternalState.InternalMu.Lock()
	host := implExecution.PreparedData(prepared).InternalState.InternalHosts[implEndpoint.InternalHostInfoHostUUID(conn.InternalHost)]
	host.InternalId = []byte("foobar")
	implExecution.PreparedData(prepared).InternalState.InternalHosts[implEndpoint.InternalHostInfoHostUUID(conn.InternalHost)] = host
	implExecution.PreparedData(prepared).InternalState.InternalMu.Unlock()
	return cassandraBound(t, prepared, "bar"), conn
}

func TestPrepare_MissingSchemaPrepare(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	conn := getRandomConn(t, session)
	statement := cql.Statement[int32, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(val) VALUES(?)", table), Params: cql.OneParam(cql.Int32), Rows: cql.EmptyRowDecoder}
	options := gocql.PrepareOptions{HostID: cql.Option[cql.UUID]{Value: implEndpoint.InternalHostInfoHostUUID(conn.InternalHost), Set: true}}
	_, err := gocql.Prepare(context.Background(), session, statement, options)
	require.Error(t, err)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(val int PRIMARY KEY)", table))
	prepared := cassandraPrepared(t, session, statement, options)
	_, err = Exec(context.Background(), session, cassandraBound(t, prepared, int32(5)), bindOptions(session, gocql.Options{HostID: options.HostID}))
	require.NoError(t, err)
}

func TestPrepare_ReprepareStatement(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	bound, conn := injectInvalidPreparedStatement(t, session, table)
	_, err := Exec(context.Background(), session, bound, bindOptions(session, *func() *gocql.Options {
		opaqueStorageValuexx := new(gocql.Options)
		opaqueStorageValuexx.HostID = cql.Option[cql.UUID]{Value: implEndpoint.InternalHostInfoHostUUID(conn.InternalHost), Set: true}
		implExecution.OptionsData(opaqueStorageValuexx).InternalConnection = conn
		return opaqueStorageValuexx
	}()))
	require.NoError(t, err)
	implExecution.BoundData(bound).InternalRequest.InternalPrepared().InternalMu.RLock()
	id := append([]byte(nil), implExecution.BoundData(bound).InternalRequest.InternalPrepared().InternalHosts[implEndpoint.InternalHostInfoHostUUID(conn.InternalHost)].InternalId...)
	implExecution.BoundData(bound).InternalRequest.InternalPrepared().InternalMu.RUnlock()
	require.NotEqual(t, []byte("foobar"), id)
	require.Equal(t, int64(7), cassandraInt(t, cassandraRead(t, session, "SELECT bar FROM "+table+" WHERE foo=?", codecTestMust(cql.TextValue("bar"))), "bar"))
}

func TestPrepare_ReprepareBatch(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	bound, conn := injectInvalidPreparedStatement(t, session, table)
	cassandraBatch(t, session, cql.UnloggedBatch, []batchItem{cassandraItem(t, bound)}, *func() *gocql.Options {
		opaqueStorageValuexxx := new(gocql.Options)
		opaqueStorageValuexxx.HostID = cql.Option[cql.UUID]{Value: implEndpoint.InternalHostInfoHostUUID(conn.InternalHost), Set: true}
		implExecution.OptionsData(opaqueStorageValuexxx).InternalConnection = conn
		return opaqueStorageValuexxx
	}())
	implExecution.BoundData(bound).InternalRequest.InternalPrepared().InternalMu.RLock()
	id := append([]byte(nil), implExecution.BoundData(bound).InternalRequest.InternalPrepared().InternalHosts[implEndpoint.InternalHostInfoHostUUID(conn.InternalHost)].InternalId...)
	implExecution.BoundData(bound).InternalRequest.InternalPrepared().InternalMu.RUnlock()
	require.NotEqual(t, []byte("foobar"), id)
	require.Equal(t, int64(7), cassandraInt(t, cassandraRead(t, session, "SELECT bar FROM "+table+" WHERE foo=?", codecTestMust(cql.TextValue("bar"))), "bar"))
}

func TestQueryInfo(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	prepared := cassandraPrepared(t, session, cql.Statement[string, cql.DynamicRow]{CQL: "SELECT release_version,host_id FROM system.local WHERE key=?", Params: cql.OneParam(cql.StringEncoder), Rows: cql.DynamicRows}, gocql.PrepareOptions{})
	require.Equal(t, 1, prepared.Parameters().Len())
	require.Equal(t, 2, prepared.Columns().Len())
}

// TestPreparedCacheEviction will make sure that the cache size is maintained
func TestPrepare_PreparedCacheEviction(t *testing.T) {
	t.Parallel()
	const maxPrepared = 4
	session := createV2Session(t, func(c *gocql.Config) { c.PreparedCacheSize = maxPrepared; c.Metadata.SchemaEvents = false })
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
		cassandraWrite(t, session, insert, cql.IntValue(i), cql.IntValue(10000%(i+1)))
	}
	selects := []string{fmt.Sprintf("SELECT id,mod FROM %s WHERE id=0", table), fmt.Sprintf("SELECT id,mod FROM %s WHERE id=1", table)}
	for _, stmt := range selects {
		prepared := cassandraPrepared(t, session, cql.Statement[cql.EmptyParams, cql.DynamicRow]{CQL: stmt, Params: cql.EmptyParamsEncoder, Rows: cql.DynamicRows}, gocql.PrepareOptions{})
		cassandraReadBound(t, session, cassandraBound(t, prepared, cql.EmptyParams{}), gocql.Options{})
	}
	update := fmt.Sprintf("UPDATE %s SET mod=? WHERE id=?", table)
	cassandraWrite(t, session, update, cql.IntValue(1), cql.IntValue(11))
	del := fmt.Sprintf("DELETE FROM %s WHERE id=?", table)
	cassandraWrite(t, session, del, cql.IntValue(1))
	cassandraWrite(t, session, insert, cql.IntValue(3), cql.IntValue(11))
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
}

func TestPrepare_PreparedCacheKey(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	ks := testKeyspaceName(t, "ks2")
	other := cassandraOtherSession(t, session, ks)
	defer other.Close()
	defer cassandraDDL(t, session, "DROP KEYSPACE "+ks)
	for _, s := range []*gocql.Session{session, other} {
		cassandraDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id varchar PRIMARY KEY,field varchar)", table))
	}
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,field) VALUES(?,?)", table), codecTestMust(cql.TextValue("key")), codecTestMust(cql.TextValue("one")))
	cassandraWrite(t, other, fmt.Sprintf("INSERT INTO %s(id,field) VALUES(?,?)", table), codecTestMust(cql.TextValue("key")), codecTestMust(cql.TextValue("two")))
	stmt := fmt.Sprintf("SELECT field FROM %s WHERE id=?", table)
	require.Equal(t, "one", cassandraText(t, cassandraRead(t, session, stmt, codecTestMust(cql.TextValue("key"))), "field"))
	require.Equal(t, "two", cassandraText(t, cassandraRead(t, other, stmt, codecTestMust(cql.TextValue("key"))), "field"))
}

// TestMarshalFloat64Ptr tests to see that a pointer to a float64 is marshalled correctly.
func TestMarshalFloat64Ptr(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id double PRIMARY KEY,test double)", table))
	testNum := 7500.0
	params := cql.Params(cql.Param("id", cql.Float64, func(v *float64) float64 { return *v }), cql.Param("test", cql.Float64, func(v *float64) float64 { return *v }))
	prepared := cassandraPrepared(t, session, cql.Statement[*float64, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(id,test) VALUES(?,?)", table), Params: params, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
	_, err := Exec(context.Background(), session, cassandraBound(t, prepared, &testNum), gocql.SessionOptions{})
	require.NoError(t, err)
	got, err := cassandraField(t, cassandraRead(t, session, "SELECT test FROM "+table), "test").Float()
	require.NoError(t, err)
	require.Equal(t, testNum, got)
}

// TestMarshalInet tests to see that a pointer to a float64 is marshalled correctly.
func TestMarshalInet(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(ip inet PRIMARY KEY,name text)", table))
	ipFromNet, ok := netip.AddrFromSlice(net.ParseIP("222.43.54.65"))
	require.True(t, ok)
	for _, want := range []netip.Addr{netip.MustParseAddr("123.34.45.56"), ipFromNet.Unmap()} {
		cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(ip,name) VALUES(?,?)", table), cassandraValue(cql.InetValue(want)), codecTestMust(cql.TextValue("Test IP")))
		for i := 0; i < 2; i++ {
			row := cassandraRead(t, session, "SELECT ip FROM "+table)
			got, err := cassandraField(t, row, "ip").Inet()
			require.NoError(t, err)
			require.Equal(t, want.String(), got.String())
			require.Equal(t, want, got)
		}
		cassandraWrite(t, session, fmt.Sprintf("DELETE FROM %s WHERE ip=?", table), cassandraValue(cql.InetValue(want)))
	}
}

func TestVarint(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id varchar PRIMARY KEY,test varint,test2 varint)", table))
	insert := fmt.Sprintf("INSERT INTO %s(id,test) VALUES(?,?)", table)
	selectCQL := "SELECT test FROM " + table
	for _, want := range []int64{0, -1, int64(math.MaxInt32) + 1} {
		cassandraWrite(t, session, insert, codecTestMust(cql.TextValue("id")), cql.VarintValue(cql.VarintFromBigInt(big.NewInt(want))))
		row := cassandraRead(t, session, selectCQL)
		got, err := cassandraField(t, row, "test").Varint()
		require.NoError(t, err)
		require.Equal(t, want, got.BigInt().Int64())
	}
	cassandraWrite(t, session, insert, codecTestMust(cql.TextValue("id")), cql.NullValue(cql.VarintValue(cql.Varint{}).Type()))
	nullable := cassandraUnprepared(t, selectCQL, cql.OneColumn(cql.NullableCodec(cql.VarintCodec)))
	null := cassandraReadBound(t, session, nullable, gocql.Options{})
	require.False(t, null.Valid)
	require.Zero(t, null.Value.BigInt().Sign())
	biggie, ok := new(big.Int).SetString("36893488147419103232", 10)
	require.True(t, ok)
	cassandraWrite(t, session, insert, codecTestMust(cql.TextValue("id")), cql.VarintValue(cql.VarintFromBigInt(biggie)))
	got, err := cassandraField(t, cassandraRead(t, session, selectCQL), "test").Varint()
	require.NoError(t, err)
	require.Equal(t, biggie.String(), got.BigInt().String())
	rows, err := Run(context.Background(), session, cassandraUnprepared(t, selectCQL, cql.OneColumn(cassandraVarintInt64{})), gocql.SessionOptions{})
	require.NoError(t, err)
	defer rows.Close()
	var narrowed int64
	require.False(t, rows.NextInto(&narrowed))
	require.ErrorContains(t, rows.Close(), "int64 range")
	absent := cassandraReadBound(t, session, cassandraUnprepared(t, "SELECT test2 FROM "+table, cql.OneColumn(cql.NullableCodec(cql.VarintCodec))), gocql.Options{})
	require.False(t, absent.Valid)
	require.Zero(t, absent.Value.BigInt().Sign())
}

// TestQueryStats confirms that the stats are returning valid data. Accuracy may be questionable.
func TestQueryStats(t *testing.T) {
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
}

// TestIterHosts confirms that host is added to Iter when the query succeeds.
func TestIterHost(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	rows, err := v2Query(context.Background(), session, "SELECT * FROM system.peers")
	require.NoError(t, err)
	defer rows.Close()
	require.NotEqual(t, cql.UUID{}, rows.Info().HostID)
}

// TestBatchStats confirms that the stats are returning valid data. Accuracy may be questionable.
func TestBatchStats(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY)", table))
	var items []batchItem
	for _, id := range []int32{1, 2} {
		items = append(items, cassandraItem(t, cassandraWriteBound(t, session, fmt.Sprintf("INSERT INTO %s(id) VALUES(?)", table), []cql.Value{cql.IntValue(id)}, gocql.PrepareOptions{})))
	}
	info := cassandraBatch(t, session, cql.LoggedBatch, items, gocql.Options{})
	require.Positive(t, info.Attempts)
	require.Positive(t, info.Latency)
}

func TestBatchObserve(t *testing.T) {
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
}

// TestNilInQuery tests to see that a nil value passed to a query is handled by Cassandra
// TODO validate the nil value by reading back the nil. Need to fix Unmarshalling.
func TestNilInQuery(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,count int)", table))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,count) VALUES(?,?)", table), cql.IntValue(1), cql.NullValue(cql.IntValue(0).Type()))
	row := cassandraRead(t, session, "SELECT id,count FROM "+table)
	require.Equal(t, int64(1), cassandraInt(t, row, "id"))
	require.Equal(t, cql.Null, cassandraField(t, row, "count").State())
}

// Don't initialize time.Time bind variable if cassandra timestamp column is empty
func TestEmptyTimestamp(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,time timestamp,num int)", table))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,num) VALUES(?,?)", table), cql.IntValue(1), cql.IntValue(561))
	read := cassandraPrepared(t, session, cql.Statement[int32, cql.Nullable[cql.Timestamp]]{CQL: fmt.Sprintf("SELECT time FROM %s WHERE id=?", table), Params: cql.OneParam(cql.Int32), Rows: cql.OneColumn(cql.NullableCodec(cql.TimestampCodec))}, gocql.PrepareOptions{})
	value := cassandraReadBound(t, session, cassandraBound(t, read, int32(1)), gocql.Options{})
	require.False(t, value.Valid)
	require.Zero(t, value.Value)
}

// Integration test of just querying for data from the system.schema_keyspace table where the keyspace DOES exist.
func TestGetKeyspaceMetadata(t *testing.T) {
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
}

func TestSessionMetadataAPIs(t *testing.T) {
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
}

func tableNames(tables map[string]*metadata.Table) []string {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	return names
}

func columnNames(columns map[string]*metadata.Column) []string {
	names := make([]string, 0, len(columns))
	for name := range columns {
		names = append(names, name)
	}
	return names
}

// Integration test of just querying for data from the system.schema_keyspace table where the keyspace DOES NOT exist.
func TestGetKeyspaceMetadataFails(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	_, found := session.Metadata().Keyspace("gocql_keyspace_does_not_exist")
	require.False(t, found, "snapshot must report a missing keyspace")
}

// Integration test of the routing key calculation
func TestRoutingKey(t *testing.T) {
	t.Parallel()
	session := createV2Session(t, func(c *gocql.Config) { c.Metadata.SchemaEvents = false })
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
		require.Same(t, implExecution.PreparedData(prepared).InternalState, implExecution.PreparedData(again).InternalState, "preparing again must hit typed cache")
		bound := cassandraBound(t, prepared, []cql.Value{cql.IntValue(1), cql.IntValue(2)})
		key, present, err := newPartitionKey(implExecution.BoundData(bound).InternalRequest.InternalValues.Cells, prepared.Info().PartitionKeyIndexes)
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
}

// Integration test of the token-aware policy-based connection pool
func TestTokenAwareConnPool(t *testing.T) {
	t.Parallel()
	session := createV2Session(t, func(c *gocql.Config) {
		c.Pool.ConnectionsPerHost = 1
		p := *c.DefaultProfile
		p.PageSize = 1
		c.DefaultProfile = &p
	})
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
}

func TestNegativeStream(t *testing.T) {
	t.Parallel()

	session := createV2Session(t)
	defer session.Close()

	conn := getRandomConn(t, session)

	const stream = -50
	writer := frameWriterFunc(func(f *framer, streamID int) error {
		f.WriteHeader(0, frm.OpOptions, stream)
		return f.Finish()
	})

	frame, err := conn.InternalExec(context.Background(), writer, nil, time.Second)
	if err == nil {
		t.Fatalf("expected to get an error on stream %d", stream)
	} else if frame != nil {
		t.Fatalf("expected to get nil frame got %+v", frame)
	}
}

func TestManualQueryPaging(t *testing.T) {
	t.Parallel()
	const count = 5
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,count int)", table))
	for i := int32(0); i < count; i++ {
		cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,count) VALUES(?,?)", table), cql.IntValue(i), cql.IntValue(i*i))
	}
	bound := cassandraUnprepared(t, "SELECT id,count FROM "+table, cql.DynamicRows)
	state := gocql.StartPage()
	fetched := 0
	for {
		page, err := RunPage(context.Background(), session, bound, state, bindOptions(session, gocql.Options{PageSize: cql.Option[int32]{Value: 2, Set: true}}))
		require.NoError(t, err)
		defer page.Close()
		var row cql.DynamicRow
		for page.NextInto(&row) {
			id := cassandraInt(t, row, "id")
			require.Equal(t, id*id, cassandraInt(t, row, "count"))
			fetched++
		}
		state = page.State()
		require.NoError(t, page.Close())
		if state.Complete() {
			break
		}
		state = gocql.RestorePageState(state.Bytes())
	}
	require.Equal(t, count, fetched)
}

// Issue 475
func TestSessionBindRoutingKey(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(key varchar PRIMARY KEY,value int)", table))
	type input struct {
		Key   string
		Value int32
	}
	params := cql.Params(cql.Param("key", cql.StringEncoder, func(v input) string { return v.Key }), cql.Param("value", cql.Int32, func(v input) int32 { return v.Value }))
	prepared := cassandraPrepared(t, session, cql.Statement[input, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(key,value) VALUES(?,?)", table), Params: params, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
	require.Equal(t, []int{0}, prepared.Info().PartitionKeyIndexes)
	bound := cassandraBound(t, prepared, input{"routing-key", 5})
	require.True(t, bound.Routing().Token.Set)
	require.Equal(t, murmur.Murmur3H1([]byte("routing-key")), bound.Routing().Token.Value)
	_, err := Exec(context.Background(), session, bound, gocql.SessionOptions{})
	require.NoError(t, err)
}

func TestJSONSupport(t *testing.T) {
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
}

func TestUnmarshallNestedTypes(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id text PRIMARY KEY,val list<frozen<map<text,text>>>)", table))
	type nested = []cql.Entry[string, string]
	want := []nested{{{Key: "key1", Value: "val1"}}, {{Key: "key2", Value: "val2"}}}
	prepared := cassandraPrepared(t, session, cql.Statement[[]nested, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(id,val) VALUES('key',?)", table), Params: cql.OneParam(cql.ListEncoder(cql.MapEncoder(cql.StringEncoder, cql.StringEncoder))), Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
	_, err := Exec(context.Background(), session, cassandraBound(t, prepared, want), gocql.SessionOptions{})
	require.NoError(t, err)
	row := cassandraReadBound(t, session, cassandraUnprepared(t, fmt.Sprintf("SELECT val FROM %s WHERE id='key'", table), cql.OneColumn(cql.ListDecoder(cql.MapDecoder(cql.TextCodec, cql.TextCodec)))), gocql.Options{})
	iter := row.Iterator()
	var got []nested
	for iter.Next() {
		var entries nested
		mapIter := iter.Value().Iterator()
		for mapIter.Next() {
			entry := mapIter.Value()
			entries = append(entries, cql.Entry[string, string]{Key: entry.Key.String(), Value: entry.Value.String()})
		}
		require.NoError(t, mapIter.Err())
		got = append(got, entries)
	}
	require.NoError(t, iter.Err())
	require.Equal(t, want, got)
}

func TestSchemaReset(t *testing.T) {
	t.Parallel()
	if flagCassVersion.Major == 0 || flagCassVersion.Before(2, 1, 3) {
		t.Skipf("skipping TestSchemaReset due to CASSANDRA-7910 in Cassandra <2.1.3 version=%v", flagCassVersion)
	}
	session := createV2Session(t, func(c *gocql.Config) { c.Pool.ConnectionsPerHost = 1 })
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id text PRIMARY KEY)", table))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id) VALUES(?)", table), codecTestMust(cql.TextValue("test")))
	selectCQL := fmt.Sprintf("SELECT * FROM %s WHERE id=?", table)
	before := cassandraDynamicBound(t, session, selectCQL, []cql.Value{codecTestMust(cql.TextValue("test"))}, gocql.PrepareOptions{})
	require.Equal(t, "test", cassandraText(t, cassandraReadBound(t, session, before, gocql.Options{}), "id"))
	cassandraDDL(t, session, fmt.Sprintf("ALTER TABLE %s ADD val text", table))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,val) VALUES(?,?)", table), codecTestMust(cql.TextValue("test")), codecTestMust(cql.TextValue("test-val")))
	row := cassandraReadBound(t, session, before, gocql.Options{})
	require.Equal(t, "test", cassandraText(t, row, "id"))
	require.Equal(t, "test-val", cassandraText(t, row, "val"))
}

func TestCreateSession_DontSwallowError(t *testing.T) {
	t.Parallel()
	config := createV2Cluster()
	config.Protocol = 0x100
	session, err := gocql.Connect(context.Background(), config)
	if err == nil {
		session.Close()
		t.Fatal("expected unsupported protocol error")
	}
	require.Contains(t, err.Error(), "protocol")
}

func TestControl_DiscoverProtocol(t *testing.T) {
	t.Parallel()
	config := createV2Cluster()
	config.Protocol = 0
	config.Compression = gocql.NoCompression
	session, err := gocql.Connect(context.Background(), config)
	require.NoError(t, err)
	defer session.Close()
	require.NotZero(t, implCluster.SessionData(session).InternalCfg.ProtoVersion)
}

// TestUnsetCol verify unset column will not replace an existing column
func TestUnsetCol(t *testing.T) {
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
}

// TestUnsetColBatch verify unset column will not replace a column in batch
func TestUnsetColBatch(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	if implCluster.SessionData(session).InternalCfg.ProtoVersion < 4 {
		t.Skip("Unset Values are not supported in protocol < 4")
	}
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,my_int int,my_text text)", table))
	stmt := fmt.Sprintf("INSERT INTO %s(id,my_int,my_text) VALUES(?,?,?)", table)
	var items []batchItem
	for _, values := range [][]cql.Value{{cql.IntValue(1), cql.IntValue(1), cql.UnsetDynamic(codecTestMust(cql.TextValue("")).Type())}, {cql.IntValue(1), cql.UnsetDynamic(cql.IntValue(0).Type()), codecTestMust(cql.TextValue(""))}, {cql.IntValue(2), cql.IntValue(2), cql.UnsetDynamic(codecTestMust(cql.TextValue("")).Type())}} {
		items = append(items, cassandraItem(t, cassandraWriteBound(t, session, stmt, values, gocql.PrepareOptions{})))
	}
	info := cassandraBatch(t, session, cql.LoggedBatch, items, gocql.Options{})
	require.Positive(t, info.Attempts)
	require.Positive(t, info.Latency)
	require.Equal(t, int64(2), cassandraInt(t, cassandraRead(t, session, "SELECT count(*) FROM "+table), "count"))
	row := cassandraRead(t, session, "SELECT id,my_int,my_text FROM "+table+" WHERE id=1")
	require.Equal(t, int64(1), cassandraInt(t, row, "id"))
	require.Equal(t, int64(1), cassandraInt(t, row, "my_int"))
	require.Equal(t, "", cassandraText(t, row, "my_text"))
}

func TestQuery_NamedValues(t *testing.T) {
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
}

// TestQuery_SetHostID ensures that queries are sent to the specified host only.
// WARNING: This test must NOT use t.Parallel(). It calls pool.host.setState(NodeDown)
// which mutates shared HostInfo state visible to all concurrent sessions.
//
//nolint:paralleltest // mutates shared HostInfo state via setState(NodeDown)
func TestQuery_SetHostID(t *testing.T) {
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
	var invalidHost *cql.ValidationError
	require.ErrorAs(t, err, &invalidHost, "an empty host ID is rejected when options are built")
	_, err = Run(context.Background(), session, bound, bindOptions(session, gocql.Options{HostID: cql.Option[cql.UUID]{Value: codecTestMust(cql.RandomUUID()), Set: true}}))
	require.ErrorIs(t, err, gocql.ErrNoConnections)
	host := hosts[0]
	pool, ok := implCluster.SessionData(session).InternalPool.InternalGetPoolByHostID(host.ID.String())
	require.True(t, ok)
	implEndpoint.InternalHostInfoSetState(pool.InternalHost, NodeDown)
	defer implEndpoint.InternalHostInfoSetState(pool.InternalHost, NodeUp)
	_, err = Run(context.Background(), session, bound, bindOptions(session, gocql.Options{HostID: cql.Option[cql.UUID]{Value: host.ID, Set: true}}))
	require.ErrorIs(t, err, gocql.ErrNoConnections)
}

func TestQuery_WithNowInSeconds(t *testing.T) {
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
}

func TestQuery_SetKeyspace(t *testing.T) {
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
}

// TestLargeSizeQuery runs a query bigger than the max allowed size of the payload of a frame,
// so it should be sent as 2 different frames where each contains a self-contained bit set to zero.
func TestLargeSizeQuery(t *testing.T) {
	session := createV2Session(t)
	defer session.Close()
	if implCluster.SessionData(session).InternalCfg.ProtoVersion < 5 {
		t.Skip("segmented frames are only produced on protocol >= 5")
	}
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,text_col text)", table))
	text := strings.Repeat("a", 500000)
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,text_col) VALUES(?,?)", table), cql.IntValue(1), codecTestMust(cql.TextValue(text)))
	require.Equal(t, text, cassandraText(t, cassandraRead(t, session, "SELECT text_col FROM "+table), "text_col"))
}

// TestCompressorNegotiated pins the compressor the suite was asked for to the one the
// connections actually ended up with.
//
// startupCoordinator.startup silently clears driverConn.compressor when the server's SUPPORTED
// response does not list Compressor.Name(), so a lane configured with -compressor=lz4
// against a server that does not offer lz4 would run entirely uncompressed while
// session.cfg.Compressor stayed non-nil. Every compression-dependent test guards on the
// connection and would simply skip, leaving the lane green and empty. Fail here instead,
// with the mismatch named.
//
// Every host is checked, not just the one session.InternalGetConn happens to pick: the compressor
// is negotiated per connection from that node's own SUPPORTED response, so on a cluster
// where one node does not advertise the compressor a single-connection check passes while
// queries routed to that node run uncompressed. One connection per host is the right
// granularity, since the SUPPORTED response is a property of the node.
//
// waitForHostConns is what makes "every host" true. Walking the hosts directly would skip
// the pools still filling, so on a three-node cluster this would usually inspect one node
// and pass -- leaving exactly the degraded node it exists to catch invisible.
//
// Deliberately not parallel. A parallel test resumes only once the sequential tests at its
// level have started, which would land this diagnosis at the tail of a ~470s run, after
// every test it explains has already skipped.
func TestCompressorNegotiated(t *testing.T) {
	if *flagCompressTest == "" || *flagCompressTest == "no-compression" {
		t.Skip("no compressor requested")
	}

	session := createV2Session(t)
	defer session.Close()

	conns := waitForHostConns(t, session)
	t.Logf("checked the negotiated compressor on %d host(s)", len(conns))

	for addr, conn := range conns {
		switch {
		case conn.InternalCompressor == nil:
			t.Errorf("%s: requested -compressor=%s but the connection negotiated none; the server's SUPPORTED response did not offer it",
				addr, *flagCompressTest)
		case conn.InternalCompressor.Name() != *flagCompressTest:
			t.Errorf("%s: requested -compressor=%s but the connection negotiated %q",
				addr, *flagCompressTest, conn.InternalCompressor.Name())
		}
	}
}

// TestQueryCompressionNotWorthIt runs a query that is not likely to be compressed efficiently
// (uncompressed payload size > compressed payload size).
// So, it should send a Compressed Frame where:
//  1. Compressed length is set to the length of the uncompressed payload;
//  2. Uncompressed length is set to zero;
//  3. Payload is the uncompressed payload.
func TestQueryCompressionNotWorthIt(t *testing.T) {
	session := createV2Session(t)
	defer session.Close()
	if implCluster.SessionData(session).InternalCfg.ProtoVersion < 5 {
		t.Skip("compressed segments are only produced on protocol >= 5")
	}
	if getRandomConn(t, session).InternalCompressor == nil {
		t.Skip("no compressor negotiated on the connection; the compressed-segment path is unreachable")
	}
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,text_col text)", table))
	text := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!@#$%^&*()_+"
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,text_col) VALUES(?,?)", table), cql.IntValue(1), codecTestMust(cql.TextValue(text)))
	require.Equal(t, text, cassandraText(t, cassandraRead(t, session, fmt.Sprintf("SELECT text_col FROM %s WHERE id=?", table), cql.IntValue(1)), "text_col"))
}

// TestPrepareExecuteMetadataChangedFlag covers native v5 result metadata IDs
// and the negotiated Scylla v4 extension with the same exact-connection flow.
// The protocol gate selects one path per lane; advertised-but-unnegotiated
// extension support is a failure, not a capability skip.
func TestPrepareExecuteMetadataChangedFlag(t *testing.T) {
	for _, tc := range []struct {
		name  string
		table string
		// gate returns a reason to skip, or "" to run. It may also fail outright, for
		// a state that must not be allowed to pass as a skip.
		gate func(t *testing.T, session *gocql.Session, conn *driverConn) string
	}{
		{
			name:  "native protocol v5",
			table: "metadata_changed",
			gate: func(t *testing.T, session *gocql.Session, conn *driverConn) string {
				if implCluster.SessionData(session).InternalCfg.ProtoVersion < protoVersion5 {
					return "Metadata_changed mechanism is only available in proto > 4"
				}
				if *flagDistribution == "scylla" && flagCassVersion.Before(2025, 3, 0) {
					return "ScyllaDB before 2025.3 does not exchange result metadata ids"
				}
				return ""
			},
		},
		{
			name:  "protocol v4 with SCYLLA_USE_METADATA_ID",
			table: "scylla_metadata_changed",
			gate: func(t *testing.T, session *gocql.Session, conn *driverConn) string {
				// The extension backports the v5 result metadata id to v4 and is
				// negotiated there only, so a run pinned to another protocol version
				// says nothing about it either way.
				if conn.InternalVersion&protoVersionMask != protoVersion4 {
					return "SCYLLA_USE_METADATA_ID is negotiated on protocol v4 only"
				}
				// Skip only for a server that cannot do this at all. If the server
				// advertised SCYLLA_USE_METADATA_ID and the driver still failed to
				// negotiate it, that is a regression — and since this is the only
				// end-to-end coverage of the extension, skipping would turn that
				// regression green. Fail instead.
				if !conn.InternalScyllaSupported.IsMetadataIDSupported() {
					return "server does not advertise SCYLLA_USE_METADATA_ID"
				}
				if !conn.InternalUsesMetadataID() {
					t.Fatal("server advertises SCYLLA_USE_METADATA_ID but the driver did not negotiate it")
				}
				return ""
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session := createV2Session(t, func(c *gocql.Config) {
				c.Metadata.SchemaEvents = false
				c.Metadata.AutomaticAgreement = false
				c.Metadata.RefreshAfterAgreement = false
			})
			defer session.Close()

			// We have to specify conn for all queries to ensure that
			// all queries are running on the same node
			conn := getRandomConn(t, session)
			if reason := tc.gate(t, session, conn); reason != "" {
				t.Skip(reason)
			}

			runMetadataChangedFlow(t, session, conn, testTableName(t, tc.table))
		})
	}
}

// runMetadataChangedFlow drives the METADATA_CHANGED flow against
// gocql_test.<table>, with every statement pinned to conn so they all land on the
// node whose prepared-statement cache entry is being inspected.
func runMetadataChangedFlow(t *testing.T, session *gocql.Session, conn *driverConn, table string) {
	t.Helper()
	qualified := (fixtureKeyspace("") + ".") + table
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	host := cql.Option[cql.UUID]{Value: implEndpoint.InternalHostInfoHostUUID(conn.InternalHost), Set: true}
	options := *func() *gocql.Options {
		opaqueStorageValuexxxx := new(gocql.Options)
		opaqueStorageValuexxxx.HostID = host
		implExecution.OptionsData(opaqueStorageValuexxxx).InternalConnection = conn
		return opaqueStorageValuexxxx
	}()
	prepareOptions := *func() *gocql.PrepareOptions {
		opaqueStorageValuexxxxx := new(gocql.PrepareOptions)
		opaqueStorageValuexxxxx.HostID = host
		implExecution.PrepareOptionsData(opaqueStorageValuexxxxx).InternalConnection = conn
		return opaqueStorageValuexxxxx
	}()
	create := cassandraUnprepared(t, "CREATE TABLE "+qualified+"(id int PRIMARY KEY)", cql.EmptyRowDecoder)
	_, err := Exec(ctx, session, create, bindOptions(session, options))
	require.NoError(t, err)
	require.NoError(t, implCluster.InternalSessionAwaitV2SchemaAgreementOn(session, ctx, conn))
	write := cassandraWriteBound(t, session, "INSERT INTO "+qualified+"(id) VALUES(?)", []cql.Value{cql.IntValue(1)}, prepareOptions)
	_, err = Exec(ctx, session, write, bindOptions(session, options))
	require.NoError(t, err)
	prepared := cassandraPrepared(t, session, cql.Statement[cql.EmptyParams, cql.DynamicRow]{CQL: "SELECT * FROM " + qualified, Params: cql.EmptyParamsEncoder, Rows: cql.DynamicRows}, prepareOptions)
	bound := cassandraBound(t, prepared, cql.EmptyParams{})
	row := cassandraReadBound(t, session, bound, options)
	require.Equal(t, 1, row.Columns().Len())
	require.Equal(t, int64(1), cassandraInt(t, row, "id"))
	cached := func() hostPrepared {
		t.Helper()
		implExecution.PreparedData(prepared).InternalState.InternalMu.RLock()
		defer implExecution.PreparedData(prepared).InternalState.InternalMu.RUnlock()
		entry, ok := implExecution.PreparedData(prepared).InternalState.InternalHosts[host.Value]
		require.True(t, ok)
		v := *entry
		v.InternalId = append([]byte(nil), v.InternalId...)
		v.InternalMetadataID = append([]byte(nil), v.InternalMetadataID...)
		return v
	}
	before := cached()
	require.NotEmpty(t, before.InternalMetadataID)
	alter := cassandraUnprepared(t, "ALTER TABLE "+qualified+" ADD new_col int", cql.EmptyRowDecoder)
	_, err = Exec(ctx, session, alter, bindOptions(session, options))
	require.NoError(t, err)
	// Include the exact host that applied ALTER; checking another host can
	// falsely agree on the old version before schema propagation starts.
	require.NoError(t, implCluster.InternalSessionAwaitV2SchemaAgreementOn(session, ctx, conn))
	write = cassandraWriteBound(t, session, "INSERT INTO "+qualified+"(id,new_col) VALUES(?,?)", []cql.Value{cql.IntValue(2), cql.IntValue(10)}, prepareOptions)
	_, err = Exec(ctx, session, write, bindOptions(session, options))
	require.NoError(t, err)
	checkRows := func() {
		t.Helper()
		rows, err := Run(ctx, session, bound, bindOptions(session, options))
		require.NoError(t, err)
		defer rows.Close()
		found := map[int64]cql.Value{}
		var row cql.DynamicRow
		for rows.NextInto(&row) {
			require.Equal(t, 2, row.Columns().Len())
			found[cassandraInt(t, row, "id")] = cassandraField(t, row, "new_col").Clone()
		}
		require.NoError(t, rows.Close())
		require.Len(t, found, 2)
		require.Equal(t, cql.Null, found[1].State())
		value, err := found[2].Signed()
		require.NoError(t, err)
		require.Equal(t, int64(10), value)
	}
	require.Equal(t, before.InternalGeneration, implCluster.InternalSessionTypedGeneration(session), "metadata-ID coverage must not be replaced by generation-triggered reprepare")
	checkRows()
	after := cached()
	require.NotEqual(t, before.InternalMetadataID, after.InternalMetadataID)
	require.False(t, before.InternalColumns.Equal(after.InternalColumns))
	// Servers may invalidate the statement on ALTER and force an UNPREPARED
	// repair here. Both paths must update the metadata ID and columns; the
	// explicit stale-ID execution below must consume METADATA_CHANGED.
	// Reintroduce exactly the old result ID and column layout. Preserve the
	// revision ordering so the next change must be observable as a new revision.
	stale := before
	stale.InternalRevision = after.InternalRevision
	implExecution.PreparedData(prepared).InternalState.InternalMu.Lock()
	implExecution.PreparedData(prepared).InternalState.InternalHosts[host.Value] = &stale
	implExecution.PreparedData(prepared).InternalState.InternalMu.Unlock()
	checkRows()
	healed := cached()
	require.True(t, healed.InternalMetadataChanged)
	require.NotEqual(t, before.InternalMetadataID, healed.InternalMetadataID)
	require.Equal(t, after.InternalMetadataID, healed.InternalMetadataID)
	require.True(t, after.InternalColumns.Equal(healed.InternalColumns))
	require.Greater(t, healed.InternalRevision, after.InternalRevision)
	checkRows()
	unchanged := cached()
	require.Equal(t, healed.InternalRevision, unchanged.InternalRevision, "matching result metadata must not replace cached metadata")
	require.Equal(t, healed.InternalMetadataID, unchanged.InternalMetadataID)
}

func TestStmtCacheUsesOverriddenKeyspace(t *testing.T) {
	session := createV2Session(t)
	defer session.Close()
	if implCluster.SessionData(session).InternalCfg.ProtoVersion < 5 {
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
		bound := cassandraWriteBound(t, session, insert, []cql.Value{cql.IntValue(int32(i + 1))}, gocql.PrepareOptions{Keyspace: keyspace})
		cassandraBatch(t, session, cql.LoggedBatch, []batchItem{cassandraItem(t, bound)}, gocql.Options{Keyspace: keyspace})
	}
	for i, keyspace := range []string{fixtureKeyspace(""), ks} {
		prepared := cassandraPrepared(t, session, cql.Statement[cql.EmptyParams, int32]{CQL: "SELECT * FROM " + table, Params: cql.EmptyParamsEncoder, Rows: cql.OneColumn(cql.Int32)}, gocql.PrepareOptions{Keyspace: keyspace})
		require.Equal(t, keyspace, prepared.Info().Keyspace)
		require.Equal(t, int32(i+1), cassandraReadBound(t, session, cassandraBound(t, prepared, cql.EmptyParams{}), gocql.Options{Keyspace: keyspace}))
	}
}

func TestRoutingKeyCacheUsesOverriddenKeyspace(t *testing.T) {
	session := createV2Session(t)
	defer session.Close()
	if implCluster.SessionData(session).InternalCfg.ProtoVersion < 5 {
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
}

// TestPrepareExecuteScyllaEmptyMetadataID covers the rolling-upgrade case: a
// prepared statement cached before SCYLLA_USE_METADATA_ID was negotiated has no
// result metadata ID, and the prepared cache is keyed by host and survives
// reconnects, so it can still be executed over an extension-enabled connection.
//
// Two things have to hold. The driver must not ask the server to skip metadata for
// such a statement — it has no ID for the server to compare against, so a skipped
// response would leave it decoding against whatever it had cached. And Scylla must
// accept the empty ID as a mismatch rather than rejecting the frame, answering with
// METADATA_CHANGED and a fresh ID so the statement heals itself.
//
// The same scenario is covered in the sibling drivers: java-driver's
// PreparedStatementIT.should_handle_empty_metadata_id_when_executing_statement_when_supported
// (scylladb/java-driver#758) and python-driver's
// test_empty_sentinel_id_triggers_metadata_changed (scylladb/python-driver#770).
func TestPrepareExecuteScyllaEmptyMetadataID(t *testing.T) {
	session := createV2Session(t, func(c *gocql.Config) {
		c.Metadata.SchemaEvents = false
		c.Metadata.AutomaticAgreement = false
		c.Metadata.RefreshAfterAgreement = false
	})
	defer session.Close()
	conn := getRandomConn(t, session)
	if conn.InternalVersion&protoVersionMask != protoVersion4 {
		t.Skip("SCYLLA_USE_METADATA_ID is negotiated on protocol v4 only")
	}
	if !conn.InternalScyllaSupported.IsMetadataIDSupported() {
		t.Skip("server does not advertise SCYLLA_USE_METADATA_ID")
	}
	require.True(t, conn.InternalUsesMetadataID(), "server advertises metadata IDs but driver did not negotiate them")
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,val int)", table))
	cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,val) VALUES(?,?)", table), cql.IntValue(1), cql.IntValue(7))
	host := cql.Option[cql.UUID]{Value: implEndpoint.InternalHostInfoHostUUID(conn.InternalHost), Set: true}
	prepared := cassandraPrepared(t, session, cql.Statement[int32, cql.DynamicRow]{CQL: fmt.Sprintf("SELECT id,val FROM %s WHERE id=?", table), Params: cql.OneParam(cql.Int32), Rows: cql.DynamicRows}, *func() *gocql.PrepareOptions {
		opaqueStorageValuexxxxxx := new(gocql.PrepareOptions)
		opaqueStorageValuexxxxxx.HostID = host
		implExecution.PrepareOptionsData(opaqueStorageValuexxxxxx).InternalConnection = conn
		return opaqueStorageValuexxxxxx
	}())
	bound := cassandraBound(t, prepared, int32(1))
	cassandraReadBound(t, session, bound, *func() *gocql.Options {
		opaqueStorageValuexxxxxxx := new(gocql.Options)
		opaqueStorageValuexxxxxxx.HostID = host
		implExecution.OptionsData(opaqueStorageValuexxxxxxx).InternalConnection = conn
		return opaqueStorageValuexxxxxxx
	}())
	implExecution.PreparedData(prepared).InternalState.InternalMu.Lock()
	entry, ok := implExecution.PreparedData(prepared).InternalState.InternalHosts[host.Value]
	require.True(t, ok)
	cached := *entry
	require.NotEmpty(t, cached.InternalMetadataID)
	cached.InternalMetadataID = nil
	cached.InternalRevision++
	implExecution.PreparedData(prepared).InternalState.InternalHosts[host.Value] = &cached
	implExecution.PreparedData(prepared).InternalState.InternalMu.Unlock()
	row := cassandraReadBound(t, session, bound, *func() *gocql.Options {
		opaqueStorageValuexxxxxxxx := new(gocql.Options)
		opaqueStorageValuexxxxxxxx.HostID = host
		implExecution.OptionsData(opaqueStorageValuexxxxxxxx).InternalConnection = conn
		return opaqueStorageValuexxxxxxxx
	}())
	require.Equal(t, int64(1), cassandraInt(t, row, "id"))
	require.Equal(t, int64(7), cassandraInt(t, row, "val"))
	implExecution.PreparedData(prepared).InternalState.InternalMu.RLock()
	healed := implExecution.PreparedData(prepared).InternalState.InternalHosts[host.Value]
	implExecution.PreparedData(prepared).InternalState.InternalMu.RUnlock()
	require.NotEmpty(t, healed.InternalMetadataID, "empty metadata ID must be repaired from response")
	require.True(t, healed.InternalMetadataChanged)
	require.Greater(t, healed.InternalRevision, cached.InternalRevision)
}
