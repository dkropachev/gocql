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

	"context"
	"fmt"
	"testing"
	"time"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

	"github.com/stretchr/testify/require"
)

type FullName struct {
	FirstName string
	LastName  string
}

type ClusteredKeyValue struct {
	Id      int32
	Cluster int32
	Value   string
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

		for _, host := range implExecution.InternalSessionData((*implExecution.Session)(session)).InternalHostSource.InternalGetHostsList() {
			// Collected rather than merely skipped: a host that stays down keeps the
			// count below *clusterSize while contributing nothing to pending, so the
			// failure below would otherwise report "2 of 3 connected, 0 still without
			// a connection: []" and name no address to go and look at.
			if !host.IsUp() {
				down = append(down, host.ConnectAddressAndPort())
				continue
			}

			addr := host.ConnectAddressAndPort()
			pool, found := implExecution.InternalSessionData((*implExecution.Session)(session)).InternalPool.InternalGetPool(host)
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
		conn = implExecution.InternalCore((*implExecution.Session)(session)).GetConn()
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
	prepared := cassandraPrepared(t, session, cql.Statement[string, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(foo,bar) VALUES(?,7)", table), Params: cql.OneParam(cql.StringEncoder), Rows: cql.EmptyRowDecoder}, *func() *implExecution.PrepareOptions {
		opaqueStorageValuex := new(implExecution.PrepareOptions)
		opaqueStorageValuex.HostID = cql.Option[cql.UUID]{Value: implEndpoint.InternalHostInfoHostUUID(conn.InternalHost), Set: true}
		implExecution.PrepareOptionsData(opaqueStorageValuex).InternalConnection = conn
		return opaqueStorageValuex
	}())
	preparedData(prepared).InternalState.InternalMu.Lock()
	host := preparedData(prepared).InternalState.InternalHosts[implEndpoint.InternalHostInfoHostUUID(conn.InternalHost)]
	host.InternalId = []byte("foobar")
	preparedData(prepared).InternalState.InternalHosts[implEndpoint.InternalHostInfoHostUUID(conn.InternalHost)] = host
	preparedData(prepared).InternalState.InternalMu.Unlock()
	return cassandraBound(t, prepared, "bar"), conn
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

// runMetadataChangedFlow drives the METADATA_CHANGED flow against
// gocql_test.<table>, with every statement pinned to conn so they all land on the
// node whose prepared-statement cache entry is being inspected.
func runMetadataChangedFlow(t *testing.T, session *gocql.Session, conn *driverConn, table string) {
	t.Helper()
	qualified := (fixtureKeyspace("") + ".") + table
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	host := cql.Option[cql.UUID]{Value: implEndpoint.InternalHostInfoHostUUID(conn.InternalHost), Set: true}
	options := *func() *implExecution.Options {
		opaqueStorageValuexxxx := new(implExecution.Options)
		opaqueStorageValuexxxx.HostID = host
		implExecution.OptionsData(opaqueStorageValuexxxx).InternalConnection = conn
		return opaqueStorageValuexxxx
	}()
	prepareOptions := *func() *implExecution.PrepareOptions {
		opaqueStorageValuexxxxx := new(implExecution.PrepareOptions)
		opaqueStorageValuexxxxx.HostID = host
		implExecution.PrepareOptionsData(opaqueStorageValuexxxxx).InternalConnection = conn
		return opaqueStorageValuexxxxx
	}()
	create := cassandraUnprepared(t, "CREATE TABLE "+qualified+"(id int PRIMARY KEY)", cql.EmptyRowDecoder)
	_, err := Exec(ctx, session, create, bindOptions(session, options))
	require.NoError(t, err)
	require.NoError(t, implExecution.InternalCore((*implExecution.Session)(session)).AwaitV2SchemaAgreementOn(ctx, conn))
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
		preparedData(prepared).InternalState.InternalMu.RLock()
		defer preparedData(prepared).InternalState.InternalMu.RUnlock()
		entry, ok := preparedData(prepared).InternalState.InternalHosts[host.Value]
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
	require.NoError(t, implExecution.InternalCore((*implExecution.Session)(session)).AwaitV2SchemaAgreementOn(ctx, conn))
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
	require.Equal(t, before.InternalGeneration, implExecution.InternalCore((*implExecution.Session)(session)).TypedGeneration(), "metadata-ID coverage must not be replaced by generation-triggered reprepare")
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
	preparedData(prepared).InternalState.InternalMu.Lock()
	preparedData(prepared).InternalState.InternalHosts[host.Value] = &stale
	preparedData(prepared).InternalState.InternalMu.Unlock()
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
