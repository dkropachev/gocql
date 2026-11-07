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
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
)

func TestPaging(t *testing.T) {
	t.Run("AllowFiltering", func(t *testing.T) {
		t.Parallel()
		// The reads below run at ONE and QUORUM. Writes at the default QUORUM
		// leave a replica that a ONE read can reach before it has every row
		// (W+R must exceed RF), so the fixture is written at ALL.
		session := createV2Session(t, func(c *gocql.Config) {
			profile := *c.DefaultProfile
			profile.Consistency = cql.All
			c.DefaultProfile = &profile
		})
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
	})

	t.Run("Auto", func(t *testing.T) {
		t.Parallel()

		t.Run("AllRows", func(t *testing.T) {
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
		})
		t.Run("Bound", func(t *testing.T) {
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
		})
	})

	t.Run("Manual", func(t *testing.T) {
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
	})

	t.Run("PreparedSchemaChange", func(t *testing.T) {
		t.Parallel()
		session := createV2Session(t)
		defer session.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		table := fmt.Sprintf("prepared_rows_%x", time.Now().UnixNano())
		other := table + "_ddl"
		if err := v2CreateTable(ctx, session, "CREATE TABLE "+table+"(id int,val int,payload blob,PRIMARY KEY(id,val))"); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = v2CreateTable(context.Background(), session, "DROP TABLE IF EXISTS "+other)
			_ = v2CreateTable(context.Background(), session, "DROP TABLE IF EXISTS "+table)
		}()
		payload := bytes.Repeat([]byte("live-borrowed"), 512)
		for i := int32(0); i < 64; i++ {
			cassandraWrite(t, session, "INSERT INTO "+table+"(id,val,payload) VALUES(?,?,?)", cql.IntValue(1), cql.IntValue(i), cql.BlobValue(payload))
		}
		var calls atomic.Int32
		decoder := liveDecoder{calls: &calls, base: cql.Row(cql.Column("val", cql.Int32, func(r *liveDestination) *int32 { return &r.Value }), cql.Column("payload", cql.BlobCodec, func(r *liveDestination) *cql.Bytes { return &r.Payload }))}
		prepared := codecTestMust(gocql.Prepare(ctx, session, cql.Statement[int32, liveDestination]{CQL: "SELECT val,payload FROM " + table + " WHERE id=?", Params: cql.OneParam(cql.Int32), Rows: decoder}, gocql.PrepareOptions{}))
		bound := codecTestMust(BindBorrowed(prepared, int32(1)))
		defer bound.Close()
		churn := make(chan error, 1)
		go func() {
			for i := 0; i < 8; i++ {
				if err := v2CreateTable(ctx, session, "CREATE TABLE "+other+"(id int PRIMARY KEY)"); err != nil {
					churn <- err
					return
				}
				if err := v2CreateTable(ctx, session, "DROP TABLE "+other); err != nil {
					churn <- err
					return
				}
			}
			churn <- nil
		}()
		rows := codecTestMust(Run(ctx, session, bound, bindOptions(session, gocql.Options{PageSize: cql.Option[int32]{Value: 3, Set: true}})))
		defer rows.Close()
		var dst liveDestination
		var retained *gocql.PageLease
		var first cql.Bytes
		count := 0
		for rows.NextInto(&dst) {
			if dst.Value != int32(count) || !bytes.Equal(dst.Payload.OwnedBytes(), payload) {
				t.Fatal("row mismatch", count, dst.Value)
			}
			if count == 0 {
				retained = codecTestMust(rows.Retain())
				defer retained.Close()
				first = dst.Payload
			}
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if count != 64 || dst.Value != 0 || dst.Payload.Len() != 0 {
			t.Fatal("count/final reset", count, dst)
		}
		if rows.NextInto(nil) || rows.Err() != nil {
			t.Fatal("exhaustion replaced by nil error", rows.Err())
		}
		if calls.Load() != 1 {
			t.Fatal("steady pages recompiled", calls.Load())
		}
		if err := <-churn; err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(first.OwnedBytes(), payload) {
			t.Fatal("retained output lost during DDL")
		}
		bound.Close()
		if err := bound.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Stream", func(t *testing.T) {
		config := createV2Cluster(func(c *gocql.Config) {
			c.Pool.ConnectionsPerHost = 1
			c.KeepaliveInterval = 0
			c.Metadata.PeriodicRefresh = 0
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		s, err := gocql.Connect(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		keyspace := testKeyspaceName(t, fmt.Sprintf("%x", time.Now().UnixNano()))
		exec := func(stmt string) {
			t.Helper()
			b := codecTestMust(BindUnpreparedOwned(cql.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: stmt, Kind: cql.OrdinaryStatement, Params: cql.EmptyParamsEncoder, Rows: cql.EmptyRowDecoder}, cql.NewColumns(), cql.EmptyParams{}))
			defer b.Close()
			if _, err := Exec(ctx, s, b, gocql.SessionOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		exec("CREATE KEYSPACE " + keyspace + " WITH replication={'class':'NetworkTopologyStrategy','replication_factor':1}")
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			b, err := BindUnpreparedOwned(cql.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: "DROP KEYSPACE " + keyspace, Kind: cql.OrdinaryStatement, Params: cql.EmptyParamsEncoder, Rows: cql.EmptyRowDecoder}, cql.NewColumns(), cql.EmptyParams{})
			if err == nil {
				defer b.Close()
				_, err = Exec(cleanup, s, b, gocql.SessionOptions{})
			}
			if err != nil {
				t.Error("unique keyspace cleanup", err)
			}
		}()
		exec("CREATE TABLE " + keyspace + ".records(id int PRIMARY KEY,value blob,pieces list<blob>,txt text)")
		type input struct {
			ID     int32
			Value  []byte
			Pieces [][]byte
			Text   string
		}
		params := cql.Params(cql.Param("id", cql.Int32, func(v input) int32 { return v.ID }), cql.Param("value", cql.BytesEncoder, func(v input) []byte { return v.Value }), cql.Param("pieces", cql.ListEncoder(cql.BytesEncoder), func(v input) [][]byte { return v.Pieces }), cql.Param("txt", cql.StringEncoder, func(v input) string { return v.Text }))
		prepared := codecTestMust(gocql.Prepare(ctx, s, cql.Statement[input, cql.EmptyRow]{CQL: "INSERT INTO " + keyspace + ".records(id,value,pieces,txt)VALUES(?,?,?,?)", Kind: cql.OrdinaryStatement, Params: params, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{}))
		payload := bytes.Repeat([]byte("segment-crossing"), 30000)
		textPayload := strings.Repeat("stream-🙂-", 24000)
		for i := 0; i < 5; i++ {
			b := codecTestMust(BindOwned(prepared, input{int32(i), payload, [][]byte{[]byte("nested"), []byte("storage")}, textPayload}))
			_, err := Exec(ctx, s, b, gocql.SessionOptions{})
			b.Close()
			if err != nil {
				t.Fatal(err)
			}
			if err := b.Wait(ctx); err != nil {
				t.Fatal(err)
			}
		}
		statement := cql.Statement[cql.EmptyParams, cql.DynamicRow]{CQL: "SELECT id,value,pieces,txt FROM " + keyspace + ".records", Kind: cql.OrdinaryStatement, Params: cql.EmptyParamsEncoder, Rows: cql.DynamicRows}
		selectPrepared := codecTestMust(gocql.Prepare(ctx, s, statement, gocql.PrepareOptions{}))
		bound := codecTestMust(BindOwned(selectPrepared, cql.EmptyParams{}))
		defer bound.Close()
		type output struct {
			ID     int32
			Value  []byte
			Pieces [][]byte
			Text   []byte
		}
		decoder := cql.StreamRow(func(*output) {}, cql.StreamColumn("id", cql.StreamInt32, func(r *output) *int32 { return &r.ID }), cql.StreamColumn("value", cql.StreamBlob, func(r *output) *[]byte { return &r.Value }), cql.StreamColumn("pieces", cql.StreamList(cql.StreamBlob), func(r *output) *[][]byte { return &r.Pieces }), cql.StreamColumn("txt", cql.StreamText, func(r *output) *[]byte { return &r.Text }))
		options := gocql.StreamOptions{Incremental: true, Options: gocql.Options{PageSize: cql.Option[int32]{Set: true, Value: 2}}}
		if config.Protocol < 5 && config.Compression != gocql.NoCompression {
			_, err := RunStream(ctx, s, bound, decoder, options)
			var capability *cql.StreamCapabilityError
			if !errors.As(err, &capability) {
				t.Fatal("frame compression pre-submit capability", err)
			}
			t.Log("frame-compressed streaming explicitly rejected before query")
			return
		}
		stream, err := RunStream(ctx, s, bound, decoder, options)
		if err != nil {
			t.Fatal(err)
		}
		dst := output{Value: make([]byte, 0, len(payload)), Text: make([]byte, 0, len(textPayload)), Pieces: [][]byte{make([]byte, 0, 16), make([]byte, 0, 16)}}
		valueBacking := &dst.Value[:cap(dst.Value)][0]
		nestedBacking := &dst.Pieces[0][:cap(dst.Pieces[0])][0]
		seen := map[int32]bool{}
		for stream.NextInto(&dst) {
			if seen[dst.ID] || !bytes.Equal(dst.Value, payload) || string(dst.Text) != textPayload || len(dst.Pieces) != 2 || string(dst.Pieces[0]) != "nested" || string(dst.Pieces[1]) != "storage" {
				t.Fatal("streamed live output", dst.ID, len(dst.Value), dst.Pieces)
			}
			if &dst.Value[0] != valueBacking || &dst.Pieces[0][0] != nestedBacking {
				t.Fatal("caller capacity replaced")
			}
			seen[dst.ID] = true
		}
		if err := stream.Err(); err != nil {
			t.Fatal(err)
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		info := stream.Info()
		if len(seen) != 5 || info.DeliveredRows != 5 || info.Pages < 3 || info.Result.Attempts < 3 || !info.OutputStarted {
			t.Fatal("incomplete live stream", len(seen), info)
		}
		if !bytes.Equal(dst.Value, payload) {
			t.Fatal("output lost after close")
		}
		// A separate caller destination is never borrowed past early Close. Ordinary
		// configured pool repair must restore subsequent execution within its budget.
		early, err := RunStream(ctx, s, bound, decoder, options)
		if err != nil {
			t.Fatal(err)
		}
		if !early.NextInto(&dst) {
			t.Fatal(early.Err())
		}
		if err := early.Close(); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(20 * time.Second)
		for {
			ready := true
			for _, host := range implCluster.SessionData(s).InternalHostSource.InternalGetHostsList() {
				pool, ok := implCluster.SessionData(s).InternalPool.InternalGetPool(host)
				if !ok {
					ready = false
					break
				}
				pool.InternalMu.RLock()
				count, missing := pool.InternalConnPicker.Size()
				pool.InternalMu.RUnlock()
				if count != 1 || missing != 0 {
					ready = false
					break
				}
			}
			if ready {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("bounded pool repair failed")
			}
			time.Sleep(10 * time.Millisecond)
		}
		rows, err := Run(ctx, s, bound, bindOptions(s, gocql.Options{Prefetch: cql.Option[int]{Set: true, Value: 0}}))
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		var row cql.DynamicRow
		for rows.NextInto(&row) {
			count++
		}
		if err := rows.Close(); err != nil || count != 5 {
			t.Fatal("shared execution after stream repair", count, err)
		}
		emptyBound := codecTestMust(BindUnpreparedOwned(cql.Statement[cql.EmptyParams, cql.DynamicRow]{CQL: "SELECT id,value,pieces,txt FROM " + keyspace + ".records WHERE id=-1", Kind: cql.OrdinaryStatement, Params: cql.EmptyParamsEncoder, Rows: cql.DynamicRows}, cql.NewColumns(), cql.EmptyParams{}))
		empty, err := RunStream(ctx, s, emptyBound, decoder, options)
		if err != nil {
			t.Fatal(err)
		}
		if empty.NextInto(&dst) || empty.Err() != nil || empty.Info().OutputStarted {
			t.Fatal("empty result", empty.Err(), empty.Info())
		}
		empty.Close()
		emptyBound.Close()

		// Change a SELECT * result while its pull-driven continuation is live. The
		// existing binding must select a new streaming plan from real server metadata.
		changedPrepared := codecTestMust(gocql.Prepare(ctx, s, cql.Statement[cql.EmptyParams, cql.DynamicRow]{CQL: "SELECT * FROM " + keyspace + ".records", Kind: cql.OrdinaryStatement, Params: cql.EmptyParamsEncoder, Rows: cql.DynamicRows}, gocql.PrepareOptions{}))
		changedBound := codecTestMust(BindOwned(changedPrepared, cql.EmptyParams{}))
		defer changedBound.Close()
		changed, err := RunStream(ctx, s, changedBound, decoder, gocql.StreamOptions{Incremental: true, Options: gocql.Options{PageSize: cql.Option[int32]{Set: true, Value: 1}}})
		if err != nil {
			t.Fatal(err)
		}
		defer changed.Close()
		if !changed.NextInto(&dst) {
			t.Fatal("first metadata page", changed.Err())
		}
		if changed.Columns().Len() != 4 {
			t.Fatal("original metadata shape", changed.Columns().Len())
		}
		changedSeen := map[int32]bool{dst.ID: true}
		// Keep this cursor idle while exhausting its exact host reservation. A
		// separate native control connection must still dispatch real schema events.
		sub, err := s.Subscribe(gocql.EventFilter{Kinds: []gocql.EventKind{gocql.SchemaEvent}, Keyspace: keyspace, Capacity: 2})
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
		blockedCtx, blockedCancel := context.WithTimeout(ctx, 100*time.Millisecond)
		second, blockedErr := RunStream(blockedCtx, s, bound, decoder, gocql.StreamOptions{Incremental: true, Options: gocql.Options{HostID: cql.Option[cql.UUID]{Set: true, Value: changed.Info().Result.HostID}}})
		blockedCancel()
		if second != nil {
			second.Close()
			t.Fatal("reserved host opened extra streaming capacity")
		}
		if !errors.Is(blockedErr, context.DeadlineExceeded) {
			t.Fatal("pool exhaustion was not bounded", blockedErr)
		}
		// Use another real client: this stream deliberately owns its one configured
		// connection slot across pages, including during caller think time.
		ddlSession, err := gocql.Connect(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		alterErr := v2CreateTable(ctx, ddlSession, "ALTER TABLE "+keyspace+".records ADD revision int")
		ddlSession.Close()
		if alterErr != nil {
			t.Fatal(alterErr)
		}
		select {
		case event, ok := <-sub.Events():
			if !ok || event.Kind != gocql.SchemaEvent {
				t.Fatal("control event lost while stream idle", event, ok)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("control dispatch blocked behind idle/exhausted stream reservation")
		}
		t.Log("actual schema event dispatched while cursor idle and its host reservation exhausted; subscriber capacity=2")
		for changed.NextInto(&dst) {
			if changedSeen[dst.ID] || !bytes.Equal(dst.Value, payload) || string(dst.Text) != textPayload || len(dst.Pieces) != 2 || string(dst.Pieces[0]) != "nested" || string(dst.Pieces[1]) != "storage" {
				t.Fatal("metadata changed output", dst.ID)
			}
			if &dst.Value[0] != valueBacking || &dst.Pieces[0][0] != nestedBacking {
				t.Fatal("metadata change replaced caller storage")
			}
			changedSeen[dst.ID] = true
		}
		if err := changed.Close(); err != nil {
			t.Fatal("metadata changed stream", err)
		}
		if len(changedSeen) != 5 || changed.Columns().Len() != 5 {
			t.Fatal("changed metadata not observed", len(changedSeen), changed.Columns().Len())
		}
		t.Logf("live metadata change: 4 -> %d physical columns, %d delivered rows", changed.Columns().Len(), len(changedSeen))

		// TODO(scylladb/gocql#339): cover a coordinator interruption once the CCM
		// package can stop and start a node of a test-owned cluster. With it, this
		// step should:
		//   - open an incremental stream pinned to node1 (HostID) with page size 1
		//     and read its first row;
		//   - stop node1 gracefully and wait for it to exit (ccm node1 stop);
		//   - check the stream fails terminally: NextInto stays false, Err and Close
		//     return the same error, and DeliveredRows stays 1 (no replay);
		//   - start node1 and wait for its binary protocol
		//     (ccm node1 start --wait-for-binary-proto), restarting it even when an
		//     assertion fails;
		//   - probe that queries pinned to node1 return all 5 rows within 40 s,
		//     proving the shared pool recovered.
		// It must not stop a node of the shared integration cluster, which other
		// tests use concurrently. The steps below ran through the node control
		// socket of tests/fixtures/native.py, which is gone: nodeCommand(t, "stop")
		// and nodeCommand(t, "start", "--wait-for-binary-proto") stand for the CCM
		// package's node stop and start, and GOCQL_FIXTURE_NODE for node1's address.
		// Re-enabling them needs the os and internal/endpoint imports back.
		//
		// // Stop the actual selected coordinator after a delivered row and before the
		// // next page request. This is server lifecycle interruption, not a mocked EOF.
		// if os.Getenv("GOCQL_FIXTURE_CONTROL") == "" {
		// 	t.Fatal("owned node control required for live interruption coverage")
		// }
		// var target cql.UUID
		// for _, host := range implCluster.SessionData(s).InternalHostSource.InternalGetHostsList() {
		// 	if host.ConnectAddress().String() == os.Getenv("GOCQL_FIXTURE_NODE") {
		// 		target = implEndpoint.InternalHostInfoHostUUID(host)
		// 	}
		// }
		// if target.IsEmpty() {
		// 	t.Fatal("owned interruption coordinator missing")
		// }
		// interrupted, err := RunStream(ctx, s, bound, decoder, gocql.StreamOptions{Incremental: true, Options: gocql.Options{HostID: cql.Option[cql.UUID]{Set: true, Value: target}, PageSize: cql.Option[int32]{Set: true, Value: 1}}})
		// if err != nil {
		// 	t.Fatal(err)
		// }
		// defer interrupted.Close()
		// if !interrupted.NextInto(&dst) {
		// 	t.Fatal("interruption first row", interrupted.Err())
		// }
		// nodeCommand(t, "stop")
		// // Always restart the exact stopped node, even if an assertion below fails.
		// restarted := false
		// defer func() {
		// 	if !restarted {
		// 		nodeCommand(t, "start", "--wait-for-binary-proto")
		// 	}
		// }()
		// if interrupted.NextInto(&dst) || interrupted.Err() == nil {
		// 	t.Fatal("server interruption was not terminal", interrupted.Err())
		// }
		// terminal := interrupted.Err()
		// if interrupted.Info().DeliveredRows != 1 || !interrupted.Info().OutputStarted {
		// 	t.Fatal("interruption replayed output", interrupted.Info())
		// }
		// if interrupted.NextInto(&dst) || !errors.Is(interrupted.Err(), terminal) {
		// 	t.Fatal("terminal interruption changed")
		// }
		// if err := interrupted.Close(); !errors.Is(err, terminal) {
		// 	t.Fatal("close lost interruption", err)
		// }
		// nodeCommand(t, "start", "--wait-for-binary-proto")
		// restarted = true
		// repairDeadline := time.Now().Add(40 * time.Second)
		// for {
		// 	probe, probeErr := Run(ctx, s, bound, bindOptions(s, gocql.Options{HostID: cql.Option[cql.UUID]{Set: true, Value: target}, Prefetch: cql.Option[int]{Set: true, Value: 0}}))
		// 	if probeErr == nil {
		// 		n := 0
		// 		var probeRow cql.DynamicRow
		// 		for probe.NextInto(&probeRow) {
		// 			n++
		// 		}
		// 		probeErr = probe.Close()
		// 		if probeErr == nil && n == 5 {
		// 			break
		// 		}
		// 	}
		// 	if time.Now().After(repairDeadline) {
		// 		t.Fatal("coordinator pool did not recover", probeErr)
		// 	}
		// 	time.Sleep(20 * time.Millisecond)
		// }
		// t.Logf("actual coordinator stop/join after one delivered row: %v; restart and shared pool probe succeeded", terminal)
		bound.Close()
		if err := bound.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		t.Logf("public RunStream protocol=%d compression=%d tls=%v rows=%d pages=%d bytes/row=%d attempts=%d", config.Protocol, config.Compression, *flagRunSslTest, info.DeliveredRows, info.Pages, len(payload), info.Result.Attempts)
	})
}
