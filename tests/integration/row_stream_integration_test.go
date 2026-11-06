//go:build integration

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package integration

import (
	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"

	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
)

func TestRunStreamLive(t *testing.T) {
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
		b := codecTestMust(BindUnpreparedOwned(cql.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: stmt, Kind: cql.OrdinaryStatement, Params: cql.NoParams, Rows: cql.NoRows}, cql.NewColumns(), cql.EmptyParams{}))
		defer b.Close()
		if _, err := Exec(ctx, s, b, gocql.Options{}); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE KEYSPACE " + keyspace + " WITH replication={'class':'NetworkTopologyStrategy','replication_factor':1}")
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		b, err := BindUnpreparedOwned(cql.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: "DROP KEYSPACE " + keyspace, Kind: cql.OrdinaryStatement, Params: cql.NoParams, Rows: cql.NoRows}, cql.NewColumns(), cql.EmptyParams{})
		if err == nil {
			defer b.Close()
			_, err = Exec(cleanup, s, b, gocql.Options{})
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
	prepared := codecTestMust(gocql.Prepare(ctx, s, cql.Statement[input, cql.EmptyRow]{CQL: "INSERT INTO " + keyspace + ".records(id,value,pieces,txt)VALUES(?,?,?,?)", Kind: cql.OrdinaryStatement, Params: params, Rows: cql.NoRows}, gocql.PrepareOptions{}))
	payload := bytes.Repeat([]byte("segment-crossing"), 30000)
	textPayload := strings.Repeat("stream-🙂-", 24000)
	for i := 0; i < 5; i++ {
		b := codecTestMust(BindOwned(prepared, input{int32(i), payload, [][]byte{[]byte("nested"), []byte("storage")}, textPayload}))
		_, err := Exec(ctx, s, b, gocql.Options{})
		b.Close()
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	statement := cql.Statement[cql.EmptyParams, cql.DynamicRow]{CQL: "SELECT id,value,pieces,txt FROM " + keyspace + ".records", Kind: cql.OrdinaryStatement, Params: cql.NoParams, Rows: cql.DynamicRows}
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
	options := gocql.StreamOptions{Incremental: true, Options: testOptions(gocql.OptionValues{PageSize: cql.Option[int32]{Set: true, Value: 2}})}
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
	rows, err := Run(ctx, s, bound, testOptions(gocql.OptionValues{Prefetch: cql.Option[int]{Set: true, Value: 0}}))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Close(); err != nil || count != 5 {
		t.Fatal("shared execution after stream repair", count, err)
	}
	emptyBound := codecTestMust(BindUnpreparedOwned(cql.Statement[cql.EmptyParams, cql.DynamicRow]{CQL: "SELECT id,value,pieces,txt FROM " + keyspace + ".records WHERE id=-1", Kind: cql.OrdinaryStatement, Params: cql.NoParams, Rows: cql.DynamicRows}, cql.NewColumns(), cql.EmptyParams{}))
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
	changedPrepared := codecTestMust(gocql.Prepare(ctx, s, cql.Statement[cql.EmptyParams, cql.DynamicRow]{CQL: "SELECT * FROM " + keyspace + ".records", Kind: cql.OrdinaryStatement, Params: cql.NoParams, Rows: cql.DynamicRows}, gocql.PrepareOptions{}))
	changedBound := codecTestMust(BindOwned(changedPrepared, cql.EmptyParams{}))
	defer changedBound.Close()
	changed, err := RunStream(ctx, s, changedBound, decoder, gocql.StreamOptions{Incremental: true, Options: testOptions(gocql.OptionValues{PageSize: cql.Option[int32]{Set: true, Value: 1}})})
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
	second, blockedErr := RunStream(blockedCtx, s, bound, decoder, gocql.StreamOptions{Incremental: true, Options: testOptions(gocql.OptionValues{HostID: cql.Option[cql.UUID]{Set: true, Value: changed.Info().Result.HostID}})})
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

	// Stop the actual selected coordinator after a delivered row and before the
	// next page request. This is server lifecycle interruption, not a mocked EOF.
	if os.Getenv("GOCQL_FIXTURE_CONTROL") == "" {
		t.Fatal("owned node control required for live interruption coverage")
	}
	var target cql.UUID
	for _, host := range implCluster.SessionData(s).InternalHostSource.InternalGetHostsList() {
		if host.ConnectAddress().String() == os.Getenv("GOCQL_FIXTURE_NODE") {
			target = implEndpoint.InternalHostInfoHostUUID(host)
		}
	}
	if target.IsEmpty() {
		t.Fatal("owned interruption coordinator missing")
	}
	interrupted, err := RunStream(ctx, s, bound, decoder, gocql.StreamOptions{Incremental: true, Options: testOptions(gocql.OptionValues{HostID: cql.Option[cql.UUID]{Set: true, Value: target}, PageSize: cql.Option[int32]{Set: true, Value: 1}})})
	if err != nil {
		t.Fatal(err)
	}
	defer interrupted.Close()
	if !interrupted.NextInto(&dst) {
		t.Fatal("interruption first row", interrupted.Err())
	}
	nodeCommand(t, "stop")
	// Always restart the exact stopped node, even if an assertion below fails.
	restarted := false
	defer func() {
		if !restarted {
			nodeCommand(t, "start", "--wait-for-binary-proto")
		}
	}()
	if interrupted.NextInto(&dst) || interrupted.Err() == nil {
		t.Fatal("server interruption was not terminal", interrupted.Err())
	}
	terminal := interrupted.Err()
	if interrupted.Info().DeliveredRows != 1 || !interrupted.Info().OutputStarted {
		t.Fatal("interruption replayed output", interrupted.Info())
	}
	if interrupted.NextInto(&dst) || !errors.Is(interrupted.Err(), terminal) {
		t.Fatal("terminal interruption changed")
	}
	if err := interrupted.Close(); !errors.Is(err, terminal) {
		t.Fatal("close lost interruption", err)
	}
	nodeCommand(t, "start", "--wait-for-binary-proto")
	restarted = true
	repairDeadline := time.Now().Add(40 * time.Second)
	for {
		probe, probeErr := Run(ctx, s, bound, testOptions(gocql.OptionValues{HostID: cql.Option[cql.UUID]{Set: true, Value: target}, Prefetch: cql.Option[int]{Set: true, Value: 0}}))
		if probeErr == nil {
			n := 0
			for probe.Next() {
				n++
			}
			probeErr = probe.Close()
			if probeErr == nil && n == 5 {
				break
			}
		}
		if time.Now().After(repairDeadline) {
			t.Fatal("coordinator pool did not recover", probeErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("actual coordinator stop/join after one delivered row: %v; restart and shared pool probe succeeded", terminal)
	bound.Close()
	if err := bound.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("public RunStream protocol=%d compression=%d tls=%v rows=%d pages=%d bytes/row=%d attempts=%d", config.Protocol, config.Compression, *flagRunSslTest, info.DeliveredRows, info.Pages, len(payload), info.Result.Attempts)
}

// The finite protocol is serviced by the same native.py process holding the
// address locks. It accepts only nodes belonging to that exact live cluster.
func nodeCommand(t testing.TB, action ...string) {
	t.Helper()
	conn, err := net.DialTimeout("unix", os.Getenv("GOCQL_FIXTURE_CONTROL"), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	command := append([]string{"node1"}, action...)
	if err := json.NewEncoder(conn).Encode(struct {
		Command []string `json:"command"`
	}{command}); err != nil {
		t.Fatal(err)
	}
	var response struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Error != "" {
		t.Fatal(response.Error)
	}
}
