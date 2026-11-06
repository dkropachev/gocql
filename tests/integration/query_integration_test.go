//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	gocql "github.com/scylladb/gocql/v2"
)

func TestV2QueryLiveReuse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	config := createV2Cluster()
	config.Keyspace = ""
	session, err := gocql.Connect(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	namespace := fmt.Sprintf("gocql_query_%x", time.Now().UnixNano())
	exec := func(ctx context.Context, cql string) error {
		q, err := gocql.NewUnpreparedQuery(session, gocql.Statement[gocql.Unit, gocql.Unit]{CQL: cql, Params: gocql.NoParams, Rows: gocql.NoRows}, gocql.NewColumns(), testOptions(gocql.OptionValues{Timeout: gocql.Option[time.Duration]{Set: true, Value: 30 * time.Second}}))
		if err != nil {
			return err
		}
		defer q.Close()
		err = q.Exec(ctx, gocql.Unit{})
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
		typ := codecTestMust(gocql.ScalarType(gocql.TypeText))
		query, err := gocql.NewUnpreparedQuery(session, gocql.Statement[string, gocql.Text]{Kind: gocql.OrdinaryStatement, CQL: "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name=?", Params: gocql.OneParam(gocql.StringEncoder), Rows: gocql.OneColumn(gocql.TextCodec)}, gocql.NewColumns(gocql.ColumnSpec{Name: "keyspace_name", Type: typ}), testOptions(gocql.OptionValues{Timeout: gocql.Option[time.Duration]{Set: true, Value: 10 * time.Second}}))
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
		if rows.Next() {
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
		var partial *gocql.PartialRefreshError
		if attempt >= 2 || !errors.As(err, &partial) || len(partial.Keyspaces) != 1 || partial.Keyspaces["<publication>"] == nil {
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
	write := codecTestMust(gocql.Prepare(ctx, session, gocql.Statement[record, gocql.Unit]{Kind: gocql.OrdinaryStatement, CQL: "INSERT INTO " + table + "(k,c,p)VALUES(?,?,?)", Params: gocql.Params(gocql.Param("k", gocql.Int32, func(r record) int32 { return r.K }), gocql.Param("c", gocql.Int32, func(r record) int32 { return r.C }), gocql.Param("p", gocql.BytesEncoder, func(r record) []byte { return r.P })), Rows: gocql.NoRows}, gocql.PrepareOptions{}))
	w := codecTestMust(gocql.NewQuery(write, gocql.Options{}))
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
	decoder := gocql.RowInto(func(*record) {}, gocql.Column("c", gocql.Int32, func(r *record) *int32 { return &r.C }), gocql.Column("p", gocql.OwnedBlobCodec, func(r *record) *[]byte { return &r.P }))
	read := codecTestMust(gocql.Prepare(ctx, session, gocql.Statement[int32, record]{Kind: gocql.OrdinaryStatement, CQL: "SELECT c,p FROM " + table + " WHERE k=?", Params: gocql.OneParam(gocql.Int32), Rows: decoder}, gocql.PrepareOptions{}))
	q := codecTestMust(gocql.NewQuery(read, testOptions(gocql.OptionValues{PageSize: gocql.Option[int32]{Set: true, Value: 2}, Prefetch: gocql.Option[int]{Set: true, Value: 0}})))
	defer q.Close()
	var previous gocql.Rows[record]
	for range 2 {
		rows := codecTestMust(q.Run(ctx, int32(1)))
		// A stale handle from the previous execution must not touch this one.
		if err := previous.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := q.Run(ctx, int32(1)); !errors.Is(err, gocql.ErrBusy) {
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
	state := gocql.StartPage()
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
	unpaged := codecTestMust(gocql.NewQuery(read, testOptions(gocql.OptionValues{Prefetch: gocql.Option[int]{Set: true, Value: 0}})))
	defer unpaged.Close()
	page := codecTestMust(unpaged.RunUnpaged(ctx, int32(1)))
	n := 0
	for page.Next() {
		n++
	}
	if err := page.Close(); err != nil || n != 6 {
		t.Fatal("unpaged", err, n)
	}
	for _, incremental := range []bool{false, true} {
		streamDecoder := gocql.StreamRow(func(*record) {}, gocql.StreamColumn("c", gocql.StreamInt32, func(r *record) *int32 { return &r.C }), gocql.StreamColumn("p", gocql.StreamBlob, func(r *record) *[]byte { return &r.P }))
		streamQuery := codecTestMust(gocql.NewStreamQuery(read, streamDecoder, gocql.StreamOptions{Incremental: incremental, Options: testOptions(gocql.OptionValues{PageSize: gocql.Option[int32]{Set: true, Value: 2}})}))
		cursor, err := streamQuery.Run(ctx, int32(1))
		if incremental && config.Protocol < 5 && config.Compression != gocql.NoCompression {
			var unsupported *gocql.StreamCapabilityError
			if !errors.As(err, &unsupported) {
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
	var entries []gocql.BatchItem
	for i := int32(0); i < 3; i++ {
		entries = append(entries, codecTestMust(w.BatchEntry(record{K: 2, C: i, P: payload})))
	}
	payload[0] = 99
	if _, err := gocql.ExecBatch(ctx, session, gocql.UnloggedBatch, entries, gocql.Options{}); err != nil {
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
}
