//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package integration

import (
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"testing"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cql/stream"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/exec"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/tests/integration/internal/mapped"
)

// TestStructMapping covers struct mappings through every query execution API:
// Prepared, Query, unprepared queries, batches, conditional statements and row
// streams.
func TestStructMapping(t *testing.T) {
	// Query.AddToBatch with Session.ExecBatch, and a counter table updated through a struct.
	t.Run("Batch", func(t *testing.T) {
		ctx, f := newSMFixture(t)
		insert, err := f.insert(t, ctx).Query(exec.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer insert.Close()
		var batch gocql.Batch
		var want []smRecord
		for i := range 3 {
			r := smRecordAt(6, i)
			want = append(want, r)
			if err := insert.AddToBatch(&batch, r); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := f.session.ExecBatch(ctx, &batch); err != nil {
			t.Fatal(err)
		}
		var got smRecord
		n := 0
		if _, err := f.selectBucket(t, ctx, cql.MustRowOf(smAttrs)).Each(ctx, 6, &got, func(r *smRecord) error {
			err := recordError(*r, want[n])
			n++
			return err
		}); err != nil || n != len(want) {
			t.Fatal(n, err)
		}

		counters := testTableName(t, "counters")
		if err := v2CreateTable(ctx, f.session, "CREATE TABLE IF NOT EXISTS "+counters+" (k int PRIMARY KEY, hits counter)"); err != nil {
			t.Fatal(err)
		}
		type hit struct {
			K    int32
			Hits cql.Counter
		}
		addPrepared, err := f.session.Prepare(ctx, cql.Statement[hit, cql.EmptyRow]{
			Kind:   cql.OrdinaryStatement,
			CQL:    "UPDATE " + counters + " SET hits = hits + ? WHERE k = ?",
			Params: cql.MustParamsOf[hit](),
		})
		if err != nil {
			t.Fatal(err)
		}
		add, err := addPrepared.Query(exec.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer add.Close()
		if err := truncateTable(t, f.session, counters); err != nil {
			t.Fatal(err)
		}
		batch.Reset()
		batch.Kind = cql.CounterBatch
		for _, h := range []hit{{K: 1, Hits: 2}, {K: 1, Hits: 3}} {
			if err := add.AddToBatch(&batch, h); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := f.session.ExecBatch(ctx, &batch); err != nil {
			t.Fatal(err)
		}
		readPrepared, err := f.session.Prepare(ctx, cql.Statement[int32, hit]{
			Kind: cql.OrdinaryStatement, CQL: "SELECT k, hits FROM " + counters + " WHERE k = ?",
			Params: cql.OneParam(cql.Int32Codec), Rows: cql.MustRowOf[hit](),
		})
		if err != nil {
			t.Fatal(err)
		}
		read, err := readPrepared.Query(exec.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer read.Close()
		var h hit
		if ok, _, err := read.One(ctx, 1, &h); err != nil || !ok || h != (hit{K: 1, Hits: 5}) {
			t.Fatal(h, ok, err)
		}
	})

	// Conditional statements decode the previous row into a struct, alone and in
	// a conditional batch.
	t.Run("Conditional", func(t *testing.T) {
		ctx, f := newSMFixture(t)
		insertPrepared, err := f.session.Prepare(ctx, cql.Statement[smRecord, cql.CASResult[smRecord]]{
			Kind:   cql.ConditionalStatement,
			CQL:    "INSERT INTO " + f.table + " (" + smColumns + ") VALUES (?" + repeatMarkers(21) + ") IF NOT EXISTS",
			Params: cql.MustParamsOf(smAttrs),
			Rows:   cql.CASRows(cql.MustRowOf(smAttrs)),
		})
		if err != nil {
			t.Fatal(err)
		}
		insert, err := insertPrepared.Query(exec.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer insert.Close()
		r := smRecordAt(7, 0)
		var result cql.CASResult[smRecord]
		if ok, _, err := insert.One(ctx, r, &result); err != nil || !ok || !result.Applied {
			t.Fatal("first insert:", ok, err, result.Applied)
		}
		changed := r
		changed.Name = "other"
		if ok, _, err := insert.One(ctx, changed, &result); err != nil || !ok || result.Applied || !result.Previous.Set {
			t.Fatal("second insert:", ok, err, result)
		}
		requireRecord(t, result.Previous.Value, r)

		type guard struct {
			Bucket int32
			ID     cql.UUID `cql:"id"`
			Name   string
			Big    int64
		}
		updateRows := cql.CASRows(cql.MustRowOf(smAttrs))
		updatePrepared, err := f.session.Prepare(ctx, cql.Statement[guard, cql.CASResult[smRecord]]{
			Kind:   cql.ConditionalStatement,
			CQL:    "UPDATE " + f.table + " SET name = ? WHERE bucket = ? AND id = ? IF big = ?",
			Params: cql.MustParamsOf[guard](),
			Rows:   updateRows,
		})
		if err != nil {
			t.Fatal(err)
		}
		update, err := updatePrepared.Query(exec.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer update.Close()
		var batch gocql.Batch
		if err := update.AddToBatch(&batch, guard{Bucket: 7, ID: r.ID, Name: "batched", Big: r.Big}); err != nil {
			t.Fatal(err)
		}
		rows, err := f.session.RunBatch(ctx, &batch, updateRows)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		applied := 0
		for rows.NextInto(&result) {
			if result.Applied {
				applied++
			}
		}
		if err := rows.Err(); err != nil || applied == 0 {
			t.Fatal("conditional batch:", applied, err)
		}
	})

	// A mapping generated by cqlgen and the one reflection builds for the same
	// struct write and read each other's rows, buffered and streamed.
	t.Run("Generated", func(t *testing.T) {
		ctx, f := newSMFixture(t)
		if fields := (mapped.Event{}).CQLFields(); fields.Params == nil {
			t.Fatal("no generated Event mapping")
		}
		if (mapped.StreamEvent{}).CQLStreamColumns() == nil {
			t.Fatal("no generated StreamEvent stream mapping")
		}
		const columns = "bucket, id, name, blob, big, maybe, tags, scores, createdat, updatedat"
		reflectedParams := codecTestMust(codec.ReflectParams[mapped.Event]())
		reflectedRows := codecTestMust(codec.ReflectRow[mapped.Event]())
		events := make([]mapped.Event, 2)
		for i := range events {
			r := smRecordAt(10, i)
			events[i] = mapped.Event{Bucket: r.Bucket, ID: r.ID, Name: r.Name, Blob: r.Blob, Big: r.Big, Maybe: r.Maybe,
				Tags: r.Tags, Scores: r.Scores, Audit: mapped.Audit{CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt}}
		}
		event := func(i int) mapped.Event { return events[i] }
		for i, params := range []cql.ParamEncoder[mapped.Event]{cql.MustParamsOf[mapped.Event](), reflectedParams} {
			insertPrepared, err := f.session.Prepare(ctx, cql.Statement[mapped.Event, cql.EmptyRow]{
				Kind: cql.OrdinaryStatement, CQL: "INSERT INTO " + f.table + " (" + columns + ") VALUES (?" + repeatMarkers(9) + ")", Params: params,
			})
			if err != nil {
				t.Fatal(err)
			}
			insert, err := insertPrepared.Query(exec.Options{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := insert.Exec(ctx, event(i)); err != nil {
				t.Fatal(err)
			}
			insert.Close()
			if err := insert.Wait(ctx); err != nil {
				t.Fatal(err)
			}
		}
		for _, rows := range []cql.RowDecoder[mapped.Event]{cql.MustRowOf[mapped.Event](), reflectedRows} {
			readPrepared, err := f.session.Prepare(ctx, cql.Statement[int32, mapped.Event]{
				Kind: cql.OrdinaryStatement, CQL: "SELECT " + columns + " FROM " + f.table + " WHERE bucket = ?", Params: cql.OneParam(cql.Int32Codec), Rows: rows,
			})
			if err != nil {
				t.Fatal(err)
			}
			read, err := readPrepared.Query(exec.Options{})
			if err != nil {
				t.Fatal(err)
			}
			var got mapped.Event
			n := 0
			if _, err := read.Each(ctx, 10, &got, func(e *mapped.Event) error {
				if !reflect.DeepEqual(*e, event(n)) {
					return fmt.Errorf("row %d:\ngot  %+v\nwant %+v", n, *e, event(n))
				}
				n++
				return nil
			}); err != nil || n != 2 {
				t.Fatal(n, err)
			}
			read.Close()
			if err := read.Wait(ctx); err != nil {
				t.Fatal(err)
			}
		}
		for _, decoder := range []stream.RowDecoder[mapped.StreamEvent]{stream.MustRowOf[mapped.StreamEvent](), codecTestMust(codec.ReflectStreamRow[mapped.StreamEvent]())} {
			prepared, err := f.session.PrepareStream(ctx, stream.Statement[int32, mapped.StreamEvent]{
				Kind: cql.OrdinaryStatement, CQL: "SELECT bucket, id, name, blob, big, maybe, scores, createdat, updatedat FROM " + f.table + " WHERE bucket = ?",
				Params: cql.OneParam(cql.Int32Codec), Rows: decoder,
			})
			if err != nil {
				t.Fatal(err)
			}
			q, err := prepared.Query(exec.StreamOptions{})
			if err != nil {
				t.Fatal(err)
			}
			stream, err := q.Run(ctx, 10)
			if err != nil {
				t.Fatal(err)
			}
			got := mapped.StreamEvent{Name: make([]byte, 0, 32), Blob: make([]byte, 0, 32), Scores: make([]int32, 0, 4)}
			n := 0
			for stream.NextInto(&got) {
				w := event(n)
				if string(got.Name) != w.Name || got.ID != w.ID || !reflect.DeepEqual(got.Scores, w.Scores) || got.Audit != w.Audit {
					t.Fatalf("stream row %d:\ngot  %+v\nwant %+v", n, got, w)
				}
				n++
			}
			if err := stream.Err(); err != nil || n != 2 {
				t.Fatal(n, err)
			}
			stream.Close()
			q.Close()
			if err := q.Wait(ctx); err != nil {
				t.Fatal(err)
			}
		}
	})

	// Null values and empty collections, which CQL stores as null, read back as
	// invalid Nullables and empty slices.
	t.Run("Nulls", func(t *testing.T) {
		ctx, f := newSMFixture(t)
		r := smRecordAt(4, 0)
		r.Maybe, r.Tags, r.Scores, r.Attrs, r.Blob = cql.Nullable[int32]{}, []string{}, nil, nil, []byte{}
		if _, err := f.insert(t, ctx).Exec(ctx, r); err != nil {
			t.Fatal(err)
		}
		var got smRecord
		if ok, _, err := f.selectBucket(t, ctx, cql.MustRowOf(smAttrs)).One(ctx, 4, &got); err != nil || !ok {
			t.Fatal(ok, err)
		}
		if got.Maybe.Valid || len(got.Tags) != 0 || len(got.Scores) != 0 || len(got.Attrs) != 0 || len(got.Blob) != 0 {
			t.Fatalf("nulls and empty collections: %+v", got)
		}
	})

	// Varint and decimal round trip through their struct fields.
	t.Run("Numeric", func(t *testing.T) {
		ctx, f := newSMFixture(t)
		table := testTableName(t, "numeric")
		if err := v2CreateTable(ctx, f.session, "CREATE TABLE IF NOT EXISTS "+table+" (k int PRIMARY KEY, v varint, d decimal)"); err != nil {
			t.Fatal(err)
		}
		type numeric struct {
			K int32
			V cql.Varint
			D cql.Decimal
		}
		insertPrepared, err := f.session.Prepare(ctx, cql.Statement[numeric, cql.EmptyRow]{
			Kind: cql.OrdinaryStatement, CQL: "INSERT INTO " + table + " (k, v, d) VALUES (?, ?, ?)", Params: cql.MustParamsOf[numeric](),
		})
		if err != nil {
			t.Fatal(err)
		}
		insert, err := insertPrepared.Query(exec.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer insert.Close()
		huge, _ := new(big.Int).SetString("-123456789012345678901234567890", 10)
		in := numeric{K: 1, V: cql.VarintFromBigInt(huge), D: cql.Decimal{Unscaled: cql.VarintFromBigInt(big.NewInt(314)), Scale: 2}}
		if _, err := insert.Exec(ctx, in); err != nil {
			t.Fatal(err)
		}
		readPrepared, err := f.session.Prepare(ctx, cql.Statement[int32, numeric]{
			Kind: cql.OrdinaryStatement, CQL: "SELECT k, v, d FROM " + table + " WHERE k = ?", Params: cql.OneParam(cql.Int32Codec), Rows: cql.MustRowOf[numeric](),
		})
		if err != nil {
			t.Fatal(err)
		}
		read, err := readPrepared.Query(exec.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer read.Close()
		// Varint and Decimal borrow the response page: read them while the row is
		// current, or Clone the row to keep it.
		rows, err := read.Run(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		var out, kept numeric
		n := 0
		for rows.NextInto(&out) {
			if out.V.BigInt().Cmp(huge) != 0 || out.D.Unscaled.BigInt().Int64() != 314 || out.D.Scale != 2 {
				t.Fatalf("numeric round trip: %v %v/%d", out.V.BigInt(), out.D.Unscaled.BigInt(), out.D.Scale)
			}
			if kept, err = rows.Clone(); err != nil {
				t.Fatal(err)
			}
			n++
		}
		if err := rows.Err(); err != nil || n != 1 {
			t.Fatal(n, err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if kept.V.BigInt().Cmp(huge) != 0 || kept.D.Unscaled.BigInt().Int64() != 314 {
			t.Fatal("cloned numeric row did not survive its page")
		}
	})

	// Prepared.Exec, Prepared.One and Prepared.Each.
	t.Run("Prepared", func(t *testing.T) {
		ctx, f := newSMFixture(t)
		insert := f.insert(t, ctx)
		want := []smRecord{smRecordAt(1, 0), smRecordAt(1, 1), smRecordAt(1, 2)}
		for _, r := range want {
			if _, err := insert.Exec(ctx, r); err != nil {
				t.Fatal(err)
			}
		}
		one, err := f.session.Prepare(ctx, cql.Statement[smKey, smRecord]{
			Kind:   cql.OrdinaryStatement,
			CQL:    "SELECT " + smColumns + " FROM " + f.table + " WHERE bucket = ? AND id = ?",
			Params: cql.MustParamsOf[smKey](),
			Rows:   cql.MustRowOf(smAttrs),
		})
		if err != nil {
			t.Fatal(err)
		}
		var got smRecord
		if ok, _, err := one.One(ctx, smKey{Bucket: 1, ID: want[1].ID}, &got); err != nil || !ok {
			t.Fatal(ok, err)
		}
		requireRecord(t, got, want[1])
		if ok, _, err := one.One(ctx, smKey{Bucket: 1, ID: cql.UUID{}}, &got); err != nil || ok {
			t.Fatal("missing row:", ok, err)
		}
		var all []smRecord
		_, err = f.selectBucket(t, ctx, cql.MustRowOf(smAttrs)).Each(ctx, 1, &got, func(r *smRecord) error {
			all = append(all, *r)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(all) != len(want) {
			t.Fatalf("Each: %d rows, want %d", len(all), len(want))
		}
		for i := range want {
			requireRecord(t, all[i], want[i])
		}
	})

	// Query.Exec, Run, RunPage (paged and unpaged), Each, One and Routing,
	// with paging across pages of two rows.
	t.Run("Query", func(t *testing.T) {
		ctx, f := newSMFixture(t)
		insert, err := f.insert(t, ctx).Query(exec.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer insert.Close()
		var want []smRecord
		for i := range 5 {
			r := smRecordAt(2, i)
			want = append(want, r)
			if _, err := insert.Exec(ctx, r); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := insert.Routing(want[0]); err != nil {
			t.Fatal(err)
		}
		read, err := f.selectBucket(t, ctx, cql.MustRowOf(smAttrs)).Query(exec.Options{PageSize: cql.Option[int32]{Set: true, Value: 2}})
		if err != nil {
			t.Fatal(err)
		}
		defer read.Close()

		t.Run("Run", func(t *testing.T) {
			rows, err := read.Run(ctx, 2)
			if err != nil {
				t.Fatal(err)
			}
			var got smRecord
			n := 0
			for rows.NextInto(&got) {
				requireRecord(t, got, want[n])
				clone, err := rows.Clone()
				if err != nil {
					t.Fatal(err)
				}
				requireRecord(t, clone, want[n])
				n++
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			if n != len(want) {
				t.Fatalf("Run: %d rows across pages, want %d", n, len(want))
			}
		})
		t.Run("RunPage", func(t *testing.T) {
			state, n := cql.StartPage, 0
			for pages := 0; ; pages++ {
				page, err := read.RunPage(ctx, 2, state, 0)
				if err != nil {
					t.Fatal(err)
				}
				var got smRecord
				for page.NextInto(&got) {
					requireRecord(t, got, want[n])
					n++
				}
				if err := page.Err(); err != nil {
					t.Fatal(err)
				}
				next := page.State()
				if err := page.Close(); err != nil {
					t.Fatal(err)
				}
				if !next.More() {
					break
				}
				// Resume from a page state saved as bytes, as a client would.
				state = cql.PageStateFromToken(next.Token())
				if pages > len(want) {
					t.Fatal("paging does not end")
				}
			}
			if n != len(want) {
				t.Fatalf("RunPage: %d rows, want %d", n, len(want))
			}
		})
		t.Run("Unpaged", func(t *testing.T) {
			// The cql.Unpaged page size returns the whole result as one page.
			read, err := f.selectBucket(t, ctx, cql.MustRowOf(smAttrs)).Query(exec.Options{PageSize: cql.Some(cql.Unpaged)})
			if err != nil {
				t.Fatal(err)
			}
			defer read.Close()
			page, err := read.RunPage(ctx, 2, cql.StartPage, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer page.Close()
			var got smRecord
			n := 0
			for page.NextInto(&got) {
				requireRecord(t, got, want[n])
				n++
			}
			if err := page.Err(); err != nil || n != len(want) {
				t.Fatal(n, err)
			}
			if err := page.Close(); err != nil {
				t.Fatal(err)
			}
		})
		t.Run("Each", func(t *testing.T) {
			var got smRecord
			n := 0
			if _, err := read.Each(ctx, 2, &got, func(r *smRecord) error {
				err := recordError(*r, want[n])
				n++
				return err
			}); err != nil || n != len(want) {
				t.Fatal(n, err)
			}
		})
		t.Run("One", func(t *testing.T) {
			var got smRecord
			if ok, _, err := read.One(ctx, 2, &got); err != nil || !ok {
				t.Fatal(ok, err)
			}
			requireRecord(t, got, want[0])
			if ok, _, err := read.One(ctx, 99, &got); err != nil || ok {
				t.Fatal("empty bucket:", ok, err)
			}
		})
	})

	// With ReuseBuffers, NextInto decodes slice fields into the destination's
	// storage instead of allocating per row.
	t.Run("ReuseBuffers", func(t *testing.T) {
		ctx, f := newSMFixture(t)
		insert := f.insert(t, ctx)
		var want []smRecord
		for i := range 4 {
			r := smRecordAt(5, i)
			want = append(want, r)
			if _, err := insert.Exec(ctx, r); err != nil {
				t.Fatal(err)
			}
		}
		read, err := f.selectBucket(t, ctx, cql.MustRowOf(smAttrs, cql.ReuseBuffers[smRecord]())).Query(exec.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer read.Close()
		rows, err := read.Run(ctx, 5)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		got := smRecord{Blob: make([]byte, 0, 16), Tags: make([]string, 0, 4)}
		blob, tags := &got.Blob[:1][0], &got.Tags[:1][0]
		n := 0
		for rows.NextInto(&got) {
			requireRecord(t, got, want[n])
			if &got.Blob[0] != blob || &got.Tags[0] != tags {
				t.Fatalf("row %d: slice fields did not reuse the destination's storage", n)
			}
			n++
		}
		if err := rows.Err(); err != nil || n != 4 {
			t.Fatal(n, err)
		}
	})

	// Row streams decode into a struct through stream.RowOf, buffered and
	// incremental.
	t.Run("Stream", func(t *testing.T) {
		ctx, f := newSMFixture(t)
		insert := f.insert(t, ctx)
		var want []smRecord
		for i := range 5 {
			r := smRecordAt(9, i)
			want = append(want, r)
			if _, err := insert.Exec(ctx, r); err != nil {
				t.Fatal(err)
			}
		}
		const query = "SELECT bucket, id, name, blob, big, maybe, scores, span, createdat, updatedat FROM "
		read, err := f.session.PrepareStream(ctx, stream.Statement[int32, smStream]{
			Kind: cql.OrdinaryStatement, CQL: query + f.table + " WHERE bucket = ?", Params: cql.OneParam(cql.Int32Codec), Rows: stream.MustRowOf[smStream](),
		})
		if err != nil {
			t.Fatal(err)
		}
		check := func(t *testing.T, stream *gocql.RowStream[smStream]) {
			t.Helper()
			defer stream.Close()
			// Streaming decoders write into the destination's storage.
			got := smStream{Name: make([]byte, 0, 32), Blob: make([]byte, 0, 32), Scores: make([]int32, 0, 4)}
			n := 0
			for stream.NextInto(&got) {
				w := want[n]
				if got.Bucket != w.Bucket || got.ID != w.ID || string(got.Name) != w.Name || string(got.Blob) != string(w.Blob) ||
					got.Big != w.Big || got.Maybe != w.Maybe || !reflect.DeepEqual(got.Scores, w.Scores) || got.Span != w.Span || got.smAudit != w.smAudit {
					t.Fatalf("row %d:\ngot  %+v\nwant %+v", n, got, w)
				}
				n++
			}
			if err := stream.Err(); err != nil || n != len(want) {
				t.Fatal(n, err)
			}
		}
		// Protocol 4 frame compression has no incremental receive transform, so an
		// incremental stream is rejected before the query is sent.
		frameCompressed := *flagProto < 5 && fixtureCompressed()
		for _, incremental := range []bool{false, true} {
			options := exec.StreamOptions{Incremental: incremental, Options: exec.Options{PageSize: cql.Option[int32]{Set: true, Value: 2}}}
			q, err := read.Query(options)
			if err != nil {
				t.Fatal(err)
			}
			stream, err := q.Run(ctx, 9)
			if incremental && frameCompressed {
				if _, ok := errors.AsType[*cqlerr.StreamCapabilityError](err); !ok {
					t.Fatal("frame-compressed incremental stream not rejected:", err)
				}
				q.Close()
				if err := q.Wait(ctx); err != nil {
					t.Fatal(err)
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			check(t, stream)
			q.Close()
			if err := q.Wait(ctx); err != nil {
				t.Fatal(err)
			}
		}
	})

	// A struct binds statements that use a subset of its columns, and a select
	// of a subset of columns leaves the other fields zero.
	t.Run("Subset", func(t *testing.T) {
		ctx, f := newSMFixture(t)
		r := smRecordAt(3, 0)
		if _, err := f.insert(t, ctx).Exec(ctx, r); err != nil {
			t.Fatal(err)
		}
		updatePrepared, err := f.session.Prepare(ctx, cql.Statement[smRecord, cql.EmptyRow]{
			Kind:   cql.OrdinaryStatement,
			CQL:    "UPDATE " + f.table + " SET name = ?, tags = ?, maybe = ? WHERE bucket = ? AND id = ?",
			Params: cql.MustParamsOf(smAttrs),
		})
		if err != nil {
			t.Fatal(err)
		}
		update, err := updatePrepared.Query(exec.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer update.Close()
		edit := smRecord{Bucket: r.Bucket, ID: r.ID, Name: "edited", Tags: []string{"z"}, Big: 12345}
		if _, err := update.Exec(ctx, edit); err != nil {
			t.Fatal(err)
		}
		type partial struct {
			ID   cql.UUID `cql:"id"`
			Name string
			Tags []string
			Big  int64 // not selected: stays zero
			Note string
		}
		readPrepared, err := f.session.Prepare(ctx, cql.Statement[smKey, partial]{
			Kind:   cql.OrdinaryStatement,
			CQL:    "SELECT id, name, tags FROM " + f.table + " WHERE bucket = ? AND id = ?",
			Params: cql.MustParamsOf[smKey](),
			Rows:   cql.MustRowOf[partial](),
		})
		if err != nil {
			t.Fatal(err)
		}
		read, err := readPrepared.Query(exec.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer read.Close()
		got := partial{Big: 7, Note: "kept"}
		if ok, _, err := read.One(ctx, smKey{Bucket: r.Bucket, ID: r.ID}, &got); err != nil || !ok {
			t.Fatal(ok, err)
		}
		if want := (partial{ID: r.ID, Name: "edited", Tags: []string{"z"}}); !reflect.DeepEqual(got, want) {
			t.Fatalf("subset row:\ngot  %+v\nwant %+v", got, want)
		}
		// The update nulled maybe; Big and the rest are unchanged.
		var full smRecord
		one, err := f.session.Prepare(ctx, cql.Statement[smKey, smRecord]{
			Kind: cql.OrdinaryStatement, CQL: "SELECT " + smColumns + " FROM " + f.table + " WHERE bucket = ? AND id = ?",
			Params: cql.MustParamsOf[smKey](), Rows: cql.MustRowOf(smAttrs),
		})
		if err != nil {
			t.Fatal(err)
		}
		if ok, _, err := one.One(ctx, smKey{Bucket: r.Bucket, ID: r.ID}, &full); err != nil || !ok {
			t.Fatal(ok, err)
		}
		want := r
		want.Name, want.Tags, want.Maybe = "edited", []string{"z"}, cql.Nullable[int32]{}
		requireRecord(t, full, want)
	})

	// Unprepared calls bind struct fields by the bind marker names given.
	t.Run("Unprepared", func(t *testing.T) {
		ctx, f := newSMFixture(t)
		prepared := f.insert(t, ctx) // for the bind marker metadata
		r := smRecordAt(8, 0)
		if _, err := f.session.UnpreparedExec(ctx, cql.Statement[smRecord, cql.EmptyRow]{
			Kind:        cql.OrdinaryStatement,
			CQL:         "INSERT INTO " + f.table + " (" + smColumns + ") VALUES (?" + repeatMarkers(21) + ")",
			Params:      cql.MustParamsOf(smAttrs),
			BindMarkers: prepared.Parameters(),
		}, r); err != nil {
			t.Fatal(err)
		}
		keyMarkers := cql.NewColumns(
			cql.ColumnSpec{Name: "bucket", Type: cql.IntType},
			cql.ColumnSpec{Name: "id", Type: cql.UUIDType},
		)
		var got smRecord
		if ok, _, err := f.session.UnpreparedOne(ctx, cql.Statement[smKey, smRecord]{
			Kind:        cql.OrdinaryStatement,
			CQL:         "SELECT " + smColumns + " FROM " + f.table + " WHERE bucket = ? AND id = ?",
			Params:      cql.MustParamsOf[smKey](),
			Rows:        cql.MustRowOf(smAttrs),
			BindMarkers: keyMarkers,
		}, smKey{Bucket: 8, ID: r.ID}, &got); err != nil || !ok {
			t.Fatal(ok, err)
		}
		requireRecord(t, got, r)
	})
}
