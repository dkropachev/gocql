//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/routing"
)

func TestSelectWithoutFrom(t *testing.T) {
	t.Parallel()

	t.Run("Bind", func(t *testing.T) {
		t.Parallel()
		skipUnlessSelectWithoutFrom(t)
		session := createSessionWithoutKeyspace(t)
		defer session.Close()
		t.Run("value round trip", func(t *testing.T) {
			if noFromInt(t, noFromOne(t, session, "SELECT blobAsInt(?)", cql.BlobValue([]byte{0, 0, 0, 42})), 0) != 42 {
				t.Fatal("wrong bind result")
			}
		})
		t.Run("marker types the column", func(t *testing.T) {
			row := noFromOne(t, session, "SELECT intAsBlob(?)", cql.IntValue(42))
			if noFromColumn(t, row.Columns(), 0).Type.Kind() != cql.TypeBlob {
				t.Fatal("blob type expected")
			}
			blob, err := noFromValue(t, row, 0).Bytes()
			if err != nil || !bytes.Equal(blob.OwnedBytes(), []byte{0, 0, 0, 42}) {
				t.Fatalf("blob=%v err=%v", blob, err)
			}
		})
		t.Run("marker in LIMIT", func(t *testing.T) {
			if noFromInt(t, noFromOne(t, session, "SELECT 1 LIMIT ?", cql.IntValue(1)), 0) != 1 {
				t.Fatal("wrong LIMIT result")
			}
		})
		t.Run("untyped markers rejected", func(t *testing.T) {
			for _, stmt := range []string{"SELECT ?", "SELECT (int)?", "SELECT CAST(? AS int)"} {
				rows, err := v2Query(context.Background(), session, stmt, cql.IntValue(42))
				_ = rows.Close()
				if !isRejectedByServer(err) {
					t.Fatalf("%q error=%v", stmt, err)
				}
			}
		})
	})

	t.Run("DynamicScan", func(t *testing.T) {
		t.Parallel()
		skipUnlessSelectWithoutFrom(t)
		session := createSessionWithoutKeyspace(t)
		defer session.Close()
		const stmt = "SELECT 1, now()"
		t.Run("MapScan", func(t *testing.T) {
			row := noFromOne(t, session, stmt)
			seen := map[string]bool{}
			for i := 0; i < row.Columns().Len(); i++ {
				col, _ := row.Columns().At(i)
				if _, err := row.Get(col.Name); err != nil {
					t.Fatal(err)
				}
				seen[col.Name] = true
			}
			if len(seen) != row.Columns().Len() {
				t.Fatal("duplicate generated names")
			}
		})
		t.Run("SliceMap", func(t *testing.T) {
			rows := noFromRows(t, session, stmt)
			var owned []cql.DynamicRow
			var row cql.DynamicRow
			for rows.NextInto(&row) {
				owned = append(owned, row.Clone())
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if len(owned) != 1 {
				t.Fatalf("row count=%d", len(owned))
			}
			if noFromInt(t, owned[0], 0) != 1 {
				t.Fatal("owned row changed")
			}
		})
	})

	t.Run("Exec", func(t *testing.T) {
		t.Parallel()

		t.Run("Aggregate", func(t *testing.T) {
			t.Parallel()
			skipUnlessSelectWithoutFrom(t)
			session := createSessionWithoutKeyspace(t)
			defer session.Close()
			if got := noFromInt(t, noFromOne(t, session, "SELECT count(5)"), 0); got != 1 {
				t.Fatalf("count(5)=%d", got)
			}
		})
		t.Run("Alias", func(t *testing.T) {
			t.Parallel()
			skipUnlessSelectWithoutFrom(t)
			session := createSessionWithoutKeyspace(t)
			defer session.Close()
			row := noFromOne(t, session, "SELECT 1, 'hi' AS greeting, now()")
			names := make([]string, row.Columns().Len())
			for i := range names {
				names[i] = noFromColumn(t, row.Columns(), i).Name
			}
			if !slices.Equal(names, []string{"1", "greeting", "system.now()"}) {
				t.Fatalf("aliases=%v", names)
			}
			if noFromInt(t, row, 0) != 1 || noFromText(t, row, 1) != "hi" {
				t.Fatal("incorrect literal values")
			}
			if _, err := noFromValue(t, row, 2).UUID(); err != nil {
				t.Fatal(err)
			}
			greeting, err := row.Get("greeting")
			if err != nil {
				t.Fatal(err)
			}
			text, err := greeting.Text()
			if err != nil || text.String() != "hi" {
				t.Fatalf("named lookup=%v %v", text, err)
			}
		})
		t.Run("FunctionCast", func(t *testing.T) {
			t.Parallel()
			skipUnlessSelectWithoutFrom(t)
			session := createSessionWithoutKeyspace(t)
			defer session.Close()
			before := time.Now()
			row := noFromOne(t, session, "SELECT toTimestamp(now())")
			after := time.Now()
			if noFromColumn(t, row.Columns(), 0).Type.Kind() != cql.TypeTimestamp {
				t.Fatal("timestamp metadata expected")
			}
			got, err := cql.Timestamp(noFromInt(t, row, 0)).Time()
			if err != nil {
				t.Fatal(err)
			}
			if got.Before(before.Add(-time.Hour)) || got.After(after.Add(time.Hour)) {
				t.Fatalf("timestamp outside tolerance: %v", got)
			}
		})
		t.Run("JSON", func(t *testing.T) {
			t.Parallel()
			skipUnlessSelectWithoutFrom(t)
			session := createSessionWithoutKeyspace(t)
			defer session.Close()
			if implCluster.SessionData(session).InternalCfg.ProtoVersion < protoVersion4 {
				t.Skip("JSON requires protocol4")
			}
			got := noFromText(t, noFromOne(t, session, "SELECT JSON 1"), 0)
			var doc map[string]json.Number
			if err := json.Unmarshal([]byte(got), &doc); err != nil {
				t.Fatal(err)
			}
			if len(doc) != 1 || doc["1"].String() != "1" {
				t.Fatalf("incorrect JSON result: %s", got)
			}
		})
		t.Run("Literal", func(t *testing.T) {
			t.Parallel()
			skipUnlessSelectWithoutFrom(t)
			session := createSessionWithoutKeyspace(t)
			defer session.Close()
			rows := noFromRows(t, session, "SELECT 1")
			if rows.Columns().Len() != 1 {
				t.Fatal("SELECT 1 must have one column")
			}
			if noFromColumn(t, rows.Columns(), 0).Type.Kind() != cql.TypeInt {
				t.Fatal("literal must have int metadata")
			}
			if rows.Info().Rows != 1 {
				t.Fatalf("page rows=%d want1", rows.Info().Rows)
			}
			var row cql.DynamicRow
			if !rows.NextInto(&row) {
				t.Fatalf("missing row: %v", rows.Err())
			}
			if n := noFromInt(t, row, 0); n != 1 {
				t.Fatalf("got%d want1", n)
			}
			if rows.NextInto(&row) {
				t.Fatal("virtual table returned a second row")
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
		})
		t.Run("MultipleSelectors", func(t *testing.T) {
			t.Parallel()
			skipUnlessSelectWithoutFrom(t)
			session := createSessionWithoutKeyspace(t)
			defer session.Close()
			row := noFromOne(t, session, "SELECT 1, 'a', true")
			if row.Columns().Len() != 3 {
				t.Fatal("expected three columns")
			}
			for i, kind := range []cql.Type{cql.TypeInt, cql.TypeVarchar, cql.TypeBoolean} {
				if noFromColumn(t, row.Columns(), i).Type.Kind() != kind {
					t.Fatalf("column%d wrong type", i)
				}
			}
			yes, err := noFromValue(t, row, 2).Bool()
			if err != nil {
				t.Fatal(err)
			}
			if noFromInt(t, row, 0) != 1 || noFromText(t, row, 1) != "a" || !yes {
				t.Fatal("incorrect selector values")
			}
		})
		t.Run("Now", func(t *testing.T) {
			t.Parallel()
			skipUnlessSelectWithoutFrom(t)
			session := createSessionWithoutKeyspace(t)
			defer session.Close()
			before := time.Now()
			row := noFromOne(t, session, "SELECT now()")
			after := time.Now()
			if noFromColumn(t, row.Columns(), 0).Type.Kind() != cql.TypeTimeUUID {
				t.Fatal("now() metadata must be timeuuid")
			}
			got, err := noFromValue(t, row, 0).UUID()
			if err != nil {
				t.Fatal(err)
			}
			if got.Version() != 1 {
				t.Fatalf("UUID version%d", got.Version())
			}
			if at := got.Time(); at.Before(before.Add(-time.Hour)) || at.After(after.Add(time.Hour)) {
				t.Fatalf("timestamp outside tolerance: %v", at)
			}
		})
		t.Run("SessionKeyspace", func(t *testing.T) {
			t.Parallel()
			skipUnlessSelectWithoutFrom(t)
			session := createV2Session(t)
			defer session.Close()
			if implCluster.SessionData(session).InternalCfg.Keyspace == "" {
				t.Fatal("expected configured keyspace")
			}
			if got := noFromInt(t, noFromOne(t, session, "SELECT 1"), 0); got != 1 {
				t.Fatalf("got%d", got)
			}
		})
		t.Run("Where", func(t *testing.T) {
			t.Parallel()
			skipUnlessSelectWithoutFrom(t)
			session := createSessionWithoutKeyspace(t)
			defer session.Close()
			const kept = `SELECT 1 WHERE "system$dummy" = ''`
			const filtered = `SELECT 1 WHERE "system$dummy" = 'no'`
			t.Run("predicate keeps the row", func(t *testing.T) {
				if noFromInt(t, noFromOne(t, session, kept), 0) != 1 {
					t.Fatal("wrong kept value")
				}
			})
			t.Run("predicate removes the row", func(t *testing.T) {
				rows := noFromRows(t, session, filtered)
				if rows.Columns().Len() != 1 || rows.Info().Rows != 0 {
					t.Fatal("empty result must retain one column and zero rows")
				}
				var row cql.DynamicRow
				if rows.NextInto(&row) {
					t.Fatal("filtered virtual row returned")
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				if _, err := v2ReadOne(context.Background(), session, filtered); !errors.Is(err, cqlerr.ErrNoRows) {
					t.Fatalf("empty single-row result=%v", err)
				}
			})
		})
	})

	t.Run("ExecutionModes", func(t *testing.T) {
		t.Parallel()
		skipUnlessSelectWithoutFrom(t)
		session := createSessionWithoutKeyspace(t)
		defer session.Close()
		const stmt = "SELECT 1, 'a', true"
		statement := cql.Statement[cql.EmptyParams, cql.DynamicRow]{CQL: stmt, Params: cql.EmptyParamsEncoder, Rows: cql.DynamicRows}
		var baseline cql.Columns
		for _, mode := range []string{"prepared", "prepared with metadata", "not prepared"} {
			t.Run(mode, func(t *testing.T) {
				var bound *Bound[cql.DynamicRow]
				var err error
				if mode == "not prepared" {
					bound, err = BindUnpreparedOwned(statement, cql.NewColumns(), cql.EmptyParams{})
				} else {
					prepared, e := gocql.Prepare(context.Background(), session, statement, gocql.PrepareOptions{})
					if e != nil {
						t.Fatal(e)
					}
					bound, err = BindOwned(prepared, cql.EmptyParams{})
				}
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					bound.Close()
					if err := bound.Wait(context.Background()); err != nil {
						t.Error(err)
					}
				}()
				rows, err := Run(context.Background(), session, bound, bindOptions(session, gocql.Options{IncludeMetadata: mode == "prepared with metadata"}))
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				columns := rows.Columns()
				logColumns(t, stmt+" "+mode, columns)
				if columns.Len() != 3 {
					t.Fatal("expected three columns")
				}
				for i := 0; i < columns.Len(); i++ {
					col, _ := columns.At(i)
					if col.Keyspace != "system" || col.Table != "one_row" {
						t.Fatalf("source=%s.%s", col.Keyspace, col.Table)
					}
				}
				if baseline.Len() == 0 {
					baseline = columns
				} else if !columns.Equal(baseline) {
					t.Fatal("metadata differs across execution modes")
				}
				var row cql.DynamicRow
				if !rows.NextInto(&row) {
					t.Fatalf("missing row: %v", rows.Err())
				}
				yes, e := noFromValue(t, row, 2).Bool()
				if e != nil || !yes || noFromInt(t, row, 0) != 1 || noFromText(t, row, 1) != "a" {
					t.Fatal("wrong selector values")
				}
				if rows.NextInto(&row) {
					t.Fatal("extra virtual row")
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
			})
		}
	})

	t.Run("Rejected", func(t *testing.T) {
		t.Parallel()
		skipUnlessSelectWithoutFrom(t)
		session := createSessionWithoutKeyspace(t)
		defer session.Close()
		// Exercise the typed transport on the identical connection after every error;
		// host pinning alone would allow a different pooled socket to hide corruption.
		conn, err := implCluster.InternalSessionSelectTypedConn(session, context.Background(), routing.Info{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct{ name, stmt string }{{"star", "SELECT *"}, {"column reference", "SELECT some_column"}, {"distinct", "SELECT DISTINCT 1"}} {
			t.Run(tc.name, func(t *testing.T) {
				page, err := conn.InternalExecuteTyped(context.Background(), &attemptRequest{Statement: tc.stmt, Consistency: cql.One})
				if page != nil {
					page.Owner.Release()
				}
				if !isRejectedByServer(err) {
					t.Fatalf("%q rejection=%v", tc.stmt, err)
				}
				page, err = conn.InternalExecuteTyped(context.Background(), &attemptRequest{Statement: "SELECT 1", Consistency: cql.One})
				if err != nil {
					t.Fatalf("same connection unusable after rejection: %v", err)
				}
				typed, err := newPage(page, cql.OneColumn(cql.Int32), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer typed.Close()
				var value int32
				if !typed.NextInto(&value) || value != 1 {
					t.Fatalf("same connection wrong row: %v", typed.Err())
				}
				if typed.NextInto(&value) {
					t.Fatal("extra virtual row")
				}
				if err := typed.Err(); err != nil {
					t.Fatal(err)
				}
			})
		}
	})

	t.Run("RoutingKey", func(t *testing.T) {
		t.Parallel()
		skipUnlessSelectWithoutFrom(t)
		cases := []struct {
			name, stmt string
			keyspace   bool
			values     []cql.Value
		}{
			{name: "no bind markers", stmt: "SELECT 1"},
			{name: "no bind markers, with keyspace", stmt: "SELECT 1", keyspace: true},
			{name: "bind marker", stmt: "SELECT intAsBlob(?)", values: []cql.Value{cql.IntValue(42)}},
			{name: "bind marker, with keyspace", stmt: "SELECT intAsBlob(?)", keyspace: true, values: []cql.Value{cql.IntValue(42)}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var session *gocql.Session
				if tc.keyspace {
					session = createV2Session(t)
				} else {
					session = createSessionWithoutKeyspace(t)
				}
				defer session.Close()
				p, err := gocql.Prepare(context.Background(), session, cql.Statement[[]cql.Value, cql.DynamicRow]{CQL: tc.stmt, Params: cql.DynamicParams, Rows: cql.DynamicRows}, gocql.PrepareOptions{})
				if err != nil {
					t.Fatal(err)
				}
				b, err := BindOwned(p, tc.values)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { b.Close(); _ = b.Wait(context.Background()) }()
				// v2 resolves absent partition-key metadata directly. Legacy failed virtual-table
				// lookups are deliberately eliminated; no token may be invented in either case.
				if b.Routing().Token.Set {
					t.Fatal("no-FROM SELECT invented a partition token")
				}
				rows, err := Run(context.Background(), session, b, gocql.SessionOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				var row cql.DynamicRow
				if !rows.NextInto(&row) {
					t.Fatalf("query after routing failed: %v", rows.Err())
				}
				if rows.NextInto(&row) {
					t.Fatal("unexpected extra row")
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
			})
		}
	})

	t.Run("TokenAware", func(t *testing.T) {
		t.Parallel()
		skipUnlessSelectWithoutFrom(t)
		config := createV2Cluster()
		v2ConfigureRouting(t, config, routing.Options{TokenAware: true, ShuffleReplicas: true})
		session, err := gocql.Connect(context.Background(), mustBuild(t, config))
		if err != nil {
			t.Fatal(err)
		}
		defer session.Close()
		for _, tc := range []struct {
			name, stmt string
			values     []cql.Value
			want       int64
		}{{name: "nil routing key", stmt: "SELECT 1", want: 1}, {name: "routing lookup error", stmt: "SELECT blobAsInt(?)", values: []cql.Value{cql.BlobValue([]byte{0, 0, 0, 42})}, want: 42}} {
			t.Run(tc.name, func(t *testing.T) {
				for i := 0; i < 3; i++ {
					if got := noFromInt(t, noFromOne(t, session, tc.stmt, tc.values...), 0); got != tc.want {
						t.Fatalf("execution%d got%d want%d", i, got, tc.want)
					}
				}
			})
		}
	})

	t.Run("UDF", func(t *testing.T) {
		t.Parallel()
		skipUnlessSelectWithoutFrom(t)
		keyspaced := createV2Session(t)
		defer keyspaced.Close()
		const fn = "select_without_from_plus_one"
		stmt := fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s.%s(x int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE lua AS 'return x + 1'`, implCluster.SessionData(keyspaced).InternalCfg.Keyspace, fn)
		if err := v2CreateTable(context.Background(), keyspaced, stmt); err != nil {
			if isRejectedByServer(err) {
				t.Skipf("server refuses UDF creation: %v", err)
			}
			t.Fatal(err)
		}
		t.Run("unqualified without a session keyspace", func(t *testing.T) {
			s := createSessionWithoutKeyspace(t)
			defer s.Close()
			_, err := v2ReadOne(context.Background(), s, "SELECT "+fn+"(1)")
			if !isRejectedByServer(err) {
				t.Fatalf("unqualified UDF error=%v", err)
			}
		})
		t.Run("qualified without a session keyspace", func(t *testing.T) {
			s := createSessionWithoutKeyspace(t)
			defer s.Close()
			if noFromInt(t, noFromOne(t, s, "SELECT "+implCluster.SessionData(keyspaced).InternalCfg.Keyspace+"."+fn+"(1)"), 0) != 2 {
				t.Fatal("wrong qualified result")
			}
		})
		t.Run("unqualified with a session keyspace", func(t *testing.T) {
			if noFromInt(t, noFromOne(t, keyspaced, "SELECT "+fn+"(1)"), 0) != 2 {
				t.Fatal("wrong unqualified result")
			}
		})
	})
}
