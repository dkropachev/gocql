// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"slices"
	"strings"
	"testing"
)

func TestKeyspaceToCQL(t *testing.T) {
	t.Run("FunctionAndAggregateToCQL", func(t *testing.T) {
		t.Parallel()

		ks := &Keyspace{Name: "ks"}

		fn := &Function{
			Keyspace: "ks", Name: "avgfinal",
			ArgumentNames: []string{"state"}, ArgumentTypes: []string{"frozen<tuple<int, double>>"},
			ReturnType: "double", Language: "lua", Body: "return 1", CalledOnNullInput: true,
		}

		t.Run("function", func(t *testing.T) {
			t.Parallel()

			var sb strings.Builder
			if err := keyspaceMetadataFunctionToCQL(ks, &sb, "ks", fn); err != nil {
				t.Fatalf("functionToCQL: %v", err)
			}
			got := sb.String()
			for _, want := range []string{
				"CREATE FUNCTION ks.avgfinal",
				"state",
				// stripFrozen unwraps the argument type.
				"tuple<int, double>",
				"CALLED ON NULL INPUT",
				"RETURNS double",
				"LANGUAGE lua",
				"$$return 1$$",
			} {
				if !strings.Contains(got, want) {
					t.Errorf("missing %q\n--- got ---\n%s", want, got)
				}
			}
		})

		t.Run("function that returns null on null input", func(t *testing.T) {
			t.Parallel()

			strict := *fn
			strict.CalledOnNullInput = false
			var sb strings.Builder
			if err := keyspaceMetadataFunctionToCQL(ks, &sb, "ks", &strict); err != nil {
				t.Fatalf("functionToCQL: %v", err)
			}
			if got := sb.String(); !strings.Contains(got, "RETURNS NULL ON NULL INPUT") {
				t.Errorf("expected RETURNS NULL ON NULL INPUT\n--- got ---\n%s", got)
			}
		})

		// TestToCQLHelpers/BodyLiteral pins the rule; this pins the wiring. Without it the
		// template could go back to interpolating the body straight into $$...$$
		// and every other test would still pass.
		t.Run("a body that could close its literal is quoted", func(t *testing.T) {
			t.Parallel()

			for _, tc := range []struct {
				name, body, want, unsafe string
			}{
				{
					name:   "a body containing $$",
					body:   "return 1 --$$; DROP KEYSPACE ks; --",
					want:   `AS 'return 1 --$$; DROP KEYSPACE ks; --';`,
					unsafe: "$$return 1 --$$;",
				},
				{
					name:   "a body ending in $",
					body:   "return 1 --$",
					want:   `AS 'return 1 --$';`,
					unsafe: "$$return 1 --$$$;",
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					poisoned := *fn
					poisoned.Body = tc.body
					var sb strings.Builder
					if err := keyspaceMetadataFunctionToCQL(ks, &sb, "ks", &poisoned); err != nil {
						t.Fatalf("functionToCQL: %v", err)
					}
					got := sb.String()
					if !strings.Contains(got, tc.want) {
						t.Errorf("missing %q\n--- got ---\n%s", tc.want, got)
					}
					// The rendering where the body ends the literal itself, leaving
					// what follows to be parsed as CQL of its own.
					if strings.Contains(got, tc.unsafe) {
						t.Errorf("body closed its own literal: found %q\n--- got ---\n%s", tc.unsafe, got)
					}
				})
			}
		})

		t.Run("aggregate", func(t *testing.T) {
			t.Parallel()

			am := &Aggregate{
				Keyspace: "ks", Name: "average",
				ArgumentTypes: []string{"double"},
				ReturnType:    "double",
				StateType:     "frozen<tuple<int, double>>",
				InitCond:      "(0, 0)",
				StateFunc:     Function{Name: "avgstate"},
				FinalFunc:     Function{Name: "avgfinal"},
			}
			var sb strings.Builder
			if err := keyspaceMetadataAggregateToCQL(ks, &sb, am); err != nil {
				t.Fatalf("aggregateToCQL: %v", err)
			}
			got := sb.String()
			for _, want := range []string{
				"CREATE AGGREGATE ks.average(",
				"SFUNC avgstate",
				// The template applies stripFrozen to StateType, so this renders
				// "tuple<int, double>" where
				// testdata/recreate/aggregates_golden.cql has
				// "frozen<tuple<int, double>>". Both are valid CQL: a tuple is
				// intrinsically frozen, so the wrapper is redundant rather than
				// required. Checked against ScyllaDB 2026.3.0 -- it accepts either
				// spelling and echoes back the frozen one, which is why the golden,
				// captured from DESCRIBE, has it. A cosmetic divergence, not a
				// defect.
				"STYPE tuple<int, double>",
				"FINALFUNC avgfinal",
				"INITCOND (0, 0)",
			} {
				if !strings.Contains(got, want) {
					t.Errorf("missing %q\n--- got ---\n%s", want, got)
				}
			}
		})
	})
	t.Run("IndexToCQL", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name    string
			im      *Index
			want    string
			wantNot string
		}{
			{
				name: "plain target is emitted as-is",
				im: &Index{
					Name: "users_by_email", Keyspace: "ks", Table: "users",
					Options: map[string]string{"target": "email"},
				},
				want: "CREATE INDEX users_by_email ON ks.users (email);",
			},
			{
				name: "a JSON target is expanded into partition and clustering keys",
				im: &Index{
					Name: "idx", Keyspace: "ks", Table: "t",
					Options: map[string]string{"target": `{"pk":["a","b"],"ck":["c"]}`},
				},
				want: "CREATE INDEX idx ON ks.t ((a,b), c);",
			},
			{
				// The two spaces are the point: CREATE INDEX ON ks.t (...) is
				// legal, because the name is optional, while an empty quoted
				// identifier is a syntax error.
				name: "an index with no name stays unnamed",
				im: &Index{
					Keyspace: "ks", Table: "t",
					Options: map[string]string{"target": "col"},
				},
				want: "CREATE INDEX  ON ks.t (col);",
			},
			{
				name: "custom indexes are skipped -- Scylla does not support them",
				im: &Index{
					Name: "custom", Keyspace: "ks", Table: "t", Kind: IndexKindCustom,
					Options: map[string]string{"target": "x"},
				},
				wantNot: "CREATE INDEX",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				var sb strings.Builder
				ks := &Keyspace{Name: "ks"}
				if err := keyspaceMetadataIndexToCQL(ks, &sb, tc.im); err != nil {
					t.Fatalf("indexToCQL: %v", err)
				}
				got := sb.String()
				if tc.want != "" && !strings.Contains(got, tc.want) {
					t.Errorf("missing %q\n--- got ---\n%s", tc.want, got)
				}
				if tc.wantNot != "" && strings.Contains(got, tc.wantNot) {
					t.Errorf("emitted %q when it should have been skipped\n--- got ---\n%s", tc.wantNot, got)
				}
			})
		}
	})
	t.Run("KeyspaceToCQL", func(t *testing.T) {
		// Pins that a strategy option cannot close the replication map: a value
		// of `'1', 'dc_evil': '9` renders as one quoted string, not as a second
		// option.
		t.Run("QuotesOptionValues", func(t *testing.T) {
			t.Parallel()

			ks := &Keyspace{
				Name:          "ks",
				StrategyClass: "SimpleStrategy",
				StrategyOptions: map[string]string{
					"replication_factor": `'1', 'dc_evil': '9`,
				},
			}

			var sb strings.Builder
			if err := keyspaceMetadataKeyspaceToCQL(ks, &sb); err != nil {
				t.Fatalf("keyspaceToCQL: %v", err)
			}
			if want := `'replication_factor': '''1'', ''dc_evil'': ''9'`; !strings.Contains(sb.String(), want) {
				t.Errorf("keyspaceToCQL = %s, want it to contain %s", sb.String(), want)
			}
		})
		t.Run("Renders", func(t *testing.T) {
			t.Parallel()

			for _, tc := range []struct {
				name  string
				ks    *Keyspace
				wants []string
			}{
				{
					name: "network topology strategy is emitted unqualified",
					ks: &Keyspace{
						Name:            "ks",
						DurableWrites:   true,
						StrategyClass:   "org.apache.cassandra.locator.NetworkTopologyStrategy",
						StrategyOptions: map[string]string{"dc1": "3"},
					},
					// fixStrategy drops the org.apache.cassandra.locator. prefix.
					wants: []string{
						"CREATE KEYSPACE ks WITH replication = {",
						"'class': 'NetworkTopologyStrategy'",
						"'dc1': '3'",
						"durable_writes = true",
					},
				},
				{
					name: "durable_writes false is emitted explicitly",
					ks: &Keyspace{
						Name:            "ks",
						DurableWrites:   false,
						StrategyClass:   "SimpleStrategy",
						StrategyOptions: map[string]string{"replication_factor": "1"},
					},
					wants: []string{"'class': 'SimpleStrategy'", "durable_writes = false"},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					got, err := tc.ks.ToCQL()
					if err != nil {
						t.Fatalf("ToCQL: %v", err)
					}
					for _, want := range tc.wants {
						if !strings.Contains(got, want) {
							t.Errorf("output is missing %q\n--- got ---\n%s", want, got)
						}
					}
				})
			}
		})
		// A server's default for a keyspace that does not state its tablets
		// option may be either, so the fallback states it (scylladb/gocql#1055).
		t.Run("Tablets", func(t *testing.T) {
			t.Parallel()

			base := func() *Keyspace {
				return &Keyspace{
					Name:            "ks1",
					StrategyClass:   "org.apache.cassandra.locator.NetworkTopologyStrategy",
					StrategyOptions: map[string]string{"datacenter1": "2"},
					DurableWrites:   true,
				}
			}
			const head = "CREATE KEYSPACE ks1 WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '2'} AND durable_writes = true"
			for _, tc := range []struct {
				name string
				edit func(*Keyspace)
				want string
			}{
				{"Vnodes", func(*Keyspace) {}, head + ";\n"},
				{"VnodesOnTabletServer", func(ks *Keyspace) { ks.state.TabletsOption = true }, head + " AND tablets = {'enabled': false};\n"},
				{"Tablets", func(ks *Keyspace) { ks.TabletBased = true }, head + " AND tablets = {'enabled': true};\n"},
				{"InitialTablets", func(ks *Keyspace) { ks.TabletBased, ks.InitialTablets = true, 8 }, head + " AND tablets = {'initial': 8};\n"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					ks := base()
					tc.edit(ks)
					var sb strings.Builder
					if err := keyspaceMetadataKeyspaceToCQL(ks, &sb); err != nil {
						t.Fatal(err)
					}
					if sb.String() != tc.want {
						t.Fatalf("got  %q\nwant %q", sb.String(), tc.want)
					}
					// Clone keeps what the clause is rendered from.
					sb.Reset()
					if err := keyspaceMetadataKeyspaceToCQL(ks.Clone(), &sb); err != nil || sb.String() != tc.want {
						t.Fatalf("clone: %q, %v", sb.String(), err)
					}
				})
			}
		})
	})
	t.Run("ToCQL", func(t *testing.T) {
		t.Parallel()

		// Drives ToCQL's orchestration: every section
		// it emits -- keyspace, types, tables, indexes, functions, aggregates and views
		// -- plus the ordering constraints that make the output replayable.
		//
		// The per-section rendering is asserted separately above; what this adds is
		// that ToCQL actually reaches each loop, and in an order a server can replay.
		t.Run("GeneratesWholeKeyspace", func(t *testing.T) {
			t.Parallel()

			ks := &Keyspace{
				Name:            "ks",
				DurableWrites:   true,
				StrategyClass:   "SimpleStrategy",
				StrategyOptions: map[string]string{"replication_factor": "1"},
				Types: map[string]*Type{
					"addr": {Keyspace: "ks", Name: "addr", FieldNames: []string{"street", "zip"}, FieldTypes: []string{"text", "int"}},
				},
				Tables: map[string]*Table{
					"users": {
						Name:           "users",
						OrderedColumns: []string{"id", "home"},
						Columns: map[string]*Column{
							"id":   col("id", "uuid", ColumnPartitionKey),
							"home": col("home", "frozen<addr>", ColumnRegular),
						},
						PartitionKey: []*Column{col("id", "uuid", ColumnPartitionKey)},
						Options:      TableOptions{GCGraceSeconds: 864000},
					},
				},
				Indexes: map[string]*Index{
					"users_by_home": {
						Name: "users_by_home", Keyspace: "ks", Table: "users",
						Options: map[string]string{"target": "home"},
					},
				},
				Functions: map[string]*Function{
					"fsum": {
						Keyspace: "ks", Name: "fsum",
						ArgumentNames: []string{"a"}, ArgumentTypes: []string{"int"},
						ReturnType: "int", Language: "lua", Body: "return a", CalledOnNullInput: true,
					},
				},
				Aggregates: map[string]*Aggregate{
					"total": {
						Keyspace: "ks", Name: "total",
						ArgumentTypes: []string{"int"}, ReturnType: "int", StateType: "int",
						InitCond:  "0",
						StateFunc: Function{Name: "fsum"},
					},
				},
				Views: map[string]*View{
					"users_by_zip": {
						Keyspace: "ks", Name: "users_by_zip", BaseTable: "users",
						WhereClause:    "home IS NOT NULL",
						OrderedColumns: []string{"id", "home"},
						PartitionKey:   []*Column{col("id", "uuid", ColumnPartitionKey)},
					},
				},
			}

			got, err := ks.ToCQL()
			if err != nil {
				t.Fatalf("ToCQL: %v", err)
			}

			for _, want := range []string{
				"CREATE KEYSPACE ks WITH replication = {",
				"CREATE TYPE ks.addr",
				"street text",
				"CREATE TABLE ks.users",
				"id uuid PRIMARY KEY",
				"home frozen<addr>",
				"gc_grace_seconds = 864000",
				"CREATE INDEX users_by_home ON ks.users (home);",
				"CREATE FUNCTION ks.fsum",
				"CREATE AGGREGATE ks.total(",
				"CREATE MATERIALIZED VIEW ks.users_by_zip AS",
			} {
				if !strings.Contains(got, want) {
					t.Errorf("generated schema is missing %q\n--- got ---\n%s", want, got)
				}
			}

			// Ordering constraints a server enforces on replay.
			for _, ord := range []struct{ first, then, why string }{
				{"CREATE TYPE ks.addr", "CREATE TABLE ks.users", "a type must precede the table using it"},
				{"CREATE TABLE ks.users", "CREATE INDEX users_by_home", "an index needs its base table"},
				{"CREATE TABLE ks.users", "CREATE MATERIALIZED VIEW ks.users_by_zip", "a view needs its base table"},
				{"CREATE FUNCTION ks.fsum", "CREATE AGGREGATE ks.total(", "an aggregate needs its state function"},
			} {
				// Index returns -1 for a statement that was never emitted, and -1 is
				// less than any real offset, so comparing the two alone would let a
				// missing first silently satisfy the constraint.
				i, j := strings.Index(got, ord.first), strings.Index(got, ord.then)
				if i < 0 {
					t.Errorf("%q was never emitted, so %s cannot be checked\n--- got ---\n%s", ord.first, ord.why, got)
					continue
				}
				if j < 0 {
					t.Errorf("%q was never emitted, so %s cannot be checked\n--- got ---\n%s", ord.then, ord.why, got)
					continue
				}
				if i > j {
					t.Errorf("%q comes after %q: %s\n--- got ---\n%s", ord.first, ord.then, ord.why, got)
				}
			}
		})
		// Pins that a regenerated dump does not reorder
		// itself between runs. ToCQL walks six collections, five of them maps, and Go
		// randomises map iteration -- before they were ordered, a keyspace with three
		// tables produced three different outputs over 300 calls.
		//
		// Several entities per category matter here: with one apiece there is nothing
		// to permute, so a fixture that size cannot see the problem.
		t.Run("IsDeterministic", func(t *testing.T) {
			t.Parallel()

			build := func() *Keyspace {
				ks := &Keyspace{
					Name:            "ks",
					DurableWrites:   true,
					StrategyClass:   "SimpleStrategy",
					StrategyOptions: map[string]string{"replication_factor": "1"},
					Types:           map[string]*Type{},
					Tables:          map[string]*Table{},
					Indexes:         map[string]*Index{},
					Functions:       map[string]*Function{},
					Aggregates:      map[string]*Aggregate{},
					Views:           map[string]*View{},
				}
				for _, n := range []string{"a_t", "b_t", "c_t"} {
					ks.Types[n] = &Type{Keyspace: "ks", Name: n, FieldNames: []string{"f"}, FieldTypes: []string{"int"}}
					ks.Tables[n] = &Table{
						Name: n, OrderedColumns: []string{"id"},
						Columns:      map[string]*Column{"id": col("id", "uuid", ColumnPartitionKey)},
						PartitionKey: []*Column{col("id", "uuid", ColumnPartitionKey)},
					}
					ks.Indexes[n+"_idx"] = &Index{
						Name: n + "_idx", Keyspace: "ks", Table: n,
						Options: map[string]string{"target": "id"},
					}
					ks.Functions[n+"_fn"] = &Function{
						Keyspace: "ks", Name: n + "_fn",
						ArgumentNames: []string{"x"}, ArgumentTypes: []string{"int"},
						ReturnType: "int", Language: "lua", Body: "return x", CalledOnNullInput: true,
					}
					ks.Aggregates[n+"_agg"] = &Aggregate{
						Keyspace: "ks", Name: n + "_agg",
						ArgumentTypes: []string{"int"}, ReturnType: "int", StateType: "int",
						InitCond: "0", StateFunc: Function{Name: n + "_fn"},
					}
					ks.Views[n+"_v"] = &View{
						Keyspace: "ks", Name: n + "_v", BaseTable: n,
						WhereClause: "id IS NOT NULL", OrderedColumns: []string{"id"},
						PartitionKey: []*Column{col("id", "uuid", ColumnPartitionKey)},
					}
				}
				return ks
			}

			// A fresh keyspace each round, so every map is built anew.
			const rounds = 100
			var first string
			for i := 0; i < rounds; i++ {
				got, err := build().ToCQL()
				if err != nil {
					t.Fatalf("round %d: ToCQL: %v", i, err)
				}
				if i == 0 {
					first = got
					continue
				}
				if got != first {
					t.Fatalf("round %d differs from round 0 -- the dump reorders itself between runs", i)
				}
			}
		})
		// Walks every statement ToCQL emits with a name
		// that has to be quoted in it. Each of these used to render bare, and the
		// server answers a SyntaxException to the result.
		t.Run("QuotesIdentifiers", func(t *testing.T) {
			t.Parallel()

			ks := &Keyspace{
				Name:            "my-ks",
				StrategyClass:   "SimpleStrategy",
				StrategyOptions: map[string]string{"replication_factor": "1"},
				Types: map[string]*Type{
					"z-type": {Keyspace: "my-ks", Name: "z-type", FieldNames: []string{"order"}, FieldTypes: []string{"int"}},
				},
				Tables: map[string]*Table{
					"my-tbl": {
						Keyspace:          "my-ks",
						Name:              "my-tbl",
						PartitionKey:      []*Column{{Name: "pk", Type: "int"}},
						ClusteringColumns: []*Column{{Name: "Order", Type: "int", ClusteringOrder: ColumnOrderDesc}},
						OrderedColumns:    []string{"pk", "Order", "select"},
						Columns: map[string]*Column{
							"pk":     {Name: "pk", Type: "int", Kind: ColumnPartitionKey},
							"Order":  {Name: "Order", Type: "int", Kind: ColumnClusteringKey},
							"select": {Name: "select", Type: "text", Kind: ColumnRegular},
						},
					},
				},
				Indexes: map[string]*Index{
					"my-idx": {
						Name: "my-idx", Keyspace: "my-ks", Table: "my-tbl",
						Options: map[string]string{"target": `{"pk":["pk"],"ck":["Order"]}`},
					},
				},
				Views: map[string]*View{
					"my-view": {
						Keyspace: "my-ks", Name: "my-view", BaseTable: "my-tbl",
						WhereClause:    `"Order" IS NOT NULL`,
						OrderedColumns: []string{"pk", "Order"},
						PartitionKey:   []*Column{{Name: "pk"}},
					},
				},
				Functions: map[string]*Function{
					"my-fn": {
						Keyspace: "my-ks", Name: "my-fn",
						ArgumentNames: []string{"select"}, ArgumentTypes: []string{"int"},
						ReturnType: "int", Language: "lua", Body: "return 1",
					},
				},
				Aggregates: map[string]*Aggregate{
					"my-agg": {
						Keyspace: "my-ks", Name: "my-agg", ArgumentTypes: []string{"int"},
						StateFunc: Function{Name: "my-fn"}, StateType: "int",
						FinalFunc: Function{Name: "my-final"},
					},
				},
			}

			got, err := ks.ToCQL()
			if err != nil {
				t.Fatalf("ToCQL: %v", err)
			}

			for _, want := range []string{
				`CREATE KEYSPACE "my-ks" WITH`,
				`CREATE TYPE "my-ks"."z-type" (`,
				`"order" int`,
				`CREATE TABLE "my-ks"."my-tbl" (`,
				`"Order" int`,
				`"select" text`,
				`PRIMARY KEY (pk, "Order")`,
				`CLUSTERING ORDER BY ("Order" DESC)`,
				`CREATE INDEX "my-idx" ON "my-ks"."my-tbl" ((pk), "Order")`,
				`CREATE MATERIALIZED VIEW "my-ks"."my-view" AS`,
				`FROM "my-ks"."my-tbl"`,
				`CREATE FUNCTION "my-ks"."my-fn" (`,
				`CREATE AGGREGATE "my-ks"."my-agg"(`,
				`SFUNC "my-fn"`,
				`FINALFUNC "my-final"`,
			} {
				if !strings.Contains(got, want) {
					t.Errorf("missing %q\n--- got ---\n%s", want, got)
				}
			}

			// A name that needs no quotes must not gain any, or every existing dump
			// changes.
			for _, unwanted := range []string{`"pk"`, `"int"`, `"lua"`} {
				if strings.Contains(got, unwanted) {
					t.Errorf("quoted %s, which needs no quotes\n--- got ---\n%s", unwanted, got)
				}
			}
		})
		// Pins the short-circuit itself: a
		// keyspace that carries the server's DESCRIBE output gets it back untouched, and the
		// generator is not consulted. This is the branch that hides all the others.
		t.Run("UsesServerStatements", func(t *testing.T) {
			t.Parallel()

			ks := &Keyspace{
				Name: "ks",
				// Deliberately inconsistent with the server CQL: if the generator ran, the
				// output would mention this table.
				Tables: map[string]*Table{"t": {
					Name:           "t",
					OrderedColumns: []string{"id"},
					Columns:        map[string]*Column{"id": col("id", "int", ColumnPartitionKey)},
					PartitionKey:   []*Column{col("id", "int", ColumnPartitionKey)},
				}},
			}

			ks.state.ServerCQL = "CREATE KEYSPACE server_said_so;"

			got, err := ks.ToCQL()
			if err != nil {
				t.Fatalf("ToCQL: %v", err)
			}
			if got != "CREATE KEYSPACE server_said_so;" {
				t.Errorf("ToCQL returned %q, want the server CQL verbatim", got)
			}
			if strings.Contains(got, "CREATE TABLE") {
				t.Error("the generator ran even though the server CQL was present")
			}
		})
	})
	t.Run("TypesSortedTopologically", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name  string
			types []*Type
			// want, when set, is the exact order expected. Most cases only assert
			// the invariant, because several orders are equally valid; the
			// collision cases have one correct answer.
			want []string
		}{
			{
				name: "one dependency edge",
				types: []*Type{
					{Keyspace: "ks", Name: "inner", FieldNames: []string{"a"}, FieldTypes: []string{"int"}},
					{Keyspace: "ks", Name: "outer", FieldNames: []string{"b"}, FieldTypes: []string{"frozen<inner>"}},
				},
				want: []string{"inner", "outer"},
			},
			{
				// A single edge is settled by one comparison, so it cannot show
				// whether the ordering is a real traversal. A chain needs the
				// relation to compose: inner must precede outer even though
				// nothing states that directly.
				name: "three-level chain",
				types: []*Type{
					{Keyspace: "ks", Name: "inner", FieldNames: []string{"a"}, FieldTypes: []string{"int"}},
					{Keyspace: "ks", Name: "middle", FieldNames: []string{"b"}, FieldTypes: []string{"frozen<inner>"}},
					{Keyspace: "ks", Name: "outer", FieldNames: []string{"c"}, FieldTypes: []string{"frozen<middle>"}},
				},
				want: []string{"inner", "middle", "outer"},
			},
			{
				// "varchar" contains "var". Matching substrings would read that
				// as a dependency of a on var, closing a cycle with the real
				// var -> a edge and emitting var before the a it embeds.
				name: "a type name that is a substring of a built-in",
				types: []*Type{
					{Keyspace: "ks", Name: "a", FieldNames: []string{"s"}, FieldTypes: []string{"varchar"}},
					{Keyspace: "ks", Name: "var", FieldNames: []string{"x"}, FieldTypes: []string{"frozen<a>"}},
				},
				want: []string{"a", "var"},
			},
			{
				// Same collision between two user types rather than with a
				// built-in: "inner" contains "in".
				name: "a type name that is a substring of another type name",
				types: []*Type{
					{Keyspace: "ks", Name: "in", FieldNames: []string{"p"}, FieldTypes: []string{"frozen<inner>"}},
					{Keyspace: "ks", Name: "inner", FieldNames: []string{"q"}, FieldTypes: []string{"int"}},
				},
				want: []string{"inner", "in"},
			},
			{
				// system_schema.types keys a UDT by its unquoted name but stores
				// the reference with its quotes: "z-type" is keyed z-type and
				// referenced as frozen<"z-type">. Splitting on punctuation would
				// yield "z" and "type" and drop the dependency.
				name: "a quoted type name containing punctuation",
				types: []*Type{
					{Keyspace: "ks", Name: "holder", FieldNames: []string{"x"}, FieldTypes: []string{`frozen<"z-type">`}},
					{Keyspace: "ks", Name: "z-type", FieldNames: []string{"a"}, FieldTypes: []string{"int"}},
				},
				want: []string{"z-type", "holder"},
			},
			{
				// A user type may be named after a built-in, quoted at creation.
				// A bare "text" field is then the built-in, not a reference to
				// that type: following it would close a cycle with the real
				// text -> t2 edge and emit text before the t2 it embeds.
				name: "a user type named after a built-in",
				types: []*Type{
					{Keyspace: "ks", Name: "text", FieldNames: []string{"x"}, FieldTypes: []string{"frozen<t2>"}},
					{Keyspace: "ks", Name: "t2", FieldNames: []string{"p"}, FieldTypes: []string{"text"}},
				},
				want: []string{"t2", "text"},
			},
			{
				// The other direction: frozen<text> is a genuine reference to that
				// user type -- frozen never wraps a scalar built-in -- so the edge
				// must still be followed even though the bare text beside it is not.
				name: "a built-in and a user type of the same name in one field list",
				types: []*Type{
					{Keyspace: "ks", Name: "text", FieldNames: []string{"a"}, FieldTypes: []string{"int"}},
					{Keyspace: "ks", Name: "holder", FieldNames: []string{"p", "q"}, FieldTypes: []string{"text", "frozen<text>"}},
				},
				want: []string{"text", "holder"},
			},
			{
				// frozen<map<text,int>> is a frozen collection, not a reference to
				// a type named map. Treating the operand of frozen as a user type
				// without checking for a constructor would invent an edge here.
				name: "a frozen collection beside a user type named after its constructor",
				types: []*Type{
					{Keyspace: "ks", Name: "map", FieldNames: []string{"x"}, FieldTypes: []string{"frozen<other>"}},
					{Keyspace: "ks", Name: "other", FieldNames: []string{"y"}, FieldTypes: []string{"frozen<map<text,int>>"}},
				},
				want: []string{"other", "map"},
			},
			{
				// The other direction: a constructor name written without
				// arguments is a genuine reference, and the server stores it
				// unquoted, so the edge must still be followed.
				name: "a user type named after a constructor, actually referenced",
				types: []*Type{
					{Keyspace: "ks", Name: "map", FieldNames: []string{"a"}, FieldTypes: []string{"int"}},
					{Keyspace: "ks", Name: "holder", FieldNames: []string{"m"}, FieldTypes: []string{"frozen<map>"}},
				},
				want: []string{"map", "holder"},
			},
			{
				// A custom marshal type is a dotted Java class name. Tokenising it
				// would invent edges to any user type sharing one of its segments.
				name: "a custom marshal type beside a user type named after its package",
				types: []*Type{
					{Keyspace: "ks", Name: "org", FieldNames: []string{"x"}, FieldTypes: []string{"frozen<other>"}},
					{Keyspace: "ks", Name: "other", FieldNames: []string{"y"}, FieldTypes: []string{`'org.apache.cassandra.db.marshal.UTF8Type'`}},
				},
				want: []string{"other", "org"},
			},
			{
				name: "one type embedded by two others",
				types: []*Type{
					{Keyspace: "ks", Name: "shared", FieldNames: []string{"a"}, FieldTypes: []string{"int"}},
					{Keyspace: "ks", Name: "left", FieldNames: []string{"b"}, FieldTypes: []string{"frozen<shared>"}},
					{Keyspace: "ks", Name: "right", FieldNames: []string{"c"}, FieldTypes: []string{"list<frozen<shared>>"}},
				},
				// shared sorts last by name, so this order can only come from the
				// dependency walk -- unlike the two cases above, where the correct
				// order happens to be alphabetical.
				want: []string{"shared", "left", "right"},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				// typesSortedTopologically reads ks.Types, a map, so the order it
				// starts from varies per call. Sample it rather than relying on
				// one draw: with two types, one of the two possible orders is
				// already correct, so a single round would let a sorter that does
				// nothing through about half the time.
				const rounds = 50
				var first []string

				for i := 0; i < rounds; i++ {
					ks := &Keyspace{Types: map[string]*Type{}}
					for _, tm := range tc.types {
						ks.Types[tm.Name] = tm
					}

					sorted := keyspaceMetadataTypesSortedTopologically(ks)
					if len(sorted) != len(tc.types) {
						t.Fatalf("round %d: got %d types, want %d", i, len(sorted), len(tc.types))
					}

					pos := make(map[string]int, len(sorted))
					order := make([]string, 0, len(sorted))
					for j, tm := range sorted {
						pos[tm.Name] = j
						order = append(order, tm.Name)
					}

					// No type may be declared before one it embeds. Dependencies
					// are read the same way the sorter reads them -- whole CQL
					// identifiers -- so that a name like "var" inside "varchar" is
					// not mistaken for one. The exact orders asserted by the
					// collision cases below check that matching independently.
					for _, tm := range sorted {
						for _, ft := range tm.FieldTypes {
							for _, ref := range cqlTypeIdentifiers(ft) {
								if !ref.namesUserType() {
									continue
								}
								at, isType := pos[ref.name]
								if isType && ref.name != tm.Name && at > pos[tm.Name] {
									t.Fatalf("round %d: %s embeds %s but is declared first: %v",
										i, tm.Name, ref.name, order)
								}
							}
						}
					}

					if tc.want != nil && !slices.Equal(order, tc.want) {
						t.Fatalf("round %d: got order %v, want %v", i, order, tc.want)
					}

					// The output must not depend on map iteration order either, or
					// a regenerated schema dump would churn between runs.
					if i == 0 {
						first = order
					} else if !slices.Equal(order, first) {
						t.Fatalf("round %d produced %v, round 0 produced %v -- order is not deterministic",
							i, order, first)
					}
				}
			})
		}
	})
	t.Run("ViewToCQL", func(t *testing.T) {
		t.Parallel()

		base := &Keyspace{Name: "ks"}

		t.Run("named columns are listed", func(t *testing.T) {
			t.Parallel()

			vm := &View{
				Keyspace: "ks", Name: "v", BaseTable: "t",
				WhereClause:    "id IS NOT NULL",
				OrderedColumns: []string{"id", "name"},
				PartitionKey:   []*Column{col("id", "int", ColumnPartitionKey)},
			}
			var sb strings.Builder
			if err := keyspaceMetadataViewToCQL(base, &sb, vm); err != nil {
				t.Fatalf("viewToCQL: %v", err)
			}
			got := sb.String()
			for _, want := range []string{
				"CREATE MATERIALIZED VIEW ks.v AS",
				"FROM ks.t",
				"WHERE id IS NOT NULL",
				"name",
			} {
				if !strings.Contains(got, want) {
					t.Errorf("missing %q\n--- got ---\n%s", want, got)
				}
			}
		})

		t.Run("IncludeAllColumns selects star", func(t *testing.T) {
			t.Parallel()

			vm := &View{
				Keyspace: "ks", Name: "v", BaseTable: "t",
				WhereClause: "id IS NOT NULL", IncludeAllColumns: true,
				OrderedColumns: []string{"id"},
				PartitionKey:   []*Column{col("id", "int", ColumnPartitionKey)},
			}
			var sb strings.Builder
			if err := keyspaceMetadataViewToCQL(base, &sb, vm); err != nil {
				t.Fatalf("viewToCQL: %v", err)
			}
			if got := sb.String(); !strings.Contains(got, "SELECT *") {
				t.Errorf("expected SELECT *\n--- got ---\n%s", got)
			}
		})
	})
}
