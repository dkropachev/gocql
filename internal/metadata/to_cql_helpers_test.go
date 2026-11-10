/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package metadata

import (
	"math"
	"os"
	"slices"
	"strings"
	"testing"
)

func TestToCQLHelpers(t *testing.T) {
	t.Parallel()
	// Pins the rendering of a function body. $$...$$ has no escape,
	// so a body that could close it has to be quoted instead.
	t.Run("BodyLiteral", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name, body, want string
		}{
			// The common case must keep its $$ spelling, or every existing dump changes.
			{"an ordinary body keeps $$", "return 1", "$$return 1$$"},
			{"a body containing $$ is quoted", "return 1 --$$; DROP KEYSPACE ks; --",
				`'return 1 --$$; DROP KEYSPACE ks; --'`},
			// $$body$$ would let the terminator start one character early.
			{"a body ending in $ is quoted", "return 1 --$", `'return 1 --$'`},
			// Quoting is only safe if the quotes inside the body double.
			{"a quote in a quoted body doubles", "return '$$'", `'return ''$$'''`},
			// A single $ in the middle cannot close anything.
			{"a lone $ keeps $$", "return 1 -- $ x", "$$return 1 -- $ x$$"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				if got := cqlHelpers.bodyLiteral(tc.body); got != tc.want {
					t.Errorf("bodyLiteral(%q) = %q, want %q", tc.body, got, tc.want)
				}
			})
		}
	})
	// Pins the quoting rule. Quoting only what has to be quoted is the
	// point: an ordinary schema has to keep producing the output it always has, or
	// every dump in existence changes.
	t.Run("Ident", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct{ in, want string }{
			// Bare: a lower case letter, then letters, digits and underscores.
			{"t", "t"},
			{"my_table", "my_table"},
			{"t2", "t2"},
			{"a_b_2", "a_b_2"},
			// Punctuation cannot appear in a bare identifier.
			{"z-type", `"z-type"`},
			{"my ks", `"my ks"`},
			{"a.b", `"a.b"`},
			// A bare identifier is folded to lower case, so anything carrying
			// upper case has to be quoted to name the same thing back.
			{"MyTable", `"MyTable"`},
			{"T", `"T"`},
			// Neither a digit nor an underscore may lead: the grammar is
			// LETTER (LETTER | DIGIT | '_')*.
			{"2fast", `"2fast"`},
			{"_leading", `"_leading"`},
			{"_", `"_"`},
			// Reserved words are syntax wherever they appear.
			{"select", `"select"`},
			{"order", `"order"`},
			{"token", `"token"`},
			{"set", `"set"`},
			// Reserved in Scylla but not Cassandra, and reserved in Cassandra but
			// not Scylla. The set is the union, because a dump has to replay on
			// whichever server it came from.
			{"cast", `"cast"`},
			{"ann", `"ann"`},
			{"concurrency", `"concurrency"`},
			{"desc", `"desc"`},
			{"mbean", `"mbean"`},
			// Not keywords at all: BOOLEAN is lexed ahead of IDENT, so these can
			// never arrive as identifiers.
			{"true", `"true"`},
			{"false", `"false"`},
			// Not reserved, despite reading like CQL: each is admitted as an
			// identifier by Scylla's unreserved_keyword rules.
			{"comment", "comment"},
			{"type", "type"},
			{"key", "key"},
			{"keys", "keys"},
			{"clustering", "clustering"},
			{"static", "static"},
			{"partition", "partition"},
			// Native type names are unreserved too.
			{"text", "text"},
			{"timestamp", "timestamp"},
			// A quote inside the name doubles, as in CQL.
			{`a"b`, `"a""b"`},
			// Empty is not a legal bare identifier.
			{"", `""`},
		} {
			if got := cqlHelpers.ident(tc.in); got != tc.want {
				t.Errorf("ident(%q) = %q, want %q", tc.in, got, tc.want)
			}
		}
	})
	t.Run("Strings", func(t *testing.T) {
		t.Parallel()

		t.Run("escape", func(t *testing.T) {
			t.Parallel()

			for _, tc := range []struct {
				in   any
				want string
				// wantErr marks a type escape cannot render. It used to answer
				// "" for these, which the caller writes as `key = ` and the
				// server rejects a whole dump away from the cause.
				wantErr bool
			}{
				{in: "plain", want: "'plain'"},
				{in: "it's", want: "'it''s'"}, // the injection-relevant case
				{in: 42, want: "42"},
				{in: 1.5, want: "1.5"},
				{in: true, want: "true"},
				{in: false, want: "false"},
				// []byte is rejected, not inserted verbatim: the only caller
				// left is the replication map, where raw bytes let a value
				// close the map and forge further options.
				{in: []byte("raw"), wantErr: true},
				// escape renders every integer width and float32.
				{in: int64(42), want: "42"},
				{in: int32(42), want: "42"},
				{in: uint(42), want: "42"},
				{in: uint64(42), want: "42"},
				{in: float32(1.5), want: "1.5"},
				// A float32 must not be widened to render: fmt.Sprint of
				// float64(float32(0.1)) is 0.10000000149011612.
				{in: float32(0.1), want: "0.1"},
				// CQL spells the non-finite values Infinity, -Infinity and NaN.
				// Go prints the first two as +Inf and -Inf, neither of which the
				// grammar has -- it reads ('-')? (K_NAN | K_INFINITY), so there
				// is no Inf token and no leading + either.
				{in: math.Inf(1), want: "Infinity"},
				{in: math.Inf(-1), want: "-Infinity"},
				{in: float32(math.Inf(1)), want: "Infinity"},
				{in: float32(math.Inf(-1)), want: "-Infinity"},
				// NaN already matches K_NAN, which is case insensitive.
				{in: math.NaN(), want: "NaN"},
				{in: float32(math.NaN()), want: "NaN"},
				{in: struct{}{}, wantErr: true},
				{in: nil, wantErr: true},
				{in: []string{"a"}, wantErr: true},
			} {
				got, err := cqlHelpers.escape(tc.in)
				if tc.wantErr {
					if err == nil {
						t.Errorf("escape(%#v) = %q, nil; want an error", tc.in, got)
					}
					continue
				}
				if err != nil {
					t.Errorf("escape(%#v): %v", tc.in, err)
					continue
				}
				if got != tc.want {
					t.Errorf("escape(%#v) = %q, want %q", tc.in, got, tc.want)
				}
			}
		})

		t.Run("escapeString", func(t *testing.T) {
			t.Parallel()

			for _, tc := range []struct{ in, want string }{
				{"plain", "'plain'"},
				{"it's", "'it''s'"},
				{`a "b" c`, `'a "b" c'`},
				{"", "''"},
			} {
				if got := cqlHelpers.escapeString(tc.in); got != tc.want {
					t.Errorf("escapeString(%q) = %q, want %q", tc.in, got, tc.want)
				}
			}
		})

		t.Run("fixStrategy", func(t *testing.T) {
			t.Parallel()

			if got := cqlHelpers.fixStrategy("org.apache.cassandra.locator.SimpleStrategy"); got != "SimpleStrategy" {
				t.Errorf("fixStrategy = %q", got)
			}
		})

		t.Run("stripFrozen", func(t *testing.T) {
			t.Parallel()

			for _, tc := range []struct{ in, want string }{
				{"frozen<addr>", "addr"},
				{"frozen<tuple<int, double>>", "tuple<int, double>"},
				{"frozen<map<text, int>>", "map<text, int>"},
				// Not frozen-wrapped: the closing bracket used to come off
				// anyway, which corrupts every parameterised type.
				{"map<text, int>", "map<text, int>"},
				{"tuple<int, double>", "tuple<int, double>"},
				{"list<frozen<a>>", "list<frozen<a>>"},
				{"text", "text"},
				{"int", "int"},
				// Degenerate: a prefix with no matching bracket is left as it is
				// rather than half-unwrapped.
				{"frozen<unterminated", "frozen<unterminated"},
				{"", ""},
			} {
				if got := cqlHelpers.stripFrozen(tc.in); got != tc.want {
					t.Errorf("stripFrozen(%q) = %q, want %q", tc.in, got, tc.want)
				}
			}
		})

		t.Run("zip", func(t *testing.T) {
			t.Parallel()

			got := cqlHelpers.zip([]string{"a", "b"}, []string{"int", "text"})
			if len(got) != 2 || got[0][0] != "a" || got[0][1] != "int" || got[1][1] != "text" {
				t.Errorf("zip = %v", got)
			}
		})
	})
	t.Run("TableColumnToCQL", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name string
			tm   *TableMetadata
			want string
		}{
			{
				name: "single partition key is declared inline",
				tm: &TableMetadata{
					OrderedColumns: []string{"id", "v"},
					Columns: map[string]*ColumnMetadata{
						"id": col("id", "int", ColumnPartitionKey),
						"v":  col("v", "text", ColumnRegular),
					},
					PartitionKey: []*ColumnMetadata{col("id", "int", ColumnPartitionKey)},
				},
				want: "id int PRIMARY KEY,\n    v text",
			},
			{
				name: "static column carries the static keyword",
				tm: &TableMetadata{
					OrderedColumns: []string{"id", "c", "s"},
					Columns: map[string]*ColumnMetadata{
						"id": col("id", "int", ColumnPartitionKey),
						"c":  col("c", "int", ColumnClusteringKey),
						"s":  col("s", "text", ColumnStatic),
					},
					PartitionKey:      []*ColumnMetadata{col("id", "int", ColumnPartitionKey)},
					ClusteringColumns: []*ColumnMetadata{col("c", "int", ColumnClusteringKey)},
				},
				want: "s text static",
			},
			{
				name: "composite partition key moves to a PRIMARY KEY clause",
				tm: &TableMetadata{
					OrderedColumns: []string{"a", "b", "v"},
					Columns: map[string]*ColumnMetadata{
						"a": col("a", "int", ColumnPartitionKey),
						"b": col("b", "int", ColumnPartitionKey),
						"v": col("v", "text", ColumnRegular),
					},
					PartitionKey: []*ColumnMetadata{
						col("a", "int", ColumnPartitionKey),
						col("b", "int", ColumnPartitionKey),
					},
				},
				want: "PRIMARY KEY ((a, b))",
			},
			{
				name: "clustering columns follow the partition key",
				tm: &TableMetadata{
					OrderedColumns: []string{"a", "c", "d", "v"},
					Columns: map[string]*ColumnMetadata{
						"a": col("a", "int", ColumnPartitionKey),
						"c": col("c", "int", ColumnClusteringKey),
						"d": col("d", "int", ColumnClusteringKey),
						"v": col("v", "text", ColumnRegular),
					},
					PartitionKey: []*ColumnMetadata{col("a", "int", ColumnPartitionKey)},
					// Two clustering columns, so the separator arm of
					// partitionKeyString runs as well as the first-column arm.
					ClusteringColumns: []*ColumnMetadata{
						col("c", "int", ColumnClusteringKey),
						col("d", "int", ColumnClusteringKey),
					},
				},
				want: "PRIMARY KEY (a, c, d)",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				got := cqlHelpers.tableColumnToCQL(tc.tm)
				if !strings.Contains(got, tc.want) {
					t.Errorf("column clause is missing %q\n--- got ---\n%s", tc.want, got)
				}
			})
		}
	})
	t.Run("TableExtensionsToCQL", func(t *testing.T) {
		t.Parallel()

		t.Run("Renders", func(t *testing.T) {
			t.Parallel()

			blob, err := os.ReadFile("testdata/recreate/scylla_encryption_options.bin")
			if err != nil {
				t.Fatal(err)
			}

			t.Run("encryption options are decoded and rendered as CQL", func(t *testing.T) {
				t.Parallel()

				got, err := cqlHelpers.tableExtensionsToCQL(map[string][]byte{"scylla_encryption_options": blob})
				if err != nil {
					t.Fatalf("tableExtensionsToCQL: %v", err)
				}
				if len(got) != 1 {
					t.Fatalf("got %d properties, want 1: %q", len(got), got)
				}
				if !strings.HasPrefix(got[0], "scylla_encryption_options = ") {
					t.Errorf("unexpected property: %q", got[0])
				}
				// CQL quotes strings with ', so a surviving double quote is JSON that
				// was never converted and will not parse.
				if strings.Contains(got[0], `"`) {
					t.Errorf("double quotes survived into CQL: %s", got[0])
				}
			})

			t.Run("a quote in an option value cannot escape its string", func(t *testing.T) {
				t.Parallel()

				for _, tc := range []struct {
					name   string
					cipher string
					want   string
				}{
					// A single quote doubles. It used to pass straight through
					// json.Marshal and close the string it was inside, which is the
					// same injection as the table comment one function over.
					{"a single quote", "it's", `'cipher_algorithm': 'it''s'`},
					// A double quote is data. json.Marshal wrote it as \" and the
					// quote rewrite turned that into \', a backslash escape CQL does
					// not have.
					{"a double quote", `a"b`, `'cipher_algorithm': 'a"b'`},
					{"both", `a"b it's`, `'cipher_algorithm': 'a"b it''s'`},
				} {
					t.Run(tc.name, func(t *testing.T) {
						t.Parallel()

						got := cqlHelpers.encryptionOptionsToCQL(&scyllaEncryptionOptions{CipherAlgorithm: tc.cipher})
						if !strings.Contains(got, tc.want) {
							t.Errorf("encryptionOptionsToCQL = %s, want it to contain %s", got, tc.want)
						}
						if strings.Contains(got, `\'`) {
							t.Errorf("emitted a backslash escape, which CQL does not have: %s", got)
						}
					})
				}
			})

			t.Run("secret_key_strength stays a bare number", func(t *testing.T) {
				t.Parallel()

				got := cqlHelpers.encryptionOptionsToCQL(&scyllaEncryptionOptions{SecretKeyStrength: 128})
				if !strings.Contains(got, `'secret_key_strength': 128`) {
					t.Errorf("encryptionOptionsToCQL = %s, want an unquoted strength", got)
				}
			})

			t.Run("unknown extensions are dropped", func(t *testing.T) {
				t.Parallel()

				got, err := cqlHelpers.tableExtensionsToCQL(map[string][]byte{"something_else": []byte("x")})
				if err != nil {
					t.Fatalf("tableExtensionsToCQL: %v", err)
				}
				if len(got) != 0 {
					t.Errorf("unknown extension rendered as %q", got)
				}
			})

			t.Run("no extensions yields no properties", func(t *testing.T) {
				t.Parallel()

				got, err := cqlHelpers.tableExtensionsToCQL(nil)
				if err != nil {
					t.Fatalf("tableExtensionsToCQL: %v", err)
				}
				if len(got) != 0 {
					t.Errorf("got %q, want none", got)
				}
			})
		})
	})
	t.Run("TableOptionsToCQL", func(t *testing.T) {
		t.Parallel()

		// Pins the option values against a schema that
		// carries quotes. Both quote characters matter: a single quote has to double
		// so it stays inside the CQL string, and a double quote has to survive as
		// data. Rendering used to rewrite every double quote to a single one after
		// escaping, to re-quote marshalled JSON, which let a comment close its own
		// string and open a second property.
		t.Run("Escaping", func(t *testing.T) {
			t.Parallel()

			for _, tc := range []struct {
				name string
				ops  TableMetadataOptions
				want string
			}{
				{
					// The whole comment has to stay one option. Rendering used to
					// emit comment = 'x' AND gc_grace_seconds = 0 AND comment = 'y',
					// which replays as three properties, two of them forged.
					name: "a double quote in a comment stays data",
					ops:  TableMetadataOptions{Comment: `x" AND gc_grace_seconds = 0 AND comment = "y`},
					want: `comment = 'x" AND gc_grace_seconds = 0 AND comment = "y'`,
				},
				{
					name: "a single quote in a comment doubles",
					ops:  TableMetadataOptions{Comment: "it's"},
					want: `comment = 'it''s'`,
				},
				{
					name: "a map value is a CQL string, not JSON",
					ops:  TableMetadataOptions{Caching: map[string]string{"keys": "ALL"}},
					want: `caching = {'keys': 'ALL'}`,
				},
				{
					name: "a quote inside a map value doubles",
					ops:  TableMetadataOptions{Compaction: map[string]string{"class": "it's"}},
					want: `compaction = {'class': 'it''s'}`,
				},
				{
					name: "a quote inside a map key doubles",
					ops:  TableMetadataOptions{Compression: map[string]string{"it's": "x"}},
					want: `compression = {'it''s': 'x'}`,
				},
				{
					name: "map entries are ordered by key",
					ops:  TableMetadataOptions{Caching: map[string]string{"rows_per_partition": "NONE", "keys": "ALL"}},
					want: `caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}`,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					got := cqlHelpers.tableOptionsToCQL(tc.ops)
					if !slices.Contains(got, tc.want) {
						t.Errorf("missing option %q\n--- got ---\n%s", tc.want, strings.Join(got, "\n"))
					}
				})
			}
		})
		t.Run("ScyllaExtras", func(t *testing.T) {
			t.Parallel()

			// Options render as "name = value", so a prefix match is what identifies
			// one. Local to this test: nothing else in the package needs it, and the
			// test binary spans enough files that a name like this does not belong in
			// package scope.
			hasOption := func(in []string, prefix string) bool {
				return slices.ContainsFunc(in, func(s string) bool {
					return strings.HasPrefix(s, prefix)
				})
			}

			t.Run("cdc is emitted when set", func(t *testing.T) {
				t.Parallel()

				got := cqlHelpers.tableOptionsToCQL(TableMetadataOptions{CDC: map[string]string{"enabled": "true"}})
				if !hasOption(got, "cdc = ") {
					t.Errorf("cdc missing from %q", got)
				}
			})

			t.Run("cdc is omitted when unset", func(t *testing.T) {
				t.Parallel()

				got := cqlHelpers.tableOptionsToCQL(TableMetadataOptions{})
				if hasOption(got, "cdc = ") {
					t.Errorf("cdc emitted for a table that has none: %q", got)
				}
			})

			t.Run("in_memory is emitted only when true", func(t *testing.T) {
				t.Parallel()

				on := cqlHelpers.tableOptionsToCQL(TableMetadataOptions{InMemory: true})
				if !hasOption(on, "in_memory = ") {
					t.Errorf("in_memory missing from %q", on)
				}

				off := cqlHelpers.tableOptionsToCQL(TableMetadataOptions{})
				if hasOption(off, "in_memory = ") {
					t.Errorf("in_memory emitted for a non in-memory table: %q", off)
				}
			})
		})
	})
	t.Run("TablePropertiesToCQL", func(t *testing.T) {
		t.Parallel()

		opts := TableMetadataOptions{
			Comment:        "hi",
			GcGraceSeconds: 864000,
			Caching:        map[string]string{"keys": "ALL"},
			Compaction:     map[string]string{"class": "SizeTieredCompactionStrategy"},
			Compression:    map[string]string{"sstable_compression": "LZ4Compressor"},
		}

		t.Run("clustering order leads the property list", func(t *testing.T) {
			t.Parallel()

			cks := []*ColumnMetadata{{Name: "c", ClusteringOrder: "DESC"}}
			got, err := cqlHelpers.tablePropertiesToCQL(cks, opts, nil)
			if err != nil {
				t.Fatalf("tablePropertiesToCQL: %v", err)
			}
			if !strings.HasPrefix(got, "CLUSTERING ORDER BY (c DESC)") {
				t.Errorf("clustering order is not first\n--- got ---\n%s", got)
			}
			for _, want := range []string{"comment = 'hi'", "gc_grace_seconds = 864000", "caching = "} {
				if !strings.Contains(got, want) {
					t.Errorf("properties missing %q\n--- got ---\n%s", want, got)
				}
			}
		})

		t.Run("no clustering columns means no order clause", func(t *testing.T) {
			t.Parallel()

			got, err := cqlHelpers.tablePropertiesToCQL(nil, opts, nil)
			if err != nil {
				t.Fatalf("tablePropertiesToCQL: %v", err)
			}
			if strings.Contains(got, "CLUSTERING ORDER") {
				t.Errorf("emitted a clustering order for a table with none\n--- got ---\n%s", got)
			}
		})
	})
}
