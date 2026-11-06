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

package metadata_test

import (
	"testing"

	"github.com/scylladb/gocql/v2/metadata"
)

// The Equals family decides whether a schema refresh sees a table as changed
// (see refreshAllSchema). A comparison that silently ignores a field makes a
// real change invisible, so most of what follows is field-by-field drift
// detection rather than a handful of hand-written pairs: a new field on one of
// these structs fails the test until it is either compared or listed as
// deliberately ignored.
func TestMetadataEqualsAPI(t *testing.T) {
	t.Parallel()
	t.Run("Column", func(t *testing.T) {
		t.Parallel()

		t.Run("ComparesEveryField", func(t *testing.T) {
			t.Parallel()

			want := populatedColumnMetadata()
			if !want.Equals(populatedColumnMetadata()) {
				t.Fatal("Equals() = false for two identically populated values")
			}

			assertEveryFieldCompared(t, want, func(a, b any) bool {
				return a.(*metadata.Column).Equals(b.(*metadata.Column))
			})
		})
		// Pins a second field of the nested
		// index. The drift test above does reach Index -- differentValue recurses into
		// a struct -- but it stops at the first field it can mutate, and
		// ColumnIndexMetadata declares Options first, so Index.Name is never the field
		// it changes. This covers that one directly.
		t.Run("IncludesIndex", func(t *testing.T) {
			t.Parallel()

			a := populatedColumnMetadata()
			b := populatedColumnMetadata()
			b.Index.Name = "different-index"

			if a.Equals(b) {
				t.Error("Equals() = true for columns differing only in Index.Name")
			}
		})
	})
	t.Run("ColumnIndex", func(t *testing.T) {
		t.Parallel()

		// This is the drift guard for
		// ColumnIndexMetadata. TestMetadataEqualsAPI/ColumnIndex/Semantics below pins the specific
		// semantics (a renamed key, a composite value); this one only ensures no
		// exported field can be added and left out of the comparison unnoticed.
		t.Run("ComparesEveryField", func(t *testing.T) {
			t.Parallel()

			want := populatedColumnMetadata().Index
			other := populatedColumnMetadata().Index
			if !want.Equals(&other) {
				t.Fatal("Equals() = false for two identically populated values")
			}

			assertEveryFieldCompared(t, &want, func(a, b any) bool {
				return a.(*metadata.ColumnIndex).Equals(b.(*metadata.ColumnIndex))
			})
		})
		t.Run("Semantics", func(t *testing.T) {
			t.Parallel()

			base := func() *metadata.ColumnIndex {
				return &metadata.ColumnIndex{
					Name:    "idx",
					Type:    "COMPOSITES",
					Options: map[string]any{"target": "col", "nested": []string{"a", "b"}},
				}
			}

			tests := []struct {
				name   string
				mutate func(*metadata.ColumnIndex)
				want   bool
			}{
				{name: "identical", mutate: func(*metadata.ColumnIndex) {}, want: true},
				{name: "different name", mutate: func(c *metadata.ColumnIndex) { c.Name = "other" }},
				{name: "different type", mutate: func(c *metadata.ColumnIndex) { c.Type = "KEYS" }},
				{
					name:   "extra option",
					mutate: func(c *metadata.ColumnIndex) { c.Options["extra"] = "value" },
				},
				{
					name:   "missing option",
					mutate: func(c *metadata.ColumnIndex) { delete(c.Options, "target") },
				},
				{
					name:   "same key different value",
					mutate: func(c *metadata.ColumnIndex) { c.Options["target"] = "other" },
				},
				{
					// Options is map[string]any, so its values are compared with
					// reflect.DeepEqual rather than ==. A slice value must still be
					// compared by content.
					name:   "same key different slice value",
					mutate: func(c *metadata.ColumnIndex) { c.Options["nested"] = []string{"a", "c"} },
				},
				{
					// Equal length, disjoint keys: a length-only check would pass this.
					name: "same size different keys",
					mutate: func(c *metadata.ColumnIndex) {
						delete(c.Options, "target")
						c.Options["renamed"] = "col"
					},
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()

					a, b := base(), base()
					tt.mutate(b)

					if got := a.Equals(b); got != tt.want {
						t.Errorf("Equals() = %v, want %v", got, tt.want)
					}
				})
			}
		})
	})
	// Exercises compareInterfaceMaps on its own,
	// because its two callers reach it differently. TableMetadata.Equals checks the
	// lengths before calling, so the helper's own length check is redundant on that
	// path. refreshAllSchema does not: it compares a keyspace's StrategyOptions
	// through this helper with no guard of its own, which makes that length check
	// live logic -- it is what notices a replication option being added or removed,
	// and that decides whether the keyspace's tablets are dropped.
	t.Run("CompareInterfaceMaps", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			a, b map[string]any
			want bool
		}{
			{name: "both nil", a: nil, b: nil, want: true},
			{name: "nil versus empty", a: nil, b: map[string]any{}, want: true},
			{
				name: "identical",
				a:    map[string]any{"k": []byte("v")},
				b:    map[string]any{"k": []byte("v")},
				want: true,
			},
			{
				name: "different lengths",
				a:    map[string]any{"k": "v"},
				b:    map[string]any{"k": "v", "extra": "x"},
			},
			{
				name: "shorter receiver",
				a:    map[string]any{},
				b:    map[string]any{"k": "v"},
			},
			{
				name: "same length disjoint keys",
				a:    map[string]any{"k": "v"},
				b:    map[string]any{"other": "v"},
			},
			{
				name: "same key different value",
				a:    map[string]any{"k": "v"},
				b:    map[string]any{"k": "w"},
			},
			{
				// DeepEqual rather than ==, so composite values compare by content
				// and a byte slice that merely looks alike is still equal.
				name: "composite values compared by content",
				a:    map[string]any{"k": map[string]int{"n": 1}},
				b:    map[string]any{"k": map[string]int{"n": 1}},
				want: true,
			},
			{
				name: "composite values differing",
				a:    map[string]any{"k": map[string]int{"n": 1}},
				b:    map[string]any{"k": map[string]int{"n": 2}},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				if got := compareInterfaceMaps(tt.a, tt.b); got != tt.want {
					t.Errorf("compareInterfaceMaps() = %v, want %v", got, tt.want)
				}
			})
		}
	})
	// Pins the shared nil contract: two nils are
	// equal, a nil and a non-nil are not, and neither dereferences.
	t.Run("NilHandling", func(t *testing.T) {
		t.Parallel()

		t.Run("TableMetadataOptions", func(t *testing.T) {
			t.Parallel()

			var nilOpts *metadata.TableOptions
			populated := populatedTableMetadataOptions()

			if !nilOpts.Equals(nil) {
				t.Error("Equals() = false for two nils, want true")
			}
			if populated.Equals(nil) {
				t.Error("Equals(nil) = true for a populated receiver")
			}
			if nilOpts.Equals(populated) {
				t.Error("nil.Equals(populated) = true")
			}
		})

		t.Run("ColumnMetadata", func(t *testing.T) {
			t.Parallel()

			var nilCol *metadata.Column
			populated := populatedColumnMetadata()

			if !nilCol.Equals(nil) {
				t.Error("Equals() = false for two nils, want true")
			}
			if populated.Equals(nil) {
				t.Error("Equals(nil) = true for a populated receiver")
			}
			if nilCol.Equals(populated) {
				t.Error("nil.Equals(populated) = true")
			}
		})

		t.Run("ColumnIndexMetadata", func(t *testing.T) {
			t.Parallel()

			var nilIdx *metadata.ColumnIndex
			populated := &metadata.ColumnIndex{Name: "idx"}

			if !nilIdx.Equals(nil) {
				t.Error("Equals() = false for two nils, want true")
			}
			if populated.Equals(nil) {
				t.Error("Equals(nil) = true for a populated receiver")
			}
			if nilIdx.Equals(populated) {
				t.Error("nil.Equals(populated) = true")
			}
		})

		t.Run("TableMetadata", func(t *testing.T) {
			t.Parallel()

			var nilTable *metadata.Table
			populated := populatedTableMetadata()

			if !nilTable.Equals(nil) {
				t.Error("Equals() = false for two nils, want true")
			}
			if populated.Equals(nil) {
				t.Error("Equals(nil) = true for a populated receiver")
			}
			if nilTable.Equals(populated) {
				t.Error("nil.Equals(populated) = true")
			}
		})

		// A nil entry inside a compared slice must not panic either: the driver
		// builds these slices from server rows, and a gap leaves a nil behind.
		t.Run("nil column inside a slice", func(t *testing.T) {
			t.Parallel()

			a := populatedTableMetadata()
			b := deepCopyTableMetadata(a)
			b.PartitionKey[0] = nil

			if a.Equals(b) {
				t.Error("Equals() = true when one partition key column is nil")
			}
			if b.Equals(a) {
				t.Error("Equals() = true when the receiver's partition key column is nil")
			}
		})
	})
	t.Run("Table", func(t *testing.T) {
		t.Parallel()

		// This is the drift guard for
		// TableMetadata. TestMetadataEqualsAPI/Table/Semantics below pins the specific semantics
		// (a renamed column key, a reordered column list); this one only ensures no
		// exported field can be added and left out of the comparison unnoticed.
		t.Run("ComparesEveryField", func(t *testing.T) {
			t.Parallel()

			want := populatedTableMetadata()
			if !want.Equals(deepCopyTableMetadata(want)) {
				t.Fatal("Equals() = false for two identically populated values")
			}

			assertEveryFieldCompared(t, want, func(a, b any) bool {
				return a.(*metadata.Table).Equals(b.(*metadata.Table))
			})
		})
		// Covers the shapes a table can legitimately
		// have before any rows are compiled into it, which is when refreshAllSchema is
		// most likely to compare two of them.
		t.Run("EmptyAndZero", func(t *testing.T) {
			t.Parallel()

			if !(&metadata.Table{}).Equals(&metadata.Table{}) {
				t.Error("Equals() = false for two zero-valued tables")
			}

			nilMaps := &metadata.Table{Keyspace: "ks", Name: "tbl"}
			emptyMaps := &metadata.Table{
				Keyspace:   "ks",
				Name:       "tbl",
				Columns:    map[string]*metadata.Column{},
				Extensions: map[string]any{},
			}

			// A nil map and an empty map are both "no entries", and the length-based
			// guards treat them alike. Pinning this keeps a refresh from reporting a
			// change when the server simply returned no rows for a section.
			if !nilMaps.Equals(emptyMaps) {
				t.Error("Equals() = false for nil maps versus empty maps, want true")
			}
		})
		// Exercises the guards that make the
		// comparison safe as well as correct. The cases do not all do the same work.
		//
		// compareColumnSlices and compareStringSlices range over the receiver's slice
		// and index the argument's at the same offset, so they run off the end when the
		// receiver is the longer one. Dropping the length check ahead of any of them --
		// partition key, clustering columns, ordered columns, flags -- makes this test
		// panic rather than merely report unequal.
		//
		// The two map-backed cases are weaker, and are here for correctness rather than
		// for safety. compareColumnsMap cannot index past an end, so dropping the
		// columns guard is caught only because the shorter-receiver direction then
		// returns a wrong true. compareInterfaceMaps repeats the length check itself,
		// which makes the extensions guard redundant: dropping it changes no behaviour
		// and this test stays green. That case is kept because differing extension
		// counts must still compare unequal, not because it pins the guard.
		t.Run("LengthMismatch", func(t *testing.T) {
			t.Parallel()

			shorten := map[string]func(*metadata.Table){
				"partition key": func(m *metadata.Table) { m.PartitionKey = nil },
				"clustering columns": func(m *metadata.Table) {
					m.ClusteringColumns = nil
				},
				"columns":         func(m *metadata.Table) { delete(m.Columns, "pk") },
				"ordered columns": func(m *metadata.Table) { m.OrderedColumns = []string{"pk"} },
				"flags":           func(m *metadata.Table) { m.Flags = nil },
				"extensions":      func(m *metadata.Table) { m.Extensions = nil },
			}

			for name, shorter := range shorten {
				t.Run(name, func(t *testing.T) {
					t.Parallel()

					full := populatedTableMetadata()
					trimmed := deepCopyTableMetadata(full)
					shorter(trimmed)

					// compareColumnSlices and compareStringSlices range over the
					// receiver's slice and index the argument's at the same offset, so
					// the overrun needs the receiver to be the longer of the two:
					// full.Equals(trimmed) is the call that panics if a length guard is
					// dropped. The reverse direction cannot overrun, since it ranges
					// over the shorter slice; it is here so the guard is shown to
					// reject the mismatch either way round rather than only when the
					// receiver happens to be longer.
					if full.Equals(trimmed) {
						t.Error("Equals() = true for a longer receiver and shorter argument")
					}
					if trimmed.Equals(full) {
						t.Error("Equals() = true for a shorter receiver and longer argument")
					}
				})
			}
		})
		t.Run("Semantics", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name   string
				mutate func(*metadata.Table)
				want   bool
			}{
				{name: "identical", mutate: func(*metadata.Table) {}, want: true},
				{name: "keyspace", mutate: func(m *metadata.Table) { m.Keyspace = "other" }},
				{name: "name", mutate: func(m *metadata.Table) { m.Name = "other" }},
				{
					name:   "partition key column",
					mutate: func(m *metadata.Table) { m.PartitionKey[0].Name = "other" },
				},
				{
					name:   "clustering column",
					mutate: func(m *metadata.Table) { m.ClusteringColumns[0].Type = "int" },
				},
				{
					name:   "column map value",
					mutate: func(m *metadata.Table) { m.Columns["pk"].Type = "int" },
				},
				{
					// Same count, different key: the length guard passes and
					// compareColumnsMap has to do the work.
					name: "column map key renamed",
					mutate: func(m *metadata.Table) {
						m.Columns["renamed"] = m.Columns["pk"]
						delete(m.Columns, "pk")
					},
				},
				{
					name:   "ordered columns reordered",
					mutate: func(m *metadata.Table) { m.OrderedColumns = []string{"ck", "pk"} },
				},
				{name: "flags", mutate: func(m *metadata.Table) { m.Flags = []string{"dense"} }},
				{
					name:   "options",
					mutate: func(m *metadata.Table) { m.Options.GcGraceSeconds = 0 },
				},
				{
					name:   "extension value",
					mutate: func(m *metadata.Table) { m.Extensions["scylla_encryption_options"] = []byte("other") },
				},
				{
					name:   "extension added",
					mutate: func(m *metadata.Table) { m.Extensions["extra"] = []byte("x") },
				},
				{
					// Equal length, disjoint keys: compareInterfaceMaps has to notice
					// the missing key rather than stopping at the length check.
					name: "extension key renamed",
					mutate: func(m *metadata.Table) {
						m.Extensions["renamed"] = m.Extensions["scylla_encryption_options"]
						delete(m.Extensions, "scylla_encryption_options")
					},
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()

					a := populatedTableMetadata()
					b := deepCopyTableMetadata(a)
					tt.mutate(b)

					if got := a.Equals(b); got != tt.want {
						t.Errorf("Equals() = %v, want %v", got, tt.want)
					}
				})
			}
		})
	})
	t.Run("TableOptions", func(t *testing.T) {
		t.Parallel()

		t.Run("ComparesEveryField", func(t *testing.T) {
			t.Parallel()

			want := populatedTableMetadataOptions()
			if !want.Equals(populatedTableMetadataOptions()) {
				t.Fatal("Equals() = false for two identically populated values")
			}

			assertEveryFieldCompared(t, want, func(a, b any) bool {
				return a.(*metadata.TableOptions).Equals(b.(*metadata.TableOptions))
			})
		})
		// Covers what the drift test cannot: it
		// mutates a map by adding an entry, which changes the length, so the
		// length check in compareStringMaps short-circuits before the per-key
		// comparison. A key renamed in place keeps the length identical and is the
		// case that actually exercises the loop -- a compaction strategy swapped for
		// another with the same number of options looks exactly like this.
		t.Run("MapKeys", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name   string
				mutate func(*metadata.TableOptions)
				want   bool
			}{
				{name: "identical", mutate: func(*metadata.TableOptions) {}, want: true},
				{
					name: "caching key renamed",
					mutate: func(o *metadata.TableOptions) {
						o.Caching = map[string]string{"rows_per_partition": "ALL"}
					},
				},
				{
					name: "caching value changed",
					mutate: func(o *metadata.TableOptions) {
						o.Caching = map[string]string{"keys": "NONE"}
					},
				},
				{
					name: "compaction key renamed",
					mutate: func(o *metadata.TableOptions) {
						o.Compaction = map[string]string{"strategy": "SizeTieredCompactionStrategy"}
					},
				},
				{
					name:   "nil map versus populated",
					mutate: func(o *metadata.TableOptions) { o.CDC = nil },
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()

					a := populatedTableMetadataOptions()
					b := populatedTableMetadataOptions()
					tt.mutate(b)

					if got := a.Equals(b); got != tt.want {
						t.Errorf("Equals() = %v, want %v", got, tt.want)
					}
				})
			}
		})
	})
}
