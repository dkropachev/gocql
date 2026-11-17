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
	"fmt"
	"slices"
	"testing"
)

func TestCQLTypeRef(t *testing.T) {
	t.Parallel()
	t.Run("Identifiers", func(t *testing.T) {
		t.Parallel()

		// Checks the classification the
		// sorter actually depends on, on parsed input rather than hand-built refs.
		// TestCQLTypeRef/Identifiers/Names only compares names, so it cannot see a constructor
		// being mistaken for a user type.
		t.Run("ClassifiesParsedTypes", func(t *testing.T) {
			t.Parallel()

			for _, tc := range []struct {
				in   string
				name string
				want bool
			}{
				{"frozen<addr>", "addr", true},
				{"frozen<addr>", "frozen", false},
				{"frozen<list<text>>", "list", false},
				{"frozen<map>", "map", true},
				{"vector<float, 3>", "vector", false},
				{"vector<float, 3>", "float", false},
				{`frozen<"z-type">`, "z-type", true},
				// Whitespace between a constructor and its arguments must not stop it
				// reading as parameterised, or the constructor is offered as a
				// dependency in its own right.
				{"frozen <addr>", "frozen", false},
				{"frozen\t<addr>", "frozen", false},
				{"frozen\n<addr>", "frozen", false},
				{"map <text, int>", "map", false},
				// Malformed input must not corrupt what follows: the literal consumes
				// the operand slot that frozen< opened, so the text after it is the
				// built-in, not a user type inheriting a stale frozen.
				{"frozen<3>text", "text", false},
				// Same for a custom type: the literal consumes the operand slot, so
				// what follows is not read as the thing frozen was applied to.
				{`frozen<'x'>text`, "text", false},
				// A bare frozen opens no operand slot, so the token after it is read on
				// its own merits -- int stays the built-in.
				{"map<frozen, int>", "int", false},
			} {
				t.Run(fmt.Sprintf("%q/%s", tc.in, tc.name), func(t *testing.T) {
					t.Parallel()

					found := false
					for _, ref := range cqlTypeIdentifiers(tc.in) {
						if ref.name != tc.name {
							continue
						}
						found = true
						if got := ref.namesUserType(); got != tc.want {
							t.Errorf("namesUserType(%s) in %q = %v, want %v", tc.name, tc.in, got, tc.want)
						}
					}
					if !found {
						t.Fatalf("%q did not yield an identifier %q", tc.in, tc.name)
					}
				})
			}
		})
		t.Run("Names", func(t *testing.T) {
			t.Parallel()

			for _, tc := range []struct {
				// name, not in, because two inputs differing only in whitespace
				// rewrite to the same subtest name, and the empty input has none.
				name string
				in   string
				want []string
			}{
				{"a native type", "int", []string{"int"}},
				{"a frozen user type", "frozen<addr>", []string{"frozen", "addr"}},
				{"a user type inside a collection", "map<text, frozen<addr>>", []string{"map", "text", "frozen", "addr"}},
				{"a tuple", "tuple<int, double>", []string{"tuple", "int", "double"}},
				// The dimension is a literal, not a type: no identifier starts with a
				// digit, so it must not be offered as a dependency.
				{"a vector dimension", "vector<float, 3>", []string{"vector", "float"}},
				// Whitespace may separate a constructor from its arguments; it still
				// has to read as parameterised, or the constructor itself would be
				// offered as a dependency.
				{"a space before the arguments", "frozen <addr>", []string{"frozen", "addr"}},
				{"a tab before the arguments", "frozen\t<addr>", []string{"frozen", "addr"}},
				// A single-quoted custom marshal type is a Java class name, not a path
				// of UDT names: descending into it would offer org, apache and so on.
				{"a custom marshal type", `'org.apache.cassandra.db.marshal.UTF8Type'`, nil},
				{"a custom marshal type as the frozen operand", `frozen<'org.apache.cassandra.db.marshal.UTF8Type'>`, []string{"frozen"}},
				{"a custom marshal type inside a collection", `map<text, 'org.apache.cassandra.db.marshal.Int32Type'>`, []string{"map", "text"}},
				{"a doubled quote inside a single-quoted literal", `'it''s'`, nil},
				{"an unterminated single-quoted literal", `'unterminated`, nil},
				{"a frozen vector", "frozen<vector<float, 1024>>", []string{"frozen", "vector", "float"}},
				// "var" must not fall out of "varchar".
				{"a native type containing a shorter name", "varchar", []string{"varchar"}},
				// A quoted name is one identifier, punctuation and all.
				{"a quoted name as the frozen operand", `frozen<"z-type">`, []string{"frozen", "z-type"}},
				{"a quoted name alone", `"z-type"`, []string{"z-type"}},
				{"quoted names containing punctuation and a space", `map<"a-b", frozen<"c d">>`, []string{"map", "a-b", "frozen", "c d"}},
				// A doubled quote inside the quotes is one literal quote.
				{"a doubled quote inside a quoted name", `frozen<"a""b">`, []string{"frozen", `a"b`}},
				// Degenerate input must not hang or panic.
				{"empty input", "", nil},
				{"a lone quote", `"`, nil},
				{"an unterminated quoted name", `frozen<"unterminated`, []string{"frozen", "unterminated"}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					var got []string
					for _, ref := range cqlTypeIdentifiers(tc.in) {
						got = append(got, ref.name)
					}
					if !slices.Equal(got, tc.want) {
						t.Errorf("cqlTypeIdentifiers(%q) = %q, want %q", tc.in, got, tc.want)
					}
				})
			}
		})
	})
	t.Run("NamesUserType", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name string
			ref  cqlTypeRef
			want bool
		}{
			{"bare built-in is not a user type", cqlTypeRef{name: "text"}, false},
			// A constructor name is only decisive when applied to arguments. Bare,
			// it is available to a user type of that name -- which is what lets
			// frozen<map> mean the user type map rather than a collection.
			{"bare constructor name is available to a user type", cqlTypeRef{name: "frozen"}, true},
			{"bare unknown name is a user type", cqlTypeRef{name: "addr"}, true},
			// frozen never wraps a scalar built-in, so its operand is a user type
			// even when the name collides with one.
			{"frozen operand is a user type", cqlTypeRef{name: "text", frozenOperand: true}, true},
			// The server only keeps quotes for names that need them, and never
			// quotes a built-in.
			{"quoted name is a user type", cqlTypeRef{name: "text", quoted: true}, true},
			// A constructor applied to arguments is never a user type, even as the
			// operand of frozen -- frozen<map<...>> is a frozen collection.
			{"parameterised constructor is not a user type",
				cqlTypeRef{name: "map", parameterised: true, frozenOperand: true}, false},
			// Written without arguments the name is available to a user type.
			{"bare constructor name as a frozen operand is a user type",
				cqlTypeRef{name: "map", frozenOperand: true}, true},
			{"vector is a constructor", cqlTypeRef{name: "vector", parameterised: true}, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				if got := tc.ref.namesUserType(); got != tc.want {
					t.Errorf("namesUserType(%+v) = %v, want %v", tc.ref, got, tc.want)
				}
			})
		}
	})
}
