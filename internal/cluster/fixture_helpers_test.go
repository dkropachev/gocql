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

package cluster

import (
	"strings"
	"testing"
)

func TestFixtureHelpers(t *testing.T) {
	t.Parallel()

	t.Run("ExtractKeyspaceTableFromDDL", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name      string
			ddl       string
			wantKS    string
			wantTable string
		}{
			{
				name:      "simple_create_table",
				ddl:       "CREATE TABLE gocql_test.my_table (id int PRIMARY KEY)",
				wantKS:    "gocql_test",
				wantTable: "my_table",
			},
			{
				name:      "create_table_if_not_exists",
				ddl:       "CREATE TABLE IF NOT EXISTS gocql_test.my_table (id int PRIMARY KEY)",
				wantKS:    "gocql_test",
				wantTable: "my_table",
			},
			{
				name:      "lowercase_create_table",
				ddl:       "create table gocql_test.my_table (id int primary key)",
				wantKS:    "gocql_test",
				wantTable: "my_table",
			},
			{
				name:      "mixed_case_if_not_exists",
				ddl:       "Create Table If Not Exists gocql_test.my_table (id int PRIMARY KEY)",
				wantKS:    "gocql_test",
				wantTable: "my_table",
			},
			{
				name:      "no_keyspace_prefix",
				ddl:       "CREATE TABLE my_table (id int PRIMARY KEY)",
				wantKS:    "",
				wantTable: "",
			},
			{
				name:      "empty_string",
				ddl:       "",
				wantKS:    "",
				wantTable: "",
			},
			{
				name:      "create_keyspace_ignored",
				ddl:       "CREATE KEYSPACE my_ks WITH replication = {}",
				wantKS:    "",
				wantTable: "",
			},
			{
				name:      "materialized_view_ignored",
				ddl:       "CREATE MATERIALIZED VIEW my_ks.my_view AS SELECT * FROM my_ks.my_table WHERE id IS NOT NULL PRIMARY KEY (id)",
				wantKS:    "",
				wantTable: "",
			},
			{
				name:      "multiline_ddl",
				ddl:       "CREATE TABLE gocql_test.test_single_routing_key (\n\tfirst_id int,\n\tsecond_id int,\n\tPRIMARY KEY (first_id, second_id)\n)",
				wantKS:    "gocql_test",
				wantTable: "test_single_routing_key",
			},
			{
				name:      "tablets_disabled_keyspace",
				ddl:       "CREATE TABLE gocql_test_tablets_disabled.my_table (id int PRIMARY KEY)",
				wantKS:    "gocql_test_tablets_disabled",
				wantTable: "my_table",
			},
			{
				name:      "drop_table_if_exists",
				ddl:       "DROP TABLE IF EXISTS gocql_test.my_table",
				wantKS:    "gocql_test",
				wantTable: "my_table",
			},
			{
				name:      "drop_table_if_exists_lowercase",
				ddl:       "drop table if exists gocql_test.my_table",
				wantKS:    "gocql_test",
				wantTable: "my_table",
			},
			{
				name:      "drop_table_no_keyspace",
				ddl:       "DROP TABLE IF EXISTS my_table",
				wantKS:    "",
				wantTable: "",
			},
			{
				name:      "table_with_space_before_paren",
				ddl:       "CREATE TABLE gocql_test.t1 (id int PRIMARY KEY)",
				wantKS:    "gocql_test",
				wantTable: "t1",
			},
			{
				name:      "drop_keyspace_returns_empty",
				ddl:       "DROP KEYSPACE IF EXISTS gocql_test",
				wantKS:    "",
				wantTable: "",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				gotKS, gotTable := extractKeyspaceTableFromDDL(tt.ddl)
				if gotKS != tt.wantKS {
					t.Errorf("extractKeyspaceTableFromDDL(%q) keyspace = %q, want %q", tt.ddl, gotKS, tt.wantKS)
				}
				if gotTable != tt.wantTable {
					t.Errorf("extractKeyspaceTableFromDDL(%q) table = %q, want %q", tt.ddl, gotTable, tt.wantTable)
				}
			})
		}
	})

	t.Run("TableName", func(t *testing.T) {
		t.Parallel()

		t.Run("Parts", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name  string
				parts []string
				want  string
			}{
				{
					name: "basic",
					want: "testfixturehelpers_tablename_parts_basic",
				},
				{
					name:  "with_parts",
					parts: []string{"single"},
					want:  "testfixturehelp_729d3e6f22e61a4f_th_parts_single",
				},
				{
					name:  "multiple_parts",
					parts: []string{"foo", "bar"},
					want:  "testfixturehelp_de98f5a3ea4e06e7_e_parts_foo_bar",
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					got := testTableName(t, tt.parts...)
					if got != tt.want {
						t.Errorf("testTableName() = %q, want %q", got, tt.want)
					}
				})
			}
		})

		t.Run("SanitizesSpecialChars", func(t *testing.T) {
			t.Parallel()

			t.Run("sub/with/slashes", func(t *testing.T) {
				got := testTableName(t)
				if strings.Contains(got, "/") {
					t.Errorf("expected no slashes, got %q", got)
				}
				if strings.Contains(got, "__") {
					t.Errorf("expected no consecutive underscores, got %q", got)
				}
			})
		})

		t.Run("Truncation", func(t *testing.T) {
			t.Parallel()

			long := "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz"
			t.Run(long, func(t *testing.T) {
				got := testTableName(t, "extra")
				if len(got) > maxCQLIdentifierLen {
					t.Errorf("len = %d, want <= %d; value = %q", len(got), maxCQLIdentifierLen, got)
				}
				// Should preserve chars from both the start and end around the hash.
				if got[:5] != "testf" {
					t.Errorf("expected prefix from test name, got %q", got)
				}
				if !strings.HasSuffix(got, "_extra") {
					t.Errorf("expected suffix from test name and parts, got %q", got)
				}
				if len(got) != maxCQLIdentifierLen {
					t.Errorf("expected truncated name to use full identifier budget, got len=%d value=%q", len(got), got)
				}
				if got[15] != '_' || got[32] != '_' {
					t.Errorf("expected <first-n>_<hash>_<last-n> structure, got %q", got)
				}
				for _, ch := range got[16:32] {
					if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
						t.Errorf("expected hex hash in the middle, got %q", got)
						break
					}
				}
			})
		})

		t.Run("Uniqueness", func(t *testing.T) {
			t.Parallel()

			a := testTableName(t, "alpha")
			b := testTableName(t, "beta")
			if a == b {
				t.Errorf("expected different names, both got %q", a)
			}
		})
	})
}
