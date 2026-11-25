// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package binding

import (
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

func TestStatement(t *testing.T) {
	t.Run("statementDefaults", func(t *testing.T) {
		t.Run("FillsEmptyTypes", func(t *testing.T) {
			s := Statement[cql.EmptyRow, cql.EmptyRow]{CQL: "TRUNCATE ks.t"}
			statementDefaults(&s)
			if s.Params == nil || s.Rows == nil {
				t.Fatalf("empty defaults not filled: %+v", s)
			}
		})
		t.Run("LeavesTypedCodecsUnset", func(t *testing.T) {
			s := Statement[int32, string]{CQL: "SELECT v FROM ks.t WHERE k = ?"}
			statementDefaults(&s)
			if s.Params != nil || s.Rows != nil {
				t.Fatalf("typed statement got empty defaults: %+v", s)
			}
		})
	})

	t.Run("validateStatement", func(t *testing.T) {
		t.Run("AcceptsDefaultedEmptyTypes", func(t *testing.T) {
			s := Statement[cql.EmptyRow, cql.EmptyRow]{CQL: "TRUNCATE ks.t"}
			statementDefaults(&s)
			if err := validateStatement(s); err != nil {
				t.Fatal(err)
			}
		})
		t.Run("RejectsTypedWithoutCodecs", func(t *testing.T) {
			s := Statement[int32, string]{CQL: "SELECT v FROM ks.t WHERE k = ?"}
			statementDefaults(&s)
			if err := validateStatement(s); err == nil {
				t.Fatal("statement without codecs accepted")
			}
		})
	})
}

// TestResolveKind checks how an unclassified statement resolves: a CASRows
// decoder or a lexical IF in a DML statement makes it conditional, an IF in
// a string, a comment, a quoted identifier, a longer word or DDL does not,
// and an explicit kind wins unless an ordinary one is conditional.
func TestResolveKind(t *testing.T) {
	conditional := []string{
		"INSERT INTO t(k) VALUES(1) IF NOT EXISTS",
		"update t SET v = 1 WHERE k = 1 if v = 0",
		"DELETE FROM t WHERE k = 1 IF EXISTS",
		"  /* lead */ UPDATE t SET v = 'x' WHERE k = 1 IF EXISTS",
		"BEGIN BATCH INSERT INTO t(k) VALUES(1) IF NOT EXISTS; APPLY BATCH",
		"BEGIN UNLOGGED BATCH UPDATE t SET v = 1 WHERE k = 1 IF v = 0 APPLY BATCH",
		"UPDATE t SET v = 1 WHERE k = 1\nIF\tv = 0",
		"// lead\nUPDATE t SET v = 1 WHERE k = 1 IF v = 0",
		"-- lead\r\nDELETE FROM t WHERE k = 1 IF EXISTS",
		"INSERT INTO t(k, v) VALUES(1, $$ it's $$) IF NOT EXISTS",
		"BEGIN COUNTER BATCH UPDATE t SET c = c + 1 WHERE k = 1 IF EXISTS APPLY BATCH",
	}
	ordinary := []string{
		"SELECT v FROM t WHERE k = 1",
		"INSERT INTO t(k, v) VALUES(1, 'IF')",
		"INSERT INTO t(k, v) VALUES(1, 'it''s IF')",
		"UPDATE t SET \"if\" = 1 WHERE k = 1",
		"UPDATE t SET v = 1 WHERE k = 1 -- IF v = 0",
		"UPDATE t SET v = 1 WHERE k = 1 /* IF v = 0 */",
		"UPDATE t SET iff = 1, if_x = 2 WHERE k = 1",
		"CREATE TABLE IF NOT EXISTS t (k int PRIMARY KEY)",
		"DROP KEYSPACE IF EXISTS ks",
		"SELECT * FROM t WHERE k = 1 /* UPDATE IF */",
		"BEGIN BATCH INSERT INTO t(k) VALUES(1); APPLY BATCH",
		"INSERT INTO t(k, v) VALUES(1, $$ IF $$)",
		"INSERT INTO t(k, v) VALUES(1, $$it's 'IF'$$)",
		"UPDATE t SET v = 1 WHERE k = 1 // IF v = 0",
		"// UPDATE t SET v = 1 WHERE k = 1 IF v = 0\nSELECT * FROM t",
		"UPDATE t SET v = 1 WHERE k = 1 /* IF",
		"INSERT INTO t(k, v) VALUES(1, $$ IF",
	}
	for _, text := range conditional {
		if got, err := ResolveKind(cql.UnknownStatement, text, nil); err != nil || got != cql.ConditionalStatement {
			t.Errorf("%q: %v, %v; want conditional", text, got, err)
		}
		if got, err := ResolveKind(cql.ConditionalStatement, text, nil); err != nil || got != cql.ConditionalStatement {
			t.Errorf("conditional %q: %v, %v", text, got, err)
		}
		if _, err := ResolveKind(cql.OrdinaryStatement, text, nil); !errors.As(err, new(*cql.ValidationError)) {
			t.Errorf("ordinary %q: %v, want a ValidationError", text, err)
		}
	}
	for _, text := range ordinary {
		if got, err := ResolveKind(cql.UnknownStatement, text, nil); err != nil || got != cql.OrdinaryStatement {
			t.Errorf("%q: %v, %v; want ordinary", text, got, err)
		}
		if got, err := ResolveKind(cql.ConditionalStatement, text, nil); err != nil || got != cql.ConditionalStatement {
			t.Errorf("explicit conditional %q: %v, %v; want it kept", text, got, err)
		}
	}
	// A CASRows decoder makes the statement conditional whatever its text.
	if got, err := ResolveKind(cql.UnknownStatement, "SELECT * FROM t", cql.CASApplied); err != nil || got != cql.ConditionalStatement {
		t.Errorf("CASRows: %v, %v; want conditional", got, err)
	}
	if got, err := ResolveKind(cql.UnknownStatement, "SELECT * FROM t", cql.CASRows(cql.OneColumn(cql.Int32Codec))); err != nil || got != cql.ConditionalStatement {
		t.Errorf("CASRows of a decoder: %v, %v; want conditional", got, err)
	}
	if _, err := ResolveKind(cql.OrdinaryStatement, "SELECT * FROM t", cql.CASApplied); !errors.As(err, new(*cql.ValidationError)) {
		t.Errorf("ordinary CASRows: %v, want a ValidationError", err)
	}
	// ValidateStatement stores the resolved kind.
	s := Statement[cql.EmptyParams, cql.EmptyRow]{CQL: "DELETE FROM t WHERE k = 1 IF EXISTS"}
	if err := ValidateStatement(&s); err != nil || s.Kind != cql.ConditionalStatement {
		t.Errorf("ValidateStatement: kind %v, %v", s.Kind, err)
	}
}
