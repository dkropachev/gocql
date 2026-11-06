// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package binding

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/codec"
)

func TestStatement(t *testing.T) {
	t.Run("InternalStatementDefaults", func(t *testing.T) {
		t.Run("FillsEmptyTypes", func(t *testing.T) {
			s := Statement[codec.EmptyRecord, codec.EmptyRecord]{CQL: "TRUNCATE ks.t"}
			InternalStatementDefaults(&s)
			if s.Params == nil || s.Rows == nil {
				t.Fatalf("empty defaults not filled: %+v", s)
			}
		})
		t.Run("LeavesTypedCodecsUnset", func(t *testing.T) {
			s := Statement[int32, string]{CQL: "SELECT v FROM ks.t WHERE k = ?"}
			InternalStatementDefaults(&s)
			if s.Params != nil || s.Rows != nil {
				t.Fatalf("typed statement got empty defaults: %+v", s)
			}
		})
	})

	t.Run("InternalValidateStatement", func(t *testing.T) {
		t.Run("AcceptsDefaultedEmptyTypes", func(t *testing.T) {
			s := Statement[codec.EmptyRecord, codec.EmptyRecord]{CQL: "TRUNCATE ks.t"}
			InternalStatementDefaults(&s)
			if err := InternalValidateStatement(s); err != nil {
				t.Fatal(err)
			}
		})
		t.Run("RejectsTypedWithoutCodecs", func(t *testing.T) {
			s := Statement[int32, string]{CQL: "SELECT v FROM ks.t WHERE k = ?"}
			InternalStatementDefaults(&s)
			if err := InternalValidateStatement(s); err == nil {
				t.Fatal("statement without codecs accepted")
			}
		})
	})
}
