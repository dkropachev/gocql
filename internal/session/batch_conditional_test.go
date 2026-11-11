// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
)

// ScyllaDB returns one row per conditional statement of a batch, in statement
// order: row i belongs to the i-th conditional statement.
func TestBatchConditionalStatement(t *testing.T) {
	s, _, _ := runtimeTestSession(t, 0)
	build := func(kinds ...cqltype.StatementKind) *Batch {
		var batch Batch
		for _, kind := range kinds {
			if err := batch.add(s, cqlText{}, "UPDATE t SET v=1", &model.RoutingInfo{Kind: kind}, model.EncodedValues{}); err != nil {
				t.Fatal(err)
			}
		}
		return &batch
	}
	o, c, u := cqltype.OrdinaryStatement, cqltype.ConditionalStatement, cqltype.UnknownStatement
	batch := build(o, c, o, c, c)
	for row, want := range []int{1, 3, 4} {
		if got, ok := batch.InternalConditionalStatement(row); !ok || got != want {
			t.Errorf("row %d: statement %d %t, want %d", row, got, ok, want)
		}
	}
	for _, row := range []int{-1, 3} {
		if _, ok := batch.InternalConditionalStatement(row); ok {
			t.Errorf("row %d mapped to a statement", row)
		}
	}
	// A statement of unknown kind may or may not have a row, so the rows
	// after it cannot be placed.
	batch = build(c, u, c)
	if got, ok := batch.InternalConditionalStatement(0); !ok || got != 0 {
		t.Errorf("row 0 before the unknown statement: %d %t", got, ok)
	}
	if _, ok := batch.InternalConditionalStatement(1); ok {
		t.Error("row 1 after an unknown statement was mapped")
	}
	var none *Batch
	if _, ok := none.InternalConditionalStatement(0); ok {
		t.Error("nil batch mapped a row")
	}
}
