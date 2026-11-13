// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
)

// ScyllaDB returns one row per statement of a conditional batch, conditional
// or not, in statement order: row i belongs to statement i.
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
	batch := build(o, c, u, c, c)
	for row := range 5 {
		if got, ok := batch.InternalConditionalStatement(row); !ok || got != row {
			t.Errorf("row %d: statement %d %t, want %d", row, got, ok, row)
		}
	}
	for _, row := range []int{-1, 5} {
		if _, ok := batch.InternalConditionalStatement(row); ok {
			t.Errorf("row %d mapped to a statement", row)
		}
	}
	var none *Batch
	if _, ok := none.InternalConditionalStatement(0); ok {
		t.Error("nil batch mapped a row")
	}
}
