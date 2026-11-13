// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
)

// TestV2SchemaRowsManageDestination checks that the schema row plan clears
// its own destination, so the session skips its per-row clear.
func TestV2SchemaRowsManageDestination(t *testing.T) {
	type row struct{ Name string }
	plan, err := codec.CompileRowPlan[row](InternalV2SchemaRows[row]{}, cqltype.NewColumns())
	if err != nil {
		t.Fatal(err)
	}
	if !codec.ManagesRowDestination(plan) {
		t.Fatal("schema row plan does not manage its destination")
	}
}
