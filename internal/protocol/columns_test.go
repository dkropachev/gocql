// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/link"
)

func TestColumnsMirror(t *testing.T) {
	if err := link.CheckLayout[cql.Columns, columnsRep](); err != nil {
		t.Fatal(err)
	}
	a := cql.NewColumns(cql.ColumnSpec{Name: "a", Type: cql.IntType})
	b := cql.NewColumns(cql.ColumnSpec{Name: "a", Type: cql.IntType})
	if !sameColumns(a, a) || sameColumns(a, b) || !a.Equal(b) || !sameColumns(cql.Columns{}, cql.Columns{}) {
		t.Fatal("sameColumns must compare the backing, not the content")
	}
}
