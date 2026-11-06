// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
)

type countingBlobEncoder struct{ calls *int }

func (c countingBlobEncoder) CheckType(t cql.DataType) error { return cql.BytesEncoder.CheckType(t) }
func (c countingBlobEncoder) Encode(w *cql.ValueWriter, b []byte) error {
	*c.calls++
	return cql.BytesEncoder.Encode(w, b)
}
func TestV2RuntimeBindCanonicalOwnership(t *testing.T) {
	typ := cql.BlobType
	columns := cql.NewColumns(cql.ColumnSpec{Name: "v", Type: typ})
	calls := 0
	statement := cql.Statement[[]byte, cql.EmptyRow]{CQL: "INSERT INTO ks.tbl(v) VALUES (?)", Params: cql.OneParam[[]byte](countingBlobEncoder{calls: &calls}), Rows: cql.EmptyRowDecoder}
	input := []byte("original")
	borrowed, err := BindUnpreparedBorrowed(statement, columns, input)
	if err != nil {
		t.Fatal(err)
	}
	owned, err := BindUnpreparedOwned(statement, columns, input)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("encoder calls %d", calls)
	}
	input[0] = 'X'
	borrowedCell := implExecution.BoundData(borrowed).InternalRequest.InternalValues.Cells[0].Body
	ownedCell := implExecution.BoundData(owned).InternalRequest.InternalValues.Cells[0].Body
	a := make([]byte, borrowedCell.Len())
	b := make([]byte, ownedCell.Len())
	borrowedCell.CopyTo(a)
	ownedCell.CopyTo(b)
	if !bytes.Equal(a, input) || string(b) != "original" {
		t.Fatalf("borrowed=%q owned=%q", a, b)
	}
	for _, bound := range []*Bound[cql.EmptyRow]{borrowed, owned} {
		bound.Close()
		if err := bound.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}
