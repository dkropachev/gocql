// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
)

func TestBindAPI(t *testing.T) {
	t.Run("Keywords", func(t *testing.T) {
		t.Run("RejectsUseAndClassifiesConditional", func(t *testing.T) {
			for _, stmt := range []string{"USE ks", " -- comment\n use ks", "/* comment */ USE ks"} {
				if _, err := BindUnpreparedBorrowed(cql.Command(stmt), cql.NewColumns(), cql.EmptyParams{}); !errors.Is(err, cqlerr.ErrUseStmt) {
					t.Fatalf("USE accepted %q: %v", stmt, err)
				}
			}
			if conditionalCQL("INSERT INTO t(a) VALUES ('IF')") || !conditionalCQL("INSERT INTO t(a) VALUES ('x') IF NOT EXISTS") {
				t.Fatal("conditional classification")
			}
		})
	})

	t.Run("Ownership", func(t *testing.T) {
		t.Run("Canonical", func(t *testing.T) {
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
		})
	})
}
