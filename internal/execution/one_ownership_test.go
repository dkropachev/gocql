// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"testing"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
)

func TestOneOwnsBorrowedResult(t *testing.T) {
	for _, mode := range []string{"prepared", "query", "unprepared"} {
		t.Run(mode, func(t *testing.T) {
			s := loopbackTestSession(t, "pone")
			session := InternalWrapSession(s)
			statement := binding.Statement[int32, cqltype.Bytes]{CQL: "SELECT value FROM ks.tbl WHERE k = ?", Params: codec.OneParam(codec.Int32), Rows: codec.OneColumn(codec.BlobCodec), BindMarkers: cqltype.NewColumns(cqltype.ColumnSpec{Name: "k", Type: cqltype.IntType})}
			p := codecTestMust(Prepare(context.Background(), s, statement, PrepareOptions{}))
			q := codecTestMust(p.Query(Options{}))
			defer q.Close(context.Background())
			read := func(dst *cqltype.Bytes) (bool, Result, error) {
				switch mode {
				case "prepared":
					return p.One(context.Background(), int32(1), dst)
				case "query":
					return q.One(context.Background(), int32(1), dst)
				default:
					return session.UnpreparedOne(context.Background(), statement, int32(1), dst)
				}
			}
			var first, second cqltype.Bytes
			if found, _, err := read(&first); err != nil || !found {
				t.Fatal(found, err)
			}
			if found, _, err := read(&second); err != nil || !found {
				t.Fatal(found, err)
			}
			if string(first.OwnedBytes()) != loopbackValue || string(second.OwnedBytes()) != loopbackValue {
				t.Fatal("result expired after return or next execution")
			}
		})
	}
}

func TestOnePreservesOwnedBuffer(t *testing.T) {
	q, serve := eachTestQuery(t)
	dst := make([]byte, 0, 64)
	backing := &dst[:cap(dst)][0]
	for range 2 {
		served := serve(runtimeRowsBody(nil, "owned"))
		if found, _, err := q.One(context.Background(), codec.EmptyRecord{}, &dst); err != nil || !found {
			t.Fatal(found, err)
		}
		<-served
		if string(dst) != "owned" || &dst[0] != backing {
			t.Fatal("One replaced the caller-owned buffer")
		}
	}
}
