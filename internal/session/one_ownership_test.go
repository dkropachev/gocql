// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
)

func TestOneOwnsBorrowedResult(t *testing.T) {
	for _, mode := range []string{"prepared", "query", "unprepared"} {
		t.Run(mode, func(t *testing.T) {
			s := loopbackTestSession(t, "pone")
			session := InternalWrapSession(s)
			statement := binding.Statement[int32, cql.Bytes]{CQL: "SELECT value FROM ks.tbl WHERE k = ?", Params: cql.OneParam(cql.Int32Codec), Rows: cql.OneColumn(cql.BlobCodec), BindMarkers: cql.NewColumns(cql.ColumnSpec{Name: "k", Type: cql.IntType})}
			p := codecTestMust(Prepare(context.Background(), s, statement, PrepareOptions{}))
			q := codecTestMust(p.Query(Options{}))
			defer q.Close()
			read := func(dst *cql.Bytes) (bool, cql.Result, error) {
				switch mode {
				case "prepared":
					return p.One(context.Background(), int32(1), dst, CompiledOptions{})
				case "query":
					return q.One(context.Background(), int32(1), dst)
				default:
					return session.UnpreparedOne(context.Background(), statement, int32(1), dst)
				}
			}
			var first, second cql.Bytes
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
		if found, _, err := q.One(context.Background(), cql.EmptyRow{}, &dst); err != nil || !found {
			t.Fatal(found, err)
		}
		<-served
		if string(dst) != "owned" || &dst[0] != backing {
			t.Fatal("One replaced the caller-owned buffer")
		}
	}
}
