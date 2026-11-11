// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

// An Exec of a statement or batch known to be conditional is rejected before
// anything is sent: its result reports whether it applied, which Exec cannot
// return, so sending it would apply a write the caller cannot inspect.
func TestConditionalExecRejected(t *testing.T) {
	check := func(t *testing.T, err error) {
		t.Helper()
		if !errors.Is(err, runtimeerrors.ErrConditionalExec) {
			t.Fatalf("err = %v, want ErrConditionalExec", err)
		}
		if status := runtimeerrors.ExecutionStatusOf(err); status != cqltype.NotExecuted {
			t.Fatalf("status = %v, want NotExecuted", status)
		}
		if kind := runtimeerrors.KindOf(err); kind != runtimeerrors.KindInvalid {
			t.Fatalf("kind = %v, want invalid", kind)
		}
	}
	t.Run("Statement", func(t *testing.T) {
		// The session has no connections: a request that was sent would fail
		// with a connection error instead.
		s, _, _ := runtimeTestSession(t, 0)
		b, err := BindUnpreparedBorrowed(binding.Statement[codec.EmptyRecord, cqltype.Bytes]{Kind: cqltype.ConditionalStatement, CQL: "UPDATE ks.tbl SET v = 1 WHERE k = 1 IF v = 0", Params: codec.EmptyParamsEncoder, Rows: codec.OneColumn(codec.BlobCodec)}, cqltype.NewColumns(), codec.EmptyRecord{})
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		_, err = Exec(context.Background(), s, b, CompiledOptions{})
		check(t, err)
		if m := s.Metrics(); m.Requests != 0 {
			t.Fatalf("a rejected Exec counted a request: %+v", m)
		}
	})
	t.Run("Batch", func(t *testing.T) {
		s, _, _ := runtimeTestSession(t, 0)
		for _, kinds := range [][]cqltype.StatementKind{
			{cqltype.ConditionalStatement},
			{cqltype.OrdinaryStatement, cqltype.ConditionalStatement},
		} {
			var batch Batch
			for _, kind := range kinds {
				if err := batch.add(s, cqlText{}, "UPDATE t SET v=1", &model.RoutingInfo{Kind: kind}, model.EncodedValues{}); err != nil {
					t.Fatal(err)
				}
			}
			_, err := InternalExecBatch(context.Background(), s, cqltype.LoggedBatch, &batch, CompiledOptions{})
			check(t, err)
			_, err = InternalWrapSession(s).ExecBatch(context.Background(), cqltype.LoggedBatch, &batch)
			check(t, err)
		}
	})
}
