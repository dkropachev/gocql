// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution_test

import (
	. "github.com/scylladb/gocql/v2"

	"context"
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/retry"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

func TestV2RuntimeSafeRetryPreservesServerErrors(t *testing.T) {
	policy := retry.Safe{MaxAttempts: 3}
	for _, test := range []struct {
		code       int
		idempotent bool
		want       retry.Action
	}{{cql.ErrCodeOverloaded, true, retry.Next}, {cql.ErrCodeBootstrapping, true, retry.Next}, {cql.ErrCodeOverloaded, false, retry.Stop}, {cql.ErrCodeSyntax, true, retry.Stop}, {cql.ErrCodeInvalid, true, retry.Stop}, {cql.ErrCodeProtocol, true, retry.Stop}} {
		server := &frm.ErrorFrame{Code: test.code, Message: "test error"}
		err := runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: server, InternalPotentiallyExecuted: true})
		decision := policy.Decide(retry.Attempt{Err: err, Number: 1, Idempotent: test.idempotent, BytesWritten: true})
		if decision.Action != test.want {
			t.Fatalf("code %x idempotent %t got %d want %d", test.code, test.idempotent, decision.Action, test.want)
		}
		var extracted cql.RequestError
		if !errors.As(err, &extracted) || extracted.GetCode() != test.code {
			t.Fatal("server identity lost")
		}
	}
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, &cql.ProtocolError{Stage: "response", Err: errors.New("invalid frame")}, &cql.ValidationError{Field: "page size", Err: errors.New("invalid")}} {
		if got := policy.Decide(retry.Attempt{Err: err, Number: 1, Idempotent: true}); got.Action != retry.Stop {
			t.Fatalf("retry deterministic error %v", err)
		}
	}
}
func TestV2RuntimeStatementKeywords(t *testing.T) {
	for _, stmt := range []string{"USE ks", " -- comment\n use ks", "/* comment */ USE ks"} {
		if _, err := BindUnpreparedBorrowed(cql.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: stmt, Params: cql.NoParams, Rows: cql.NoRows}, cql.NewColumns(), cql.EmptyParams{}); !errors.Is(err, ErrUseStmt) {
			t.Fatalf("USE accepted %q: %v", stmt, err)
		}
	}
	if conditionalCQL("INSERT INTO t(a) VALUES ('IF')") || !conditionalCQL("INSERT INTO t(a) VALUES ('x') IF NOT EXISTS") {
		t.Fatal("conditional classification")
	}
}
