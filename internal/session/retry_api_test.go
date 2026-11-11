// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session_test

import (
	"context"
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/retry"
)

func TestRetryAPI(t *testing.T) {
	t.Run("Safe", func(t *testing.T) {
		t.Run("PreservesServerErrors", func(t *testing.T) {
			policy := retry.Safe{MaxAttempts: 3}
			for _, test := range []struct {
				code       cqlerr.ErrorCode
				idempotent bool
				want       retry.Action
			}{{cqlerr.CodeOverloaded, true, retry.Next}, {cqlerr.CodeBootstrapping, true, retry.Next}, {cqlerr.CodeOverloaded, false, retry.Next}, {cqlerr.CodeWriteTimeout, false, retry.Stop}, {cqlerr.CodeSyntax, true, retry.Stop}, {cqlerr.CodeInvalid, true, retry.Stop}, {cqlerr.CodeProtocol, true, retry.Stop}} {
				server := protocol.NewOtherServerError(test.code, "test error")
				err := runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: server, InternalPotentiallyExecuted: true})
				decision := policy.Decide(retry.Attempt{Err: err, Number: 1, Idempotent: test.idempotent, BytesWritten: true, ExecutionStatus: runtimeerrors.ExecutionStatusOf(err)})
				if decision.Action != test.want {
					t.Fatalf("code %x idempotent %t got %d want %d", test.code, test.idempotent, decision.Action, test.want)
				}
				if extracted, ok := errors.AsType[cqlerr.ServerError](err); !ok || extracted.Code() != test.code {
					t.Fatal("server identity lost")
				}
			}
			for _, err := range []error{context.Canceled, context.DeadlineExceeded, &cqlerr.ProtocolError{Stage: "response", Err: errors.New("invalid frame")}, &cqlerr.ValidationError{Field: "page size", Err: errors.New("invalid")}} {
				if got := policy.Decide(retry.Attempt{Err: err, Number: 1, Idempotent: true}); got.Action != retry.Stop {
					t.Fatalf("retry deterministic error %v", err)
				}
			}
		})
	})
}
