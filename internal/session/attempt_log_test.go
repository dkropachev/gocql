// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

func TestAttemptLog(t *testing.T) {
	timeout := runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: cqlerr.ErrTimeoutNoResponse, InternalPotentiallyExecuted: true})
	overloaded := runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: cqlerr.NewOtherServerError(cqlerr.CodeOverloaded, ""), InternalPotentiallyExecuted: true})

	t.Run("Empty", func(t *testing.T) {
		var log attemptLog
		if log.status() != cqlerr.NotExecuted || !log.empty() {
			t.Fatal(log.status())
		}
		plain := errors.New("before any attempt")
		if log.merge(plain, cql.Result{}) != plain {
			t.Fatal("empty log changed the error")
		}
	})

	t.Run("MergeHandOff", func(t *testing.T) {
		var log attemptLog
		log.fail(timeout, nil, 1, 0, cql.Option[cql.UUID]{})
		later := newExecutionError(overloaded, cql.Result{Attempts: 1}, cqlerr.NotExecuted, []AttemptFailure{{Err: overloaded, Number: 1, Status: cqlerr.NotExecuted}})
		err := log.merge(later, cql.Result{})
		merged, ok := errors.AsType[*ExecutionError](err)
		if !ok {
			t.Fatal(err)
		}
		if merged.ExecutionStatus() != cqlerr.MaybeExecuted || len(merged.Attempts) != 2 || merged.Result.Attempts != 2 {
			t.Fatalf("status %v attempts %d result %d", merged.ExecutionStatus(), len(merged.Attempts), merged.Result.Attempts)
		}
		if !errors.Is(err, cqlerr.ErrTimeoutNoResponse) {
			t.Fatal("earlier attempt lost")
		}
	})

	t.Run("MergeForeign", func(t *testing.T) {
		var log attemptLog
		log.fail(timeout, nil, 1, 0, cql.Option[cql.UUID]{})
		err := log.merge(errors.New("hand-off failed"), cql.Result{})
		if cqlerr.ExecutionStatusOf(err) != cqlerr.MaybeExecuted || !errors.Is(err, cqlerr.ErrTimeoutNoResponse) {
			t.Fatal(err)
		}
	})
}
