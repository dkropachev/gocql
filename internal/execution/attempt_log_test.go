// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/cqlerr"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

func TestAttemptLog(t *testing.T) {
	timeout := runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: runtimeerrors.ErrTimeoutNoResponse, InternalPotentiallyExecuted: true})
	overloaded := runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: &frm.ErrorFrame{Code: protocol.ErrCodeOverloaded}, InternalPotentiallyExecuted: true})

	t.Run("Empty", func(t *testing.T) {
		var log attemptLog
		if log.status() != NotExecuted || !log.empty() {
			t.Fatal(log.status())
		}
		plain := errors.New("before any attempt")
		if log.merge(plain, Result{}) != plain {
			t.Fatal("empty log changed the error")
		}
	})

	t.Run("MergeHandOff", func(t *testing.T) {
		var log attemptLog
		log.fail(timeout, nil, 1, 0)
		later := newExecutionError(overloaded, Result{Attempts: 1}, NotExecuted, []AttemptFailure{{Err: overloaded, Number: 1, Status: NotExecuted}})
		err := log.merge(later, Result{})
		merged, ok := errors.AsType[*ExecutionError](err)
		if !ok {
			t.Fatal(err)
		}
		if merged.ExecutionStatus() != MaybeExecuted || len(merged.Attempts) != 2 || merged.Result.Attempts != 2 {
			t.Fatalf("status %v attempts %d result %d", merged.ExecutionStatus(), len(merged.Attempts), merged.Result.Attempts)
		}
		if !errors.Is(err, runtimeerrors.ErrTimeoutNoResponse) {
			t.Fatal("earlier attempt lost")
		}
	})

	t.Run("MergeForeign", func(t *testing.T) {
		var log attemptLog
		log.fail(timeout, nil, 1, 0)
		err := log.merge(errors.New("hand-off failed"), Result{})
		if cqlerr.ExecutionStatusOf(err) != MaybeExecuted || !errors.Is(err, runtimeerrors.ErrTimeoutNoResponse) {
			t.Fatal(err)
		}
	})
}
