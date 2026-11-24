// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package runtimeerrors

import (
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/link"
)

func TestMirrorLayouts(t *testing.T) {
	if err := link.CheckLayout[cqlerr.ExecutionError, executionErrorRep](); err != nil {
		t.Error(err)
	}
}

func TestConstructors(t *testing.T) {
	cause := errors.New("cause")
	q := NewQueryError(QueryErrorState{InternalErr: cause, InternalPotentiallyExecuted: true})
	if !errors.Is(q, cause) || cqlerr.ExecutionStatusOf(q) != cqlerr.MaybeExecuted {
		t.Fatalf("NewQueryError = %v, status %v", q, cqlerr.ExecutionStatusOf(q))
	}
	if unsent := NewQueryError(QueryErrorState{InternalErr: cause}); cqlerr.ExecutionStatusOf(unsent) != cqlerr.NotExecuted {
		t.Fatalf("unsent attempt: status %v", cqlerr.ExecutionStatusOf(unsent))
	}
	e := NewExecutionError(q, cqlerr.ExecutionError{}.Result, cqlerr.PartiallyExecuted, nil)
	if e.ExecutionStatus() != cqlerr.PartiallyExecuted || !errors.Is(e, cause) {
		t.Fatalf("NewExecutionError = %v, status %v", e, e.ExecutionStatus())
	}
	if msg := WithMessage(&cqlerr.UnavailableError{}, "down").Message(); msg != "down" {
		t.Fatalf("WithMessage: Message() = %q", msg)
	}
}
