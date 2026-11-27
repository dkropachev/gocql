// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/exec"
)

// TestUnpreparedCompiledOptions checks that the Unprepared calls take
// exec.CompiledOptions and apply them, and that invalid values never reach a
// call: Session.CompileOptions rejects them. The dialect checks options
// compiled for another session get on each execution are
// TestDialectOnlyOptions's, in internal/session.
func TestUnpreparedCompiledOptions(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	srv := newGridServer(t, peers[0], gridPages(t))
	defer srv.stop(t, peers[0])
	defer func() { _ = s.Close() }()
	if _, err := s.CompileOptions(exec.Options{Timeout: cql.Some(-time.Second)}); !hasValidation(err) {
		t.Fatalf("CompileOptions of a negative timeout = %v, want a ValidationError", err)
	}
	options, err := s.CompileOptions(gridOptions)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var dst []byte
	if _, _, err := s.UnpreparedOne(ctx, gridSelect(), gridKey, &dst, options); err != nil {
		t.Fatalf("UnpreparedOne: %v", err)
	}
	if sent := srv.sent(); len(sent) != 1 || sent[0].consistency != cql.Three {
		t.Fatalf("sent %+v, want one request at THREE", sent)
	}
}

func hasValidation(err error) bool {
	_, ok := errors.AsType[*cql.ValidationError](err)
	return ok
}
