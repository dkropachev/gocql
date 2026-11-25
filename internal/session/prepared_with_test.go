// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"testing"
)

// TestPreparedWithSharesPool checks that a view made by With runs on the
// pool of the Prepared it was made from, also through a view of a view, and
// builds none of its own.
func TestPreparedWithSharesPool(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	defer s.Close()
	defer peers[0].Close()
	requests := make(chan oneShotRequest, 1)
	p := oneShotPrepare(t, s, peers[0], requests)
	view := p.With(CompiledOptions{}).With(CompiledOptions{})
	done := make(chan error, 1)
	go func() {
		var dst []byte
		_, _, err := view.One(context.Background(), 1, &dst, CompiledOptions{})
		done <- err
	}()
	r := <-requests
	if _, err := peers[0].Write(typedTestResponse(t, 4, r.stream, runtimeRowsBody(nil, "row"))); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	waitIdle(t, p, 1)
	if view.privateStorage.pool != nil || view.privateStorage.root != p {
		t.Fatal("the view built a pool of its own instead of sharing p's")
	}
}
