// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
)

// TestPageStateResumesOnAnotherSession checks that a page state's token,
// carried as bytes the way an application would hand it to a client and
// back, resumes the same statement on another session: the second session
// sends the token unchanged and returns the next page.
func TestPageStateResumesOnAnotherSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	run := func(state cql.PageState) (string, cql.PageState, []gridRequest) {
		t.Helper()
		s, _, peers := runtimeTestSession(t, 1)
		srv := newGridServer(t, peers[0], gridPages(t))
		defer srv.stop(t, peers[0])
		defer func() { _ = s.Close() }()
		p, err := s.Prepare(ctx, gridSelect())
		if err != nil {
			t.Fatal(err)
		}
		page, err := p.RunPage(ctx, gridKey, state, 2)
		if err != nil {
			t.Fatal(err)
		}
		defer page.Close()
		var rows string
		var row []byte
		for page.NextInto(&row) {
			rows += string(row)
		}
		if err := page.Err(); err != nil {
			t.Fatal(err)
		}
		return rows, page.State(), srv.sent()
	}
	rows, state, _ := run(cql.StartPage)
	if rows != "ab" || !state.More() {
		t.Fatalf("first page %q, more %v", rows, state.More())
	}
	token := state.Token()
	rows, state, sent := run(cql.PageStateFromToken(token))
	if rows != "c" || !state.Complete() {
		t.Fatalf("resumed page %q, complete %v", rows, state.Complete())
	}
	if len(sent) != 1 || sent[0].pageState != string(token) {
		t.Fatalf("second session sent %+v, want page state %q", sent, token)
	}
}
