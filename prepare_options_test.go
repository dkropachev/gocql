// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

func TestPrepareOptions(t *testing.T) {
	t.Run("BindOptions", func(t *testing.T) {
		t.Run("RequiresSession", func(t *testing.T) {
			var s *Session
			if _, err := s.BindOptions(Options{}); err == nil {
				t.Fatal("options bound without a session")
			}
		})
	})

	t.Run("OnePrepareOptions", func(t *testing.T) {
		t.Run("AcceptsAtMostOne", func(t *testing.T) {
			var s *Session
			statement := cql.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: "SELECT now() FROM system.local"}
			ctx := context.Background()
			for _, options := range [][]PrepareOptions{nil, {{Keyspace: "ks"}}} {
				if _, err := s.Prepare(ctx, statement, options...); err == nil || strings.Contains(err.Error(), "at most one") {
					t.Fatalf("%d PrepareOptions: %v", len(options), err)
				}
			}
			if _, err := s.Prepare(ctx, statement, PrepareOptions{}, PrepareOptions{}); err == nil || !strings.Contains(err.Error(), "at most one") {
				t.Fatalf("two PrepareOptions accepted: %v", err)
			}
		})
	})
}
