// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/exec"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
)

func TestPrepareOptions(t *testing.T) {
	t.Run("CompileOptions", func(t *testing.T) {
		t.Run("RequiresSession", func(t *testing.T) {
			var s *testSession
			if _, err := s.CompileOptions(exec.Options{}); err == nil {
				t.Fatal("options bound without a session")
			}
		})
	})

	t.Run("OnePrepareOptions", func(t *testing.T) {
		t.Run("AcceptsAtMostOne", func(t *testing.T) {
			var s *testSession
			statement := cql.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: "SELECT now() FROM system.local"}
			ctx := context.Background()
			for _, options := range [][]exec.PrepareOptions{nil, {{Keyspace: "ks"}}} {
				if _, err := s.Prepare(ctx, statement, options...); err == nil || strings.Contains(err.Error(), "at most one") {
					t.Fatalf("%d PrepareOptions: %v", len(options), err)
				}
			}
			_, err := s.Prepare(ctx, statement, exec.PrepareOptions{}, exec.PrepareOptions{})
			if v, ok := errors.AsType[*cqlerr.ValidationError](err); !ok || v.Field != "options" || !strings.Contains(err.Error(), "at most one") {
				t.Fatalf("two PrepareOptions: %v, want an options ValidationError", err)
			}
		})
	})
}
