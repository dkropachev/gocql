// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

// TestRequestIdempotence pins how a request's idempotence combines its
// statement's, its profile's and its options': a set option wins, and
// otherwise either the statement or the profile marks it idempotent.
func TestRequestIdempotence(t *testing.T) {
	for _, tc := range []struct {
		name               string
		statement, profile bool
		option             cql.Option[bool]
		want               bool
	}{
		{"Neither", false, false, cql.Option[bool]{}, false},
		{"Statement", true, false, cql.Option[bool]{}, true},
		{"Profile", false, true, cql.Option[bool]{}, true},
		{"OptionOverridesProfile", false, true, cql.Some(false), false},
		{"OptionOverridesStatement", true, false, cql.Some(false), false},
		{"OptionMarks", false, false, cql.Some(true), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := &InternalResolvedOptions{}
			options.InternalProfile.Idempotent = tc.profile
			options.InternalOptions.Idempotent = tc.option
			r := InternalExecutionRequest{idempotent: tc.statement, options: options}
			if got := r.isIdempotent(); got != tc.want {
				t.Fatalf("isIdempotent() = %v, want %v", got, tc.want)
			}
		})
	}
}
