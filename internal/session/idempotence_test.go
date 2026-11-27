// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/retry"
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

// TestFailureIdempotence checks that a failed request's ExecutionError
// carries the idempotence the driver resolved for it, on every execution
// path and whether or not the session reports statements in errors, so
// cqlerr.Retryable needs no argument for it, and that the retry policy sees
// the caller's context.
func TestFailureIdempotence(t *testing.T) {
	refuse := func(peer net.Conn) {
		_, stream, _, err := readRequest(peer)
		if err != nil {
			return
		}
		_, _ = peer.Write(serverError(t, stream, cqlerr.CodeInvalid, nil))
	}
	type key struct{}
	var seen any // the value the retry policy found in its Attempt's Context
	options := func(idempotent bool) Options {
		return Options{Idempotent: cql.Some(idempotent), Retry: retry.PolicyFunc(func(a retry.Attempt) retry.Decision {
			seen = a.Context.Value(key{})
			return retry.Decision{}
		})}
	}
	paths := map[string]func(ctx context.Context, idempotent bool) error{
		"RunPage": func(ctx context.Context, idempotent bool) error {
			s, _, peers := runtimeTestSession(t, 1)
			defer s.Close()
			go refuse(peers[0])
			_, err := RunPage(ctx, s, secretQuery(t, cql.OrdinaryStatement), cql.StartPage(), 0, testOptions(options(idempotent)))
			return err
		},
		"ExecBatch": func(ctx context.Context, idempotent bool) error {
			s, _, peers := runtimeTestSession(t, 1)
			defer s.Close()
			var batch Batch
			if err := batchTestQuery(t, s).AddToBatch(&batch, []byte("v")); err != nil {
				t.Fatal(err)
			}
			go refuse(peers[0])
			_, err := InternalWrapSession(s).ExecBatch(ctx, &batch, testOptions(options(idempotent)))
			return err
		},
		"RowStream": func(ctx context.Context, idempotent bool) error {
			s, _, peer := rowStreamSession(t, 4, nil)
			defer s.Close()
			go refuse(peer)
			_, err := RunStream(ctx, s, secretQuery(t, cql.OrdinaryStatement), cql.StreamOneColumn(cql.StreamBlob), StreamOptions{Options: options(idempotent), Incremental: true})
			return err
		},
	}
	for name, run := range paths {
		for _, idempotent := range []bool{false, true} {
			seen = nil
			ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), key{}, name), 5*time.Second)
			err := run(ctx, idempotent)
			cancel()
			failure, ok := errors.AsType[*cqlerr.ExecutionError](err)
			if !ok {
				t.Fatalf("%s: %v, want an ExecutionError", name, err)
			}
			if seen != name {
				t.Errorf("%s: the retry policy's Attempt.Context held %v", name, seen)
			}
			if failure.Idempotent != idempotent {
				t.Errorf("%s: Idempotent = %v, want %v (err %v)", name, failure.Idempotent, idempotent, err)
			}
		}
	}
}
