// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/observe"
	"github.com/scylladb/gocql/v2/retry"
	"github.com/scylladb/gocql/v2/routing"
)

// TestLayeredOptionsSpeculationWithTrace lays call options that ask for a
// CQL trace over view defaults that speculate: the result does both, keeps
// the view's consistency and takes the call's timeout.
func TestLayeredOptionsSpeculationWithTrace(t *testing.T) {
	speculation := constantSpeculation{delay: time.Millisecond}
	view := testOptions(Options{Speculation: speculation, Consistency: cql.Some(cql.Quorum), Timeout: cql.Some(time.Second)})
	call := testOptions(Options{CQLTrace: true, Timeout: cql.Some(time.Minute)})
	layered, err := call.layeredOver(view)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolveOptionValues(nil, layered.Values())
	if err != nil {
		t.Fatal(err)
	}
	if resolved.InternalProfile.Speculation != speculation || !resolved.InternalOptions.CQLTrace {
		t.Fatalf("layered options speculate with %v and trace %v, want both", resolved.InternalProfile.Speculation, resolved.InternalOptions.CQLTrace)
	}
	if resolved.InternalProfile.Consistency != cql.Quorum || resolved.InternalProfile.Timeout != time.Minute {
		t.Fatalf("layered options resolve to %v and %v, want QUORUM and the call's minute", resolved.InternalProfile.Consistency, resolved.InternalProfile.Timeout)
	}
	// The call disables the view's speculation.
	off, err := testOptions(Options{Speculation: retry.NoSpeculation{}}).layeredOver(view)
	if err != nil {
		t.Fatal(err)
	}
	if resolved, _ := resolveOptionValues(nil, off.Values()); resolved.InternalProfile.Speculation != nil {
		t.Fatalf("NoSpeculation over a speculating view resolves to %v", resolved.InternalProfile.Speculation)
	}
}

// TestLayeredOptionsCache checks that laying the same call options over the
// same view again returns the cached result without allocating, that
// another view replaces the cache, and that a zero side returns the other.
func TestLayeredOptionsCache(t *testing.T) {
	view := testOptions(Options{Consistency: cql.Some(cql.Quorum)})
	other := testOptions(Options{Consistency: cql.Some(cql.Two)})
	call := testOptions(Options{PageSize: 3})
	first, err := call.layeredOver(view)
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := call.layeredOver(view); again.p != first.p {
		t.Fatal("the same options over the same view compiled again")
	}
	if allocs := testing.AllocsPerRun(100, func() { _, _ = call.layeredOver(view) }); allocs != 0 {
		t.Fatalf("a cached layering allocated %v times", allocs)
	}
	second, _ := call.layeredOver(other)
	if second.p == first.p || second.Values().Consistency.Value != cql.Two || second.Values().PageSize != 3 {
		t.Fatalf("options over another view = %+v", second.Values())
	}
	if got, _ := call.layeredOver(CompiledOptions{}); got.p != call.p {
		t.Fatal("options over zero defaults are not the options themselves")
	}
	if got, _ := (CompiledOptions{}).layeredOver(view); got.p != view.p {
		t.Fatal("zero options over defaults are not the defaults themselves")
	}
	// A combination that fails is cached too, and fails before sending.
	unpaged := testOptions(Options{PageSize: cql.Unpaged})
	prefetch := testOptions(Options{Prefetch: cql.Some(1)})
	_, err = prefetch.layeredOver(unpaged)
	if !hasErrorType[*ExecutionError](err) {
		t.Fatalf("prefetch over an unpaged view = %v, want an ExecutionError", err)
	}
	if _, again := prefetch.layeredOver(unpaged); !errors.Is(again, err) {
		t.Fatalf("the failed combination was not cached: %v", again)
	}
}

// TestOverlayOptions checks overlayOptions field by field: each field over
// sets wins, unset fields keep base's, flags add up, observers join and a
// host pin brings its shard with it.
func TestOverlayOptions(t *testing.T) {
	hostA, hostB := cql.UUID{1}, cql.UUID{2}
	base := Options{
		Keyspace: "a", PageSize: 10, Consistency: cql.Some(cql.Quorum), SerialConsistency: cql.Some(cql.LocalSerial),
		HostID: cql.Some(hostA), ShardID: cql.Some(uint32(1)), AnyShardFallback: true,
		CQLTrace: true, Idempotent: cql.Some(true), CustomPayload: []cql.PayloadEntry{{Key: "a"}},
	}
	out := overlayOptions(base, Options{Keyspace: "b", Consistency: cql.Some(cql.One), HostID: cql.Some(hostB), IncludeMetadata: true})
	if out.Keyspace != "b" || out.Consistency.Value != cql.One || out.PageSize != 10 || out.SerialConsistency != cql.Some(cql.LocalSerial) {
		t.Fatalf("overlay = %+v", out)
	}
	if out.HostID.Value != hostB || out.ShardID.Set || out.AnyShardFallback {
		t.Fatalf("a host pin kept the base's shard: %+v", out)
	}
	if !out.CQLTrace || !out.IncludeMetadata || !out.Idempotent.Value || len(out.CustomPayload) != 1 {
		t.Fatalf("overlay lost base fields: %+v", out)
	}
	var calls []string
	hooks := func(name string) observe.Hooks {
		return observe.Hooks{Attempt: func(context.Context, routing.AttemptStart) func(routing.AttemptEvent) {
			calls = append(calls, name)
			return nil
		}}
	}
	joined := overlayOptions(Options{Observer: hooks("view")}, Options{Observer: hooks("call")})
	joined.Observer.Attempt(t.Context(), routing.AttemptStart{})
	if len(calls) != 2 || calls[0] != "view" || calls[1] != "call" {
		t.Fatalf("joined observers ran %v, want view then call", calls)
	}
}
