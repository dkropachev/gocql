// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

import (
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

// TestSimpleConvictionPolicy checks that Reset, which keeps no state,
// leaves the policy's decisions unchanged.
func TestSimpleConvictionPolicy(t *testing.T) {
	var p SimpleConvictionPolicy
	hostWide := ConvictionEvent{Err: errors.New("refused")}
	oneShard := ConvictionEvent{Err: errors.New("refused"), ShardID: cql.Some[uint32](3)}
	if !p.AddFailure(hostWide) {
		t.Fatal("a host-wide failure did not convict")
	}
	if p.AddFailure(oneShard) {
		t.Fatal("a single-shard failure convicted")
	}
	// Reset keeps no state: the next host-wide failure still convicts and a
	// shard failure still does not.
	p.Reset(ConvictionEvent{})
	p.Reset(ConvictionEvent{ShardID: cql.Some[uint32](3)})
	if !p.AddFailure(hostWide) || p.AddFailure(oneShard) {
		t.Fatal("Reset changed the policy's decisions")
	}
	var _ ConvictionPolicy = &p
}
