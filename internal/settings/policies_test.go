// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/endpoint"
)

func TestSimpleConvictionPolicy(t *testing.T) {
	var p endpoint.SimpleConvictionPolicy
	cause := errors.New("connection refused")
	for _, source := range []endpoint.ConvictionSource{endpoint.ConvictionPool, endpoint.ConvictionControl} {
		if !p.AddFailure(endpoint.ConvictionEvent{Err: cause, Source: source}) {
			t.Fatal("whole-host failure not convicted", source)
		}
	}
	if p.AddFailure(endpoint.ConvictionEvent{Err: cause, ShardID: cql.Some(uint32(3)), Source: endpoint.ConvictionPool}) {
		t.Fatal("single-shard failure convicted the host")
	}
}
