// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
)

func TestSimpleConvictionPolicy(t *testing.T) {
	var p SimpleConvictionPolicy
	cause := errors.New("connection refused")
	for _, source := range []ConvictionSource{ConvictionPool, ConvictionControl} {
		if !p.AddFailure(ConvictionEvent{Err: cause, Source: source}) {
			t.Fatal("whole-host failure not convicted", source)
		}
	}
	if p.AddFailure(ConvictionEvent{Err: cause, Shard: cqltype.Some(uint32(3)), Source: ConvictionPool}) {
		t.Fatal("single-shard failure convicted the host")
	}
}
