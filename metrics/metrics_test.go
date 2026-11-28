// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metrics_test

import (
	"testing"

	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/metrics"
)

func TestSnapshotErrorsOf(t *testing.T) {
	s := metrics.Snapshot{ErrorsByKind: map[cqlerr.Kind]uint64{cqlerr.KindTimeout: 3}}
	if got := s.ErrorsOf(cqlerr.KindTimeout); got != 3 {
		t.Fatalf("ErrorsOf(KindTimeout) = %d, want 3", got)
	}
	if got := s.ErrorsOf(cqlerr.KindOverloaded); got != 0 {
		t.Fatalf("ErrorsOf(KindOverloaded) = %d, want 0", got)
	}
	if got := (metrics.Snapshot{}).ErrorsOf(cqlerr.KindTimeout); got != 0 {
		t.Fatalf("zero Snapshot: ErrorsOf = %d, want 0", got)
	}
}
