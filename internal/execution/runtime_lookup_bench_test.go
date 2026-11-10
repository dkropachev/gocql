// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"testing"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
)

// BenchmarkInternalRuntimeFor measures the initialized lookup, which every
// statement execution and option resolve takes.
func BenchmarkInternalRuntimeFor(b *testing.B) {
	core := new(implCluster.Core)
	InternalRuntimeFor(core)
	b.ReportAllocs()
	for b.Loop() {
		_ = InternalRuntimeFor(core)
	}
}
