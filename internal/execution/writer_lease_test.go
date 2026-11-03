// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// TestInputOwnerReusesItsWriterLeaseOnlyWhenFree checks that the embedded
// writer lease is handed out again once released, that a second writer
// while it is held gets a lease of its own, and that every lease holds and
// returns one owner reference.
func TestInputOwnerReusesItsWriterLeaseOnlyWhenFree(t *testing.T) {
	o := InternalNewInputOwner()
	first := o.InternalWriterLease(wire.Chain{})
	second := o.InternalWriterLease(wire.Chain{})
	if first == second {
		t.Fatal("a held writer lease was handed out again")
	}
	if first != &o.writer.Lease {
		t.Fatal("first writer did not use the embedded lease")
	}
	if got := o.InternalRefs(); got != 2 {
		t.Fatal("writer references", got)
	}
	second.Release()
	first.Release()
	if got := o.InternalRefs(); got != 0 {
		t.Fatal("references after release", got)
	}
	again := o.InternalWriterLease(wire.Chain{})
	if again != first || !again.Alive() {
		t.Fatal("released embedded lease not reused")
	}
	again.Release()
	o.InternalClose()
	select {
	case <-o.done:
	default:
		t.Fatal("owner did not drain")
	}
}
