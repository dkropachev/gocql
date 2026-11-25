// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

//go:build !race

package binding

import "testing"

// TestConditionalCQLAllocs checks that inferring a statement's kind, which
// runs on every unprepared call, does not allocate for lower-case CQL.
func TestConditionalCQLAllocs(t *testing.T) {
	text := "update ks.t set v = 1, w = 'x' where k = 1 and c = 2 -- note\nif v = 0"
	if n := testing.AllocsPerRun(100, func() { _ = InternalConditionalCQL(text) }); n != 0 {
		t.Errorf("InternalConditionalCQL allocates %v times", n)
	}
	if n := testing.AllocsPerRun(100, func() { _ = firstCQLWord(text) }); n != 0 {
		t.Errorf("firstCQLWord allocates %v times", n)
	}
}
