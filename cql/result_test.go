// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import "testing"

func TestResultDiagnostics(t *testing.T) {
	t.Run("ZeroValue", func(t *testing.T) {
		var empty Result
		if empty.Warnings() != nil || empty.CQLTraces() != nil || empty.CustomPayload() != nil {
			t.Fatal("empty result reports diagnostics")
		}
	})

	t.Run("AddWarnings", func(t *testing.T) {
		a := Result{}
		a.addWarnings("w1")
		b := a // copies share nothing that a later append can change
		a.addWarnings("w2")
		b.addWarnings("other")
		if got := a.Warnings(); len(got) != 2 || got[1] != "w2" {
			t.Fatal(got)
		}
		if got := b.Warnings(); len(got) != 2 || got[1] != "other" {
			t.Fatal(got)
		}
	})

	t.Run("MergeResult", func(t *testing.T) {
		var a, b, merged Result
		a.addWarnings("w1", "w2")
		b.addWarnings("w1", "other")
		mergeResult(&merged, a)
		mergeResult(&merged, b)
		if len(merged.Warnings()) != 4 || len(a.Warnings()) != 2 {
			t.Fatal(merged.Warnings(), a.Warnings())
		}
	})

	t.Run("CloneResult", func(t *testing.T) {
		var r Result
		r.addWarnings("w1", "w2")
		clone := cloneResult(r)
		clone.Warnings()[0] = "changed"
		if r.Warnings()[0] != "w1" {
			t.Fatal("clone shares storage")
		}
	})

	t.Run("SetDiagnostics", func(t *testing.T) {
		var r Result
		r.addWarnings("w1")
		r.setDiagnostics(nil, nil, nil)
		if r.Warnings() != nil {
			t.Fatal("SetDiagnostics did not clear")
		}
	})
}
