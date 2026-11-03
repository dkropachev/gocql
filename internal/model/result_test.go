// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package model

import "testing"

func TestResultDiagnosticsAreCopyOnWrite(t *testing.T) {
	var empty Result
	if empty.Warnings() != nil || empty.TraceIDs() != nil || empty.CustomPayload() != nil {
		t.Fatal("empty result reports diagnostics")
	}
	a := Result{}
	a.AddWarnings("w1")
	b := a // copies share nothing that a later append can change
	a.AddWarnings("w2")
	b.AddWarnings("other")
	if got := a.Warnings(); len(got) != 2 || got[1] != "w2" {
		t.Fatal(got)
	}
	if got := b.Warnings(); len(got) != 2 || got[1] != "other" {
		t.Fatal(got)
	}
	var merged Result
	MergeResult(&merged, a)
	MergeResult(&merged, b)
	if len(merged.Warnings()) != 4 || len(a.Warnings()) != 2 {
		t.Fatal(merged.Warnings(), a.Warnings())
	}
	clone := CloneResult(merged)
	clone.Warnings()[0] = "changed"
	if merged.Warnings()[0] != "w1" {
		t.Fatal("clone shares storage")
	}
	merged.SetDiagnostics(nil, nil, nil)
	if merged.Warnings() != nil {
		t.Fatal("SetDiagnostics did not clear")
	}
}
