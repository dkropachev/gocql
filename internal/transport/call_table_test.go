//go:build unit
// +build unit

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import "testing"

func TestCallTable(t *testing.T) {
	var nilTable *InternalCallTable
	if nilTable.Get(1) != nil || nilTable.Delete(1) != nil || nilTable.Len() != 0 {
		t.Fatal("a nil table must read as empty")
	}
	for range nilTable.All() {
		t.Fatal("a nil table must yield nothing")
	}

	tbl := InternalNewCallTable()
	a, b, c := &InternalCallReq{}, &InternalCallReq{}, &InternalCallReq{}
	tbl.Set(0, a)
	tbl.Set(32767, b)
	tbl.Set(65, c)
	tbl.Set(65, c) // overwrite keeps the count
	if tbl.Len() != 3 {
		t.Fatalf("Len = %d, want 3", tbl.Len())
	}
	if tbl.Get(0) != a || tbl.Get(32767) != b || tbl.Get(65) != c {
		t.Fatal("Get returned the wrong call")
	}
	if tbl.Get(-1) != nil || tbl.Get(64) != nil || tbl.Get(1<<20) != nil {
		t.Fatal("unset or out-of-range streams must read as nil")
	}
	var order []int
	for stream := range tbl.All() {
		order = append(order, stream)
	}
	if len(order) != 3 || order[0] != 0 || order[1] != 65 || order[2] != 32767 {
		t.Fatalf("All yielded %v", order)
	}
	if tbl.Delete(65) != c || tbl.Delete(65) != nil || tbl.Len() != 2 {
		t.Fatal("Delete must remove once and keep the count")
	}
	tbl.Set(0, nil)
	if tbl.Get(0) != nil || tbl.Len() != 1 {
		t.Fatal("Set(nil) must delete")
	}
}
