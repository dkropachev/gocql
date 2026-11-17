// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"reflect"
	"testing"
)

// TestParamCellLayout proves CaptureParamCells may hand cql a []ParamCell as
// its own cell type.
func TestParamCellLayout(t *testing.T) {
	pub, rep := paramCellType(), reflect.TypeFor[ParamCell]()
	if pub.Size() != rep.Size() || pub.Align() != rep.Align() || pub.NumField() != rep.NumField() {
		t.Fatalf("cql cell %v (size %d) and ParamCell (size %d) differ", pub, pub.Size(), rep.Size())
	}
	for i := range pub.NumField() {
		p, r := pub.Field(i), rep.Field(i)
		if p.Offset != r.Offset || p.Type != r.Type || p.Name != r.Name {
			t.Fatalf("field %d: cql has %s %v at %d, ParamCell has %s %v at %d", i, p.Name, p.Type, p.Offset, r.Name, r.Type, r.Offset)
		}
	}
}
