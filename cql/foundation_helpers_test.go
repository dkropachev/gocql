// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
	"testing"
)

func FoundationScalar(t *testing.T, kind Type) DataType {
	t.Helper()
	d, err := ScalarType(kind)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// hasErrorType reports whether err's tree holds an error of type E.
func hasErrorType[E error](err error) bool {
	_, ok := errors.AsType[E](err)
	return ok
}

// decodeRow decodes one row with plan into a new R, as the engine does for a
// nil destination.
func decodeRow[R any](plan CompiledRow[R], reader *RowReader) (R, error) {
	var row R
	err := plan.DecodeInto(reader, &row)
	return row, err
}
