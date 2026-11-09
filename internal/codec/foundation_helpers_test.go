// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"errors"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

func FoundationScalar(t *testing.T, kind cqltype.Type) cqltype.DataType {
	t.Helper()
	d, err := cqltype.ScalarType(kind)
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
