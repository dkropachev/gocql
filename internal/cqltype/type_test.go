// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

import (
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

func FoundationScalar(t *testing.T, kind cql.Type) cql.DataType {
	t.Helper()
	d, err := cql.ScalarType(kind)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
