// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session_test

import (
	"github.com/scylladb/gocql/v2/cql"
)

type countingBlobEncoder struct{ calls *int }

func (c countingBlobEncoder) CheckType(t cql.DataType) error { return cql.BytesEncoder.CheckType(t) }
func (c countingBlobEncoder) Encode(w *cql.ValueWriter, b []byte) error {
	*c.calls++
	return cql.BytesEncoder.Encode(w, b)
}
