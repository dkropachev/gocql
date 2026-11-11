// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
)

type countingBlobEncoder struct{ calls *int }

func (c countingBlobEncoder) CheckType(t cqltype.DataType) error {
	return codec.BytesEncoder.CheckType(t)
}
func (c countingBlobEncoder) Encode(w *codec.ValueWriter, b []byte) error {
	*c.calls++
	return codec.BytesEncoder.Encode(w, b)
}
