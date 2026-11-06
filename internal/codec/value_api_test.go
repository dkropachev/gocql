// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"errors"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

type foundationCustomBlob struct {
	checks, encodes, decodes *int
	reject, errorAfterWrite  bool
}

func (c foundationCustomBlob) CheckType(t cqltype.DataType) error {
	if c.checks != nil {
		*c.checks++
	}
	if c.reject || t.Kind() != cqltype.TypeBlob {
		return errors.New("unexpected type")
	}
	return nil
}
func (c foundationCustomBlob) Encode(w *ValueWriter, v []byte) error {
	if c.encodes != nil {
		*c.encodes++
	}
	if err := w.BorrowBytes(v); err != nil {
		return err
	}
	if c.errorAfterWrite {
		return errors.New("custom encoder failure")
	}
	return nil
}
func (c foundationCustomBlob) Decode(r ValueReader) ([]byte, error) {
	if c.decodes != nil {
		*c.decodes++
	}
	return r.Bytes().OwnedBytes(), nil
}
func (c foundationCustomBlob) Clone(v []byte) []byte { return append([]byte(nil), v...) }

// codecTestMust unwraps checked constructors for known-valid fixture values.
func CodecTestMust[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
