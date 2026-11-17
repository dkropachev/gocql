// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"bytes"
	"errors"

	"github.com/scylladb/gocql/v2/cql"
)

type countingBlobEncoder struct{ calls *int }

func (c countingBlobEncoder) CheckType(t cql.DataType) error {
	return cql.ByteSliceCodec.CheckType(t)
}
func (c countingBlobEncoder) Encode(w *cql.ValueWriter, b []byte) error {
	*c.calls++
	return cql.ByteSliceCodec.Encode(w, b)
}

var errFailingBlob = errors.New("failing blob")

// failingBlobEncoder encodes like ByteSliceCodec, then fails for the value
// "fail" after writing it.
type failingBlobEncoder struct{}

func (failingBlobEncoder) CheckType(t cql.DataType) error {
	return cql.ByteSliceCodec.CheckType(t)
}
func (failingBlobEncoder) Encode(w *cql.ValueWriter, b []byte) error {
	if err := cql.ByteSliceCodec.Encode(w, b); err != nil {
		return err
	}
	if bytes.Equal(b, []byte("fail")) {
		return errFailingBlob
	}
	return nil
}
