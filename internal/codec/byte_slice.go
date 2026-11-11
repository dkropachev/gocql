// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"fmt"

	"github.com/scylladb/gocql/v2/internal/cqltype"
)

// ByteSliceCodec encodes a []byte and decodes a blob into caller-owned
// storage, as StringCodec does for text. With RowInto, a seeded slice is
// reused whenever its capacity accommodates the complete cell. The resulting
// bytes remain valid after the response page is released. BlobCodec decodes
// without copying.
var ByteSliceCodec Codec[[]byte] = byteSliceCodec{}

type byteSliceCodec struct{ compiled bool }

func (byteSliceCodec) CheckType(t cqltype.DataType) error {
	perfCheckType()

	if t.Kind() != cqltype.TypeBlob {
		return fmt.Errorf("gocql: blob codec cannot use %s", t.Kind())
	}
	return nil
}
func (c byteSliceCodec) Encode(w *ValueWriter, v []byte) error {
	if e := c.typeGuard(w.Type()); e != nil {
		return e
	}
	return w.BorrowBytes(v)
}
func (c byteSliceCodec) typeGuard(t cqltype.DataType) error {
	if c.compiled {
		return checkScalarType(t, cqltype.TypeBlob, 0)
	}
	return c.CheckType(t)
}
func (c byteSliceCodec) compileValueEncoder(t cqltype.DataType) (Encoder[[]byte], error) {
	if e := c.CheckType(t); e != nil {
		return nil, e
	}
	c.compiled = true
	return c, nil
}
func (c byteSliceCodec) Decode(r ValueReader) ([]byte, error) {
	var dst []byte
	err := c.DecodeInto(r, &dst)
	return dst, err
}
func (c byteSliceCodec) DecodeInto(r ValueReader, dst *[]byte) error {
	if dst == nil {
		return cqltype.TypeValidation("destination", "nil blob destination")
	}
	if err := checkScalarType(r.Type(), cqltype.TypeBlob, 0); err != nil {
		return err
	}
	if err := requirePresent(r); err != nil {
		return err
	}
	n := int(r.body.Len())
	if cap(*dst) < n {
		*dst = make([]byte, n)
	} else {
		*dst = (*dst)[:n]
	}
	_, err := r.body.CopyTo(*dst)
	return err
}
func (byteSliceCodec) decodeColumnInto(r *RowReader, dst *[]byte) error {
	n, err := r.scalarColumn(cqltype.TypeBlob, 0, true, 0)
	if err != nil {
		return err
	}
	if cap(*dst) < int(n) {
		*dst = make([]byte, int(n))
	} else {
		*dst = (*dst)[:int(n)]
	}
	_, err = r.cursor.Read(*dst)
	return err
}
func (byteSliceCodec) Clone(value []byte) []byte { return append([]byte(nil), value...) }
