// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// Wrapping the public decoder interface deliberately keeps custom codecs on
// the generic immutable ValueReader path.
type genericRowScalar[T any] struct{ Decoder[T] }

func (g genericRowScalar[T]) compileValueDecoder(typ cqltype.DataType) (Decoder[T], error) {
	decoder, err := compileValueDecoder(g.Decoder, typ)
	return genericRowScalar[T]{decoder}, err
}

func rowScalarFrame(prefix int32, body []byte) []byte {
	return append(binary.BigEndian.AppendUint32(nil, uint32(prefix)), body...)
}

func checkRowScalarEquivalent[T any](t *testing.T, kind cqltype.Type, decoder Decoder[T], input []byte, missing, expired bool) {
	t.Helper()
	for _, fragment := range []int{1, 3, 1024} {
		t.Run(fmt.Sprint(fragment), func(t *testing.T) {
			var spans []wire.Span
			for left := input; len(left) > 0; {
				n := min(fragment, len(left))
				spans = append(spans, wire.Borrow(left[:n]))
				left = left[n:]
			}
			chain, err := wire.NewChain(spans...)
			if err != nil {
				t.Fatal(err)
			}
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: rowTestType(t, kind)})
			lease := wire.NewLease(chain, nil)
			if expired {
				lease.Release()
			} else {
				defer lease.Release()
			}
			direct, generic := NewRowReader(columns, chain, lease), NewRowReader(columns, chain, lease)
			direct.allowMissing, generic.allowMissing = missing, missing
			var got, want T
			directErr := readColumnInto(direct, decoder, &got)
			genericErr := readColumnInto(generic, genericRowScalar[T]{decoder}, &want)
			if fmt.Sprint(directErr) != fmt.Sprint(genericErr) {
				t.Fatalf("error direct=%v generic=%v", directErr, genericErr)
			}
			if direct.index != generic.index || direct.cursor.Remaining() != generic.cursor.Remaining() {
				t.Fatalf("cursor direct=(%d,%d) generic=(%d,%d)", direct.index, direct.cursor.Remaining(), generic.index, generic.cursor.Remaining())
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("value direct=%#v generic=%#v", got, want)
			}
		})
	}
}

type capturedRowInt struct {
	Decoder[int32]
	seen  *ValueReader
	calls *int
}

func (c capturedRowInt) Decode(r ValueReader) (int32, error) {
	*c.seen = r
	*c.calls++
	return c.Decoder.Decode(r)
}
