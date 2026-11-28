// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package observe

import (
	"errors"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

type failingWriter struct{}

var errWrite = errors.New("write failed")

func (failingWriter) Write([]byte) (int, error) { return 0, errWrite }

func TestBoundValues(t *testing.T) {
	var zero BoundValues
	var b strings.Builder
	if zero.Len() != 0 || zero.Format(&b, nil) != nil || b.Len() != 0 {
		t.Fatalf("zero BoundValues: len %d, wrote %q", zero.Len(), b.String())
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("At out of range did not panic")
			}
		}()
		zero.At(0)
	}()
	// A marker without a name is written as its position; redacted values
	// are not decoded.
	columns := cql.NewColumns(cql.ColumnSpec{Name: "k", Type: cql.IntType}, cql.ColumnSpec{Type: cql.IntType})
	v := newBoundValues(columns, 2, nil, func(any, int) cql.ValueReader { return cql.ValueReader{} })
	if err := v.Format(&b, func(cql.ColumnSpec) bool { return true }); err != nil || b.String() != "k=<redacted>, 1=<redacted>" {
		t.Fatalf("redacted: %q, %v", b.String(), err)
	}
	if err := v.Format(failingWriter{}, nil); !errors.Is(err, errWrite) {
		t.Fatalf("Format to a failing writer: %v", err)
	}
}
