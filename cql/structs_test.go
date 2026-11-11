// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql_test

import (
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

type point struct{ X, Y int32 }

var pointCodec = cql.NamedScalar(cql.Int64Codec,
	func(p point) int64 { return int64(p.X)<<32 | int64(uint32(p.Y)) },
	func(v int64) point { return point{X: int32(v >> 32), Y: int32(v)} })

// valueMapped and pointerMapped have a field reflection cannot map, so only
// their CQLFields methods make ParamsOf and RowOf succeed.
type valueMapped struct{ At point }

func (valueMapped) CQLFields() cql.StructFields[valueMapped] {
	return cql.StructFields[valueMapped]{
		Params:  []cql.InputField[valueMapped]{cql.Param("at", pointCodec, func(v valueMapped) point { return v.At })},
		Columns: []cql.OutputField[valueMapped]{cql.Column("at", pointCodec, func(v *valueMapped) *point { return &v.At })},
	}
}

type pointerMapped struct{ At point }

func (*pointerMapped) CQLFields() cql.StructFields[pointerMapped] {
	return cql.StructFields[pointerMapped]{
		Params:  []cql.InputField[pointerMapped]{cql.Param("at", pointCodec, func(v pointerMapped) point { return v.At })},
		Columns: []cql.OutputField[pointerMapped]{cql.Column("at", pointCodec, func(v *pointerMapped) *point { return &v.At })},
	}
}

type unmapped struct{ At point }

func TestStructFieldsMethod(t *testing.T) {
	columns := cql.NewColumns(cql.ColumnSpec{Name: "at", Type: cql.BigIntType})
	check := func(t *testing.T, params func() error, rows func() error) {
		t.Helper()
		if err := params(); err != nil {
			t.Fatalf("ParamsOf: %v", err)
		}
		if err := rows(); err != nil {
			t.Fatalf("RowOf: %v", err)
		}
	}
	t.Run("ValueReceiver", func(t *testing.T) {
		check(t, func() error { _, err := cql.MustParamsOf[valueMapped]().Compile(columns); return err },
			func() error { _, err := cql.MustRowOf[valueMapped]().Compile(columns); return err })
	})
	t.Run("PointerReceiver", func(t *testing.T) {
		check(t, func() error { _, err := cql.MustParamsOf[pointerMapped]().Compile(columns); return err },
			func() error { _, err := cql.MustRowOf[pointerMapped]().Compile(columns); return err })
	})
	t.Run("Reflected", func(t *testing.T) {
		if _, err := cql.ParamsOf[unmapped](); err == nil {
			t.Fatal("reflection mapped a field with no codec")
		}
	})
}
