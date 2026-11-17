// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

import (
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

func TestDataType(t *testing.T) {

	t.Run("DataTypeFromInfo", func(t *testing.T) {
		native := NewNativeType(4, cql.TypeInt)
		input := NewUDTType(4, "record", "ks", UDTField{Name: "x", Type: NewCollectionType(NewNativeType(4, cql.TypeList), nil, native)})
		descriptor, err := DataTypeFromInfo(input)
		if err != nil {
			t.Fatal(err)
		}
		field, _ := descriptor.Fields().At(0)
		element, ok := field.Type.Element()
		if !ok || element.Kind() != cql.TypeInt {
			t.Fatal("nested conversion lost type")
		}
		input.Elements[0].Name = "mutated"
		field, _ = descriptor.Fields().At(0)
		if field.Name != "x" {
			t.Fatal("protocol descriptor slices retained")
		}
		columns, err := ColumnsFromInfo([]ColumnInfo{{Keyspace: "ks", Table: "tbl", Name: "value", TypeInfo: native}})
		if err != nil {
			t.Fatal(err)
		}
		c, _ := columns.At(0)
		if c.Keyspace != "ks" || c.Table != "tbl" || c.Name != "value" || c.Type.Kind() != cql.TypeInt {
			t.Fatalf("wrong column: %+v", c)
		}
		var nilNative *NativeType
		if _, err = DataTypeFromInfo(nilNative); err == nil {
			t.Fatal("typed nil accepted")
		}
		cyc := &CollectionType{NativeType: NewNativeType(4, cql.TypeList)}
		cyc.Elem = cyc
		if _, err = DataTypeFromInfo(cyc); err == nil {
			t.Fatal("recursive external descriptor accepted")
		}
		for _, n := range []int{-1, 0, 65536} {
			if _, err = DataTypeFromInfo(VectorType{NativeType: NewNativeType(4, cql.TypeCustom), SubType: native, Dimensions: n}); err == nil {
				t.Fatalf("invalid dimension %d accepted", n)
			}
		}
	})
}
