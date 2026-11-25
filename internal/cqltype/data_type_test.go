// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

import (
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

func TestDataType(t *testing.T) {

	t.Run("DataTypeFromInfo", func(t *testing.T) {
		native := NewNativeType(4, cql.KindInt)
		input := NewUDTType(4, "record", "ks", UDTField{Name: "x", Type: NewCollectionType(NewNativeType(4, cql.KindList), nil, native)})
		descriptor, err := DataTypeFromInfo(input)
		if err != nil {
			t.Fatal(err)
		}
		field, _ := descriptor.Fields().At(0)
		element, ok := field.Type.Element()
		if !ok || element.Kind() != cql.KindInt {
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
		if c.Keyspace != "ks" || c.Table != "tbl" || c.Name != "value" || c.Type.Kind() != cql.KindInt {
			t.Fatalf("wrong column: %+v", c)
		}
		var nilNative *NativeType
		if _, err = DataTypeFromInfo(nilNative); err == nil {
			t.Fatal("typed nil accepted")
		}
		cyc := &CollectionType{NativeType: NewNativeType(4, cql.KindList)}
		cyc.Elem = cyc
		if _, err = DataTypeFromInfo(cyc); err == nil {
			t.Fatal("recursive external descriptor accepted")
		}
		for _, n := range []int{-1, 0, 65536} {
			if _, err = DataTypeFromInfo(VectorType{NativeType: NewNativeType(4, cql.KindCustom), SubType: native, Dimensions: n}); err == nil {
				t.Fatalf("invalid dimension %d accepted", n)
			}
		}
	})
}

// The protocol's legacy text ID becomes cql.KindText, as varchar's ID does.
func TestLegacyTextID(t *testing.T) {
	for _, id := range []cql.Kind{0x000A, cql.KindText} {
		native := NewNativeType(4, id)
		if native.Type() != cql.KindText || native.String() != "text" {
			t.Fatalf("id %#x: %v %s", uint16(id), native.Type(), native)
		}
		dt, err := DataTypeFromInfo(native)
		if err != nil || !dt.Equal(cql.TextType) {
			t.Fatalf("id %#x: %v %v", uint16(id), dt, err)
		}
	}
}
