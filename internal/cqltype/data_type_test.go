// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

import (
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

func TestDataType(t *testing.T) {
	t.Run("Immutability", func(t *testing.T) {
		integer := FoundationScalar(t, cql.TypeInt)
		text := FoundationScalar(t, cql.TypeText)
		fields := []cql.ColumnSpec{{Name: "a", Type: integer}, {Name: "b", Type: text}}
		udt, err := cql.UDTType("ks", "record", fields...)
		if err != nil {
			t.Fatal(err)
		}
		fields[0] = cql.ColumnSpec{Name: "changed", Type: text}
		first, _ := udt.Fields().At(0)
		if first.Name != "a" || !first.Type.Equal(integer) {
			t.Fatal("constructor retained mutable input")
		}
		exposed := udt.Fields()
		cql.ColumnSlice(exposed)[0] = fields[0]
		again, _ := udt.Fields().At(0)
		if again.Name != "a" {
			t.Fatal("Fields exposed descriptor storage")
		}
		columns := cql.NewColumns(cql.ColumnSpec{Name: "record", Type: udt})
		c, _ := columns.At(0)
		c.Name = "changed" //nolint:govet // writes the copy At returned, to prove it is one
		original, _ := columns.At(0)
		if original.Name != "record" {
			t.Fatal("At exposed mutable column")
		}
		frozen := udt.WithFrozen(true)
		if udt.Frozen() || udt.FrozenKnown() || !frozen.Frozen() || !frozen.FrozenKnown() || !udt.Equal(frozen) || cql.Fingerprint(udt) != cql.Fingerprint(frozen) {
			t.Fatal("frozen annotation changed wire compatibility or was lost")
		}
		changed, _ := cql.UDTType("ks", "record", cql.ColumnSpec{Name: "other", Type: integer}, cql.ColumnSpec{Name: "b", Type: text})
		if udt.Equal(changed) || cql.Fingerprint(udt) == cql.Fingerprint(changed) {
			t.Fatal("UDT names missing from identity")
		}
		list, _ := cql.ListType(udt)
		different, _ := cql.ListType(changed)
		if list.Equal(different) || cql.Fingerprint(list) == cql.Fingerprint(different) {
			t.Fatal("nested identity lost")
		}
		mapType, _ := cql.MapType(integer, list)
		k, v, ok := mapType.KeyValue()
		if !ok || !k.Equal(integer) || !v.Equal(list) {
			t.Fatal("map children lost")
		}
		vector, _ := cql.VectorType(mapType, 17)
		dimension, ok := vector.Dimension()
		if !ok || dimension != 17 {
			t.Fatal("dimension lost")
		}
		element, ok := vector.Element()
		if !ok || !element.Equal(mapType) {
			t.Fatal("vector element lost")
		}
		if _, ok := columns.At(-1); ok {
			t.Fatal("negative index accepted")
		}
		if _, ok := columns.At(1); ok {
			t.Fatal("past-end index accepted")
		}
	})

	t.Run("Constructors", func(t *testing.T) {
		t.Run("Validation", func(t *testing.T) {
			integer := FoundationScalar(t, cql.TypeInt)
			cases := []func() (cql.DataType, error){func() (cql.DataType, error) { return cql.ScalarType(cql.TypeList) }, func() (cql.DataType, error) { return cql.ScalarType(cql.TypeCustom) }, func() (cql.DataType, error) { return cql.ListType(cql.DataType{}) }, func() (cql.DataType, error) { return cql.MapType(integer, cql.DataType{}) }, func() (cql.DataType, error) { return cql.VectorType(integer, 0) }, func() (cql.DataType, error) { return cql.UDTType("", "record") }, func() (cql.DataType, error) {
				return cql.UDTType("ks", "record", cql.ColumnSpec{Name: "x", Type: integer}, cql.ColumnSpec{Name: "x", Type: integer})
			}}
			for i, test := range cases {
				_, err := test()
				if _, ok := errors.AsType[*cql.ValidationError](err); !ok {
					t.Errorf("case %d: expected ValidationError, got %v", i, err)
				}
			}
			if err := cql.ValidateDataType(cql.DataType{}.WithFrozen(true)); err == nil {
				t.Fatal("zero descriptor became valid")
			}
		})
	})

	t.Run("ValidateScalarKind", func(t *testing.T) {
		for kind := cql.TypeAscii; kind <= cql.TypeDuration; kind++ {
			if err := cql.ValidateScalarKind(kind); err != nil {
				t.Fatalf("valid scalar %v: %v", kind, err)
			}
			if allocations := testing.AllocsPerRun(100, func() {
				if err := cql.ValidateScalarKind(kind); err != nil {
					panic(err)
				}
			}); allocations != 0 {
				t.Fatalf("scalar %v validation allocated: %g", kind, allocations)
			}
		}
		for _, kind := range []cql.Type{-1, cql.TypeCustom, cql.TypeDuration + 1, cql.TypeList, cql.TypeMap, cql.TypeSet, cql.TypeTuple, cql.TypeUDT, 65535} {
			err := cql.ValidateScalarKind(kind)
			if validation, ok := errors.AsType[*cql.ValidationError](err); !ok || validation.Field != "kind" {
				t.Fatalf("unsupported scalar %v: expected kind ValidationError, got %v", kind, err)
			}
			if _, constructorErr := cql.ScalarType(kind); constructorErr == nil || constructorErr.Error() != err.Error() {
				t.Fatalf("constructor and validation disagree for %v: %v / %v", kind, constructorErr, err)
			}
		}
	})

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

	t.Run("Predeclared", func(t *testing.T) {
		t.Run("Shared", func(t *testing.T) {
			predeclared := map[cql.Type]cql.DataType{
				cql.TypeAscii: cql.ASCIIType, cql.TypeBigInt: cql.BigIntType, cql.TypeBlob: cql.BlobType, cql.TypeBoolean: cql.BooleanType,
				cql.TypeCounter: cql.CounterType, cql.TypeDecimal: cql.DecimalType, cql.TypeDouble: cql.DoubleType, cql.TypeFloat: cql.FloatType,
				cql.TypeInt: cql.IntType, cql.TypeText: cql.TextType, cql.TypeTimestamp: cql.TimestampType, cql.TypeUUID: cql.UUIDType,
				cql.TypeVarchar: cql.VarcharType, cql.TypeVarint: cql.VarintType, cql.TypeTimeUUID: cql.TimeUUIDType, cql.TypeInet: cql.InetType,
				cql.TypeDate: cql.DateType, cql.TypeTime: cql.TimeType, cql.TypeSmallInt: cql.SmallIntType, cql.TypeTinyInt: cql.TinyIntType,
				cql.TypeDuration: cql.DurationType,
			}
			for kind := cql.TypeAscii; kind <= cql.TypeDuration; kind++ {
				want, ok := predeclared[kind]
				if !ok {
					t.Fatalf("no predeclared type for %v", kind)
				}
				got, err := cql.ScalarType(kind)
				if err != nil {
					t.Fatal(err)
				}
				if cql.DescriptorOf(got) != cql.DescriptorOf(want) || got.Kind() != kind {
					t.Fatalf("ScalarType(%v) = %v, want shared %v", kind, got, want)
				}
			}
			if _, err := cql.ScalarType(cql.TypeList); err == nil {
				t.Fatal("ScalarType accepted a collection kind")
			}
			if allocs := testing.AllocsPerRun(100, func() { _, _ = cql.ScalarType(cql.TypeText) }); allocs != 0 {
				t.Fatalf("ScalarType allocated %v times", allocs)
			}
		})
	})
}
