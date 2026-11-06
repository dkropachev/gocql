// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

import (
	"errors"
	"testing"
)

func TestDataType(t *testing.T) {
	t.Run("Immutability", func(t *testing.T) {
		integer := FoundationScalar(t, TypeInt)
		text := FoundationScalar(t, TypeText)
		fields := []ColumnSpec{{Name: "a", Type: integer}, {Name: "b", Type: text}}
		udt, err := UDTType("ks", "record", fields...)
		if err != nil {
			t.Fatal(err)
		}
		fields[0] = ColumnSpec{Name: "changed", Type: text}
		first, _ := udt.Fields().At(0)
		if first.Name != "a" || !first.Type.Equal(integer) {
			t.Fatal("constructor retained mutable input")
		}
		exposed := udt.Fields()
		ColumnSlice(exposed)[0] = fields[0]
		again, _ := udt.Fields().At(0)
		if again.Name != "a" {
			t.Fatal("Fields exposed descriptor storage")
		}
		columns := NewColumns(ColumnSpec{Name: "record", Type: udt})
		c, _ := columns.At(0)
		c.Name = "changed" //nolint:govet // writes the copy At returned, to prove it is one
		original, _ := columns.At(0)
		if original.Name != "record" {
			t.Fatal("At exposed mutable column")
		}
		frozen := udt.WithFrozen(true)
		if udt.Frozen() || udt.FrozenKnown() || !frozen.Frozen() || !frozen.FrozenKnown() || !udt.Equal(frozen) || Fingerprint(udt) != Fingerprint(frozen) {
			t.Fatal("frozen annotation changed wire compatibility or was lost")
		}
		changed, _ := UDTType("ks", "record", ColumnSpec{Name: "other", Type: integer}, ColumnSpec{Name: "b", Type: text})
		if udt.Equal(changed) || Fingerprint(udt) == Fingerprint(changed) {
			t.Fatal("UDT names missing from identity")
		}
		list, _ := ListType(udt)
		different, _ := ListType(changed)
		if list.Equal(different) || Fingerprint(list) == Fingerprint(different) {
			t.Fatal("nested identity lost")
		}
		mapType, _ := MapType(integer, list)
		k, v, ok := mapType.KeyValue()
		if !ok || !k.Equal(integer) || !v.Equal(list) {
			t.Fatal("map children lost")
		}
		vector, _ := VectorDataType(mapType, 17)
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
			integer := FoundationScalar(t, TypeInt)
			cases := []func() (DataType, error){func() (DataType, error) { return ScalarType(TypeList) }, func() (DataType, error) { return ScalarType(TypeCustom) }, func() (DataType, error) { return ListType(DataType{}) }, func() (DataType, error) { return MapType(integer, DataType{}) }, func() (DataType, error) { return VectorDataType(integer, 0) }, func() (DataType, error) { return UDTType("", "record") }, func() (DataType, error) {
				return UDTType("ks", "record", ColumnSpec{Name: "x", Type: integer}, ColumnSpec{Name: "x", Type: integer})
			}}
			for i, test := range cases {
				_, err := test()
				var validation *ValidationError
				if !errors.As(err, &validation) {
					t.Errorf("case %d: expected ValidationError, got %v", i, err)
				}
			}
			if err := ValidateDataType(DataType{}.WithFrozen(true)); err == nil {
				t.Fatal("zero descriptor became valid")
			}
		})
	})

	t.Run("ValidateScalarKind", func(t *testing.T) {
		for kind := TypeAscii; kind <= TypeDuration; kind++ {
			if err := ValidateScalarKind(kind); err != nil {
				t.Fatalf("valid scalar %v: %v", kind, err)
			}
			if allocations := testing.AllocsPerRun(100, func() {
				if err := ValidateScalarKind(kind); err != nil {
					panic(err)
				}
			}); allocations != 0 {
				t.Fatalf("scalar %v validation allocated: %g", kind, allocations)
			}
		}
		for _, kind := range []Type{-1, TypeCustom, TypeDuration + 1, TypeList, TypeMap, TypeSet, TypeTuple, TypeUDT, 65535} {
			err := ValidateScalarKind(kind)
			var validation *ValidationError
			if !errors.As(err, &validation) || validation.Field != "kind" {
				t.Fatalf("unsupported scalar %v: expected kind ValidationError, got %v", kind, err)
			}
			if _, constructorErr := ScalarType(kind); constructorErr == nil || constructorErr.Error() != err.Error() {
				t.Fatalf("constructor and validation disagree for %v: %v / %v", kind, constructorErr, err)
			}
		}
	})

	t.Run("DataTypeFromInfo", func(t *testing.T) {
		native := NewNativeType(4, TypeInt)
		input := NewUDTType(4, "record", "ks", UDTField{Name: "x", Type: NewCollectionType(NewNativeType(4, TypeList), nil, native)})
		descriptor, err := DataTypeFromInfo(input)
		if err != nil {
			t.Fatal(err)
		}
		field, _ := descriptor.Fields().At(0)
		element, ok := field.Type.Element()
		if !ok || element.Kind() != TypeInt {
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
		if c.Keyspace != "ks" || c.Table != "tbl" || c.Name != "value" || c.Type.Kind() != TypeInt {
			t.Fatalf("wrong column: %+v", c)
		}
		var nilNative *NativeType
		if _, err = DataTypeFromInfo(nilNative); err == nil {
			t.Fatal("typed nil accepted")
		}
		cyc := &CollectionType{NativeType: NewNativeType(4, TypeList)}
		cyc.Elem = cyc
		if _, err = DataTypeFromInfo(cyc); err == nil {
			t.Fatal("recursive external descriptor accepted")
		}
		for _, n := range []int{-1, 0, 65536} {
			if _, err = DataTypeFromInfo(VectorType{NativeType: NewNativeType(4, TypeCustom), SubType: native, Dimensions: n}); err == nil {
				t.Fatalf("invalid dimension %d accepted", n)
			}
		}
	})

	t.Run("Predeclared", func(t *testing.T) {
		t.Run("Shared", func(t *testing.T) {
			predeclared := map[Type]DataType{
				TypeAscii: ASCIIType, TypeBigInt: BigIntType, TypeBlob: BlobType, TypeBoolean: BooleanType,
				TypeCounter: CounterType, TypeDecimal: DecimalType, TypeDouble: DoubleType, TypeFloat: FloatType,
				TypeInt: IntType, TypeText: TextType, TypeTimestamp: TimestampType, TypeUUID: UUIDType,
				TypeVarchar: VarcharType, TypeVarint: VarintType, TypeTimeUUID: TimeUUIDType, TypeInet: InetType,
				TypeDate: DateType, TypeTime: TimeType, TypeSmallInt: SmallIntType, TypeTinyInt: TinyIntType,
				TypeDuration: DurationType,
			}
			for kind := TypeAscii; kind <= TypeDuration; kind++ {
				want, ok := predeclared[kind]
				if !ok {
					t.Fatalf("no predeclared type for %v", kind)
				}
				got, err := ScalarType(kind)
				if err != nil {
					t.Fatal(err)
				}
				if got.descriptor != want.descriptor || got.Kind() != kind {
					t.Fatalf("ScalarType(%v) = %v, want shared %v", kind, got, want)
				}
			}
			if _, err := ScalarType(TypeList); err == nil {
				t.Fatal("ScalarType accepted a collection kind")
			}
			if allocs := testing.AllocsPerRun(100, func() { _, _ = ScalarType(TypeText) }); allocs != 0 {
				t.Fatalf("ScalarType allocated %v times", allocs)
			}
		})
	})
}
