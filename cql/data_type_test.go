// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

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
		columnSlice(exposed)[0] = fields[0]
		again, _ := udt.Fields().At(0)
		if again.Name != "a" {
			t.Fatal("Fields exposed descriptor storage")
		}
		columns := NewColumns(ColumnSpec{Name: "record", Type: udt})
		c, _ := columns.At(0)
		c.Name = "changed"
		original, _ := columns.At(0)
		if original.Name != "record" {
			t.Fatal("At exposed mutable column")
		}
		frozen := udt.WithFrozen(true)
		if udt.Frozen() || udt.FrozenKnown() || !frozen.Frozen() || !frozen.FrozenKnown() || !udt.Equal(frozen) || typeFingerprint(udt) != typeFingerprint(frozen) {
			t.Fatal("frozen annotation changed wire compatibility or was lost")
		}
		changed, _ := UDTType("ks", "record", ColumnSpec{Name: "other", Type: integer}, ColumnSpec{Name: "b", Type: text})
		if udt.Equal(changed) || typeFingerprint(udt) == typeFingerprint(changed) {
			t.Fatal("UDT names missing from identity")
		}
		list, _ := ListType(udt)
		different, _ := ListType(changed)
		if list.Equal(different) || typeFingerprint(list) == typeFingerprint(different) {
			t.Fatal("nested identity lost")
		}
		mapType, _ := MapType(integer, list)
		k, v, ok := mapType.KeyValue()
		if !ok || !k.Equal(integer) || !v.Equal(list) {
			t.Fatal("map children lost")
		}
		vector, _ := VectorType(mapType, 17)
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
			cases := []func() (DataType, error){func() (DataType, error) { return ScalarType(TypeList) }, func() (DataType, error) { return ScalarType(TypeCustom) }, func() (DataType, error) { return ListType(DataType{}) }, func() (DataType, error) { return MapType(integer, DataType{}) }, func() (DataType, error) { return VectorType(integer, 0) }, func() (DataType, error) { return UDTType("", "record") }, func() (DataType, error) {
				return UDTType("ks", "record", ColumnSpec{Name: "x", Type: integer}, ColumnSpec{Name: "x", Type: integer})
			}}
			for i, test := range cases {
				_, err := test()
				if _, ok := errors.AsType[*ValidationError](err); !ok {
					t.Errorf("case %d: expected ValidationError, got %v", i, err)
				}
			}
			if err := validateDataType(DataType{}.WithFrozen(true)); err == nil {
				t.Fatal("zero descriptor became valid")
			}
		})
	})

	t.Run("ValidateScalarKind", func(t *testing.T) {
		for kind := TypeASCII; kind <= TypeDuration; kind++ {
			if err := validateScalarKind(kind); err != nil {
				t.Fatalf("valid scalar %v: %v", kind, err)
			}
			if allocations := testing.AllocsPerRun(100, func() {
				if err := validateScalarKind(kind); err != nil {
					panic(err)
				}
			}); allocations != 0 {
				t.Fatalf("scalar %v validation allocated: %g", kind, allocations)
			}
		}
		for _, kind := range []Type{TypeCustom, TypeDuration + 1, TypeList, TypeMap, TypeSet, TypeTuple, TypeUDT, 65535} {
			err := validateScalarKind(kind)
			if validation, ok := errors.AsType[*ValidationError](err); !ok || validation.Field != "kind" {
				t.Fatalf("unsupported scalar %v: expected kind ValidationError, got %v", kind, err)
			}
			if _, constructorErr := ScalarType(kind); constructorErr == nil || constructorErr.Error() != err.Error() {
				t.Fatalf("constructor and validation disagree for %v: %v / %v", kind, constructorErr, err)
			}
		}
	})

	t.Run("Predeclared", func(t *testing.T) {
		t.Run("Shared", func(t *testing.T) {
			predeclared := map[Type]DataType{
				TypeASCII: ASCIIType, TypeBigInt: BigIntType, TypeBlob: BlobType, TypeBoolean: BooleanType,
				TypeCounter: CounterType, TypeDecimal: DecimalType, TypeDouble: DoubleType, TypeFloat: FloatType,
				TypeInt: IntType, TypeText: TextType, TypeTimestamp: TimestampType, TypeUUID: UUIDType,
				TypeVarint: VarintType, TypeTimeUUID: TimeUUIDType, TypeInet: InetType,
				TypeDate: DateType, TypeTime: TimeType, TypeSmallInt: SmallIntType, TypeTinyInt: TinyIntType,
				TypeDuration: DurationType,
			}
			for kind := TypeASCII; kind <= TypeDuration; kind++ {
				if kind == wireText {
					continue
				}
				want, ok := predeclared[kind]
				if !ok {
					t.Fatalf("no predeclared type for %v", kind)
				}
				got, err := ScalarType(kind)
				if err != nil {
					t.Fatal(err)
				}
				if descriptorOf(got) != descriptorOf(want) || got.Kind() != kind {
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

// Text and varchar are one CQL type: the protocol's legacy text ID, the
// varchar ID and the schema names all produce TextType, so types that differ
// only in which spelling the server used are Equal, also when nested.
func TestTextVarcharOneKind(t *testing.T) {
	legacy, err := ScalarType(wireText)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Kind() != TypeText || descriptorOf(legacy) != descriptorOf(TextType) || !legacy.Equal(TextType) {
		t.Fatalf("legacy text ID = %v, want TextType", legacy)
	}
	if got := TypeText.String(); got != "text" {
		t.Fatalf("TypeText.String() = %q", got)
	}
	nest := func(text DataType) DataType {
		list := dataTypeTestMust(ListType(text))
		set := dataTypeTestMust(SetType(text))
		m := dataTypeTestMust(MapType(text, list))
		tuple := dataTypeTestMust(TupleType(text, set))
		udt := dataTypeTestMust(UDTType("ks", "u", ColumnSpec{Name: "f", Type: tuple}))
		return dataTypeTestMust(MapType(m, udt))
	}
	if a, b := nest(TextType), nest(legacy); !a.Equal(b) || a.String() != b.String() {
		t.Fatalf("nested text types differ: %v / %v", a, b)
	}
}

func dataTypeTestMust(t DataType, err error) DataType {
	if err != nil {
		panic(err)
	}
	return t
}
