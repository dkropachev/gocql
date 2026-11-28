// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

type deriveStatus string

type deriveBlob []byte

// deriveAddress has a CQLCodec method built by UDTCodecOf, so fields of it
// and of types derived from it map without an option, through the codec's
// own fieldCodec.
type deriveAddress struct {
	Street string
	Zip    int32
}

var deriveAddressCodec = MustUDTCodecOf[deriveAddress]()

func (deriveAddress) CQLCodec() Codec[deriveAddress] { return deriveAddressCodec }

// deriveMoney has a CQLCodec method returning an application codec type,
// which struct mappings call through reflection.
type deriveMoney struct{ Cents int64 }

type moneyCodec struct{}

func (moneyCodec) CheckType(t DataType) error { return Int64Codec.CheckType(t) }
func (moneyCodec) Encode(w *ValueWriter, v deriveMoney) error {
	return Int64Codec.Encode(w, v.Cents)
}
func (moneyCodec) Decode(r ValueReader) (deriveMoney, error) {
	v, err := Int64Codec.Decode(r)
	return deriveMoney{Cents: v}, err
}
func (moneyCodec) Clone(v deriveMoney) deriveMoney { return v }

func (*deriveMoney) CQLCodec() Codec[deriveMoney] { return moneyCodec{} }

func TestDerivedFieldTypes(t *testing.T) {
	integer, text, bigint := rowTestType(t, KindInt), rowTestType(t, KindText), rowTestType(t, KindBigInt)
	uuid, float := rowTestType(t, KindUUID), rowTestType(t, KindFloat)
	duration := rowTestType(t, KindDuration)
	list := func(e DataType) DataType { return CodecTestMust(ListType(e)) }
	udt := CodecTestMust(UDTType("ks", "address", ColumnSpec{Name: "street", Type: text}, ColumnSpec{Name: "zip", Type: integer}))

	type row struct {
		Nested   [][]int32
		ByName   map[string][]string
		Maybe    *Nullable[[]deriveStatus]
		Pair     Pair[int32, string]
		Vector   [3]float32
		ID       [16]byte
		Span     time.Duration
		Home     deriveAddress
		Previous []deriveAddress
		ByCity   map[string]*deriveAddress
		Balance  deriveMoney
		History  []deriveMoney
	}
	columns := NewColumns(
		ColumnSpec{Name: "nested", Type: list(list(integer))},
		ColumnSpec{Name: "by_name", Type: CodecTestMust(MapType(text, list(text)))},
		ColumnSpec{Name: "maybe", Type: CodecTestMust(SetType(text))},
		ColumnSpec{Name: "pair", Type: CodecTestMust(TupleType(integer, text))},
		ColumnSpec{Name: "vector", Type: CodecTestMust(VectorType(float, 3))},
		ColumnSpec{Name: "id", Type: uuid},
		ColumnSpec{Name: "span", Type: duration},
		ColumnSpec{Name: "home", Type: udt},
		ColumnSpec{Name: "previous", Type: list(udt)},
		ColumnSpec{Name: "by_city", Type: CodecTestMust(MapType(text, udt))},
		ColumnSpec{Name: "balance", Type: bigint},
		ColumnSpec{Name: "history", Type: list(bigint)},
	)
	want := row{
		Nested:   [][]int32{{1, 2}, {3}},
		ByName:   map[string][]string{"a": {"x", "y"}},
		Maybe:    &Nullable[[]deriveStatus]{Value: []deriveStatus{"on"}, Valid: true},
		Pair:     Pair[int32, string]{First: 7, Second: "seven"},
		Vector:   [3]float32{1, 2, 3},
		ID:       [16]byte{1, 2, 3},
		Span:     90 * time.Second,
		Home:     deriveAddress{"Main", 10},
		Previous: []deriveAddress{{"Old", 20}},
		ByCity:   map[string]*deriveAddress{"oslo": {"Fjord", 30}},
		Balance:  deriveMoney{Cents: 1234},
		History:  []deriveMoney{{1}, {2}},
	}
	chain := encodeTestParams(t, columns, MustParamsOf[row](), want)
	got := decodeTestRow(t, columns, MustRowOf[row](), chain)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip:\ngot  %+v\nwant %+v", got, want)
	}
	if clone := MustRowOf[row]().Clone(got); !reflect.DeepEqual(clone, want) {
		t.Fatalf("clone: %+v", clone)
	}

	t.Run("ArrayLength", func(t *testing.T) {
		type short struct{ Vector [3]int32 }
		cols := NewColumns(ColumnSpec{Name: "vector", Type: list(integer)})
		type source struct{ Vector []int32 }
		chain := encodeTestParams(t, cols, MustParamsOf[source](), source{Vector: []int32{1, 2}})
		plan := CodecTestMust(MustRowOf[short]().Compile(cols))
		reader := newRowReader(cols, chain, nil)
		if _, err := decodeRow(plan, reader); err == nil || !strings.Contains(err.Error(), "needs 3") {
			t.Fatalf("a list of 2 decoded into [3]int32: %v", err)
		}
	})

	t.Run("TimeDurationIsNotBigint", func(t *testing.T) {
		type span struct{ Span time.Duration }
		cols := NewColumns(ColumnSpec{Name: "span", Type: bigint})
		if _, err := MustRowOf[span]().Compile(cols); err == nil {
			t.Fatal("a time.Duration field compiled for bigint")
		}
		d := CodecTestMust(EncodeValue(duration, DurationCodec, Duration{Days: 1}))
		if _, err := DecodeValue(TimeDurationCodec, d); err == nil {
			t.Fatal("a duration of a day decoded into time.Duration")
		}
	})

	t.Run("NestedBindValue", func(t *testing.T) {
		type nested struct{ Values []BindValue[int32] }
		if _, err := ParamsOf[nested](); err == nil || !strings.Contains(err.Error(), "BindValue maps only") {
			t.Fatalf("a slice of BindValue mapped: %v", err)
		}
	})

	t.Run("BadCQLCodecMethod", func(t *testing.T) {
		type bad struct{ V deriveBadCodec }
		if _, err := ParamsOf[bad](); err == nil || !strings.Contains(err.Error(), "CQLCodec must be") {
			t.Fatalf("a CQLCodec of the wrong type: %v", err)
		}
	})
}

type deriveBadCodec struct{}

func (deriveBadCodec) CQLCodec() Codec[int32] { return Int32Codec }

func TestDeriveErrors(t *testing.T) {
	type unsupported struct{ C chan int }
	if _, err := ParamsOf[unsupported](); err == nil || !strings.Contains(err.Error(), "no codec for type chan int") {
		t.Fatalf("chan field: %v", err)
	}
	type wrongTag struct {
		V map[string]int32 `cql:"v,codec=TimestampCodec"`
	}
	if _, err := ParamsOf[wrongTag](); err == nil || !strings.Contains(err.Error(), "TimestampCodec") {
		t.Fatalf("a tag naming no leaf of the type: %v", err)
	}
	// A cmd/cqlgen codec expression, which reflection cannot evaluate,
	// gives way to the type's CQLCodec method.
	type expression struct {
		Home []deriveAddress `cql:"home,codec=cql.SliceCodec(addressCodec)"`
	}
	if _, err := ParamsOf[expression](); err != nil {
		t.Fatal(err)
	}
	type unresolved struct {
		Point structPoint `cql:"point,codec=pointCodec{}"`
	}
	if _, err := ParamsOf[unresolved](); err == nil || !strings.Contains(err.Error(), "CQLCodec method") {
		t.Fatalf("an unresolvable expression: %v", err)
	}
	// A named []byte maps like []byte, also as an element.
	type blobs struct {
		One  deriveBlob
		Many []deriveBlob
	}
	if _, err := RowOf[blobs](); err != nil {
		t.Fatal(err)
	}
	// A leaf tag applies to the leaves it maps, here the map's values.
	type asciiValues struct {
		V map[int32]string `cql:"v,codec=ASCIIStringCodec"`
	}
	if _, err := ParamsOf[asciiValues](); err != nil {
		t.Fatal(err)
	}
}
