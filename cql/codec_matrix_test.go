// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql_test

import (
	"errors"
	"math"
	"math/big"
	"net/netip"
	"reflect"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// matrixCase is one Go mapping of one CQL type: its codec, sample values,
// and how it treats the cell states and values it cannot represent.
type matrixCase interface {
	name() string
	run(t *testing.T)
}

type codecMatrix[T any] struct {
	label string
	typ   cql.DataType
	enc   cql.Encoder[T]
	dec   cql.Decoder[T]
	// values round-trip through every path.
	values []T
	// badValues are rejected by the encoder.
	badValues []T
	// wrongType is a column type the codec rejects at compile time.
	wrongType cql.DataType
	// emptyDecodes reports that the codec decodes an empty cell into the
	// zero T; otherwise decoding an empty cell fails.
	emptyDecodes bool
	// noEmptyCodec reports that EmptyCodec rejects the type: text and blob,
	// whose values can be empty anyway, and types without an empty value.
	noEmptyCodec bool
	// nullDecodes reports that the codec decodes NULL into the zero T, as
	// collections do; otherwise decoding NULL fails.
	nullDecodes bool
	equal       func(a, b T) bool
	// paths, when set, returns every decode path of a decoder. Only a few
	// mappings set it: each one instantiates every row plan for its T, and
	// the plans treat every T alike.
	paths func(cql.Decoder[T]) map[string]cql.RowDecoder[matrixRow[T]]
}

func (c codecMatrix[T]) name() string { return c.label }

func (c codecMatrix[T]) eq(a, b T) bool {
	if c.equal != nil {
		return c.equal(a, b)
	}
	return reflect.DeepEqual(a, b)
}

func (c codecMatrix[T]) columns() cql.Columns {
	return cql.NewColumns(
		cql.ColumnSpec{Name: "k", Type: matrixType(cql.TypeInt)},
		cql.ColumnSpec{Name: "v", Type: c.typ},
	)
}

// matrixEncode encodes value as column v after the int column k.
func matrixEncode[T any](columns cql.Columns, enc cql.Encoder[cql.BindValue[T]], value cql.BindValue[T], allowUnset bool) (wire.Chain, error) {
	params := cql.Params(
		cql.ParamAt(0, cql.Int32Codec, func(cql.BindValue[T]) int32 { return 7 }),
		cql.ParamAt(1, enc, func(v cql.BindValue[T]) cql.BindValue[T] { return v }),
	)
	plan, err := params.Compile(columns)
	if err != nil {
		return wire.Chain{}, err
	}
	return cql.EncodeParamsForTest(columns, plan, value, allowUnset)
}

type matrixRow[T any] struct {
	K int32
	V T
}

func matrixDecoders[T any](dec cql.Decoder[T]) map[string]cql.RowDecoder[matrixRow[T]] {
	k := func(r *matrixRow[T]) *int32 { return &r.K }
	v := func(r *matrixRow[T]) *T { return &r.V }
	return map[string]cql.RowDecoder[matrixRow[T]]{
		"Column":   cql.Row(cql.Column("k", cql.Int32Codec, k), cql.Column("v", dec, v)),
		"ColumnAt": cql.Row(cql.ColumnAt(0, cql.Int32Codec, k), cql.ColumnAt(1, dec, v)),
		"RowInto":  cql.RowInto(func(r *matrixRow[T]) { *r = matrixRow[T]{} }, cql.Column("v", dec, v), cql.Column("k", cql.Int32Codec, k)),
		"Pair": cql.ConvertRows(cql.PairRows(cql.OneColumn(cql.Int32Codec), cql.OneColumn(dec)),
			func(p cql.Pair[int32, T]) matrixRow[T] { return matrixRow[T]{K: p.First, V: p.Second} },
			func(r matrixRow[T]) matrixRow[T] { return matrixRow[T]{K: r.K, V: dec.Clone(r.V)} }),
	}
}

// decoders returns the decode paths c is checked through: Column, or every
// path when c sets paths.
func (c codecMatrix[T]) decoders() map[string]cql.RowDecoder[matrixRow[T]] {
	if c.paths != nil {
		return c.paths(c.dec)
	}
	return map[string]cql.RowDecoder[matrixRow[T]]{"Column": matrixColumn(c.dec)}
}

// matrixColumn is the Column path of matrixDecoders alone.
func matrixColumn[T any](dec cql.Decoder[T]) cql.RowDecoder[matrixRow[T]] {
	return cql.Row(cql.Column("k", cql.Int32Codec, func(r *matrixRow[T]) *int32 { return &r.K }),
		cql.Column("v", dec, func(r *matrixRow[T]) *T { return &r.V }))
}

func matrixDecode[T any](columns cql.Columns, chain wire.Chain, decoder cql.RowDecoder[matrixRow[T]]) (matrixRow[T], error) {
	plan, err := decoder.Compile(columns)
	if err != nil {
		return matrixRow[T]{}, err
	}
	return cql.DecodeRowForTest(columns, plan, chain)
}

func (c codecMatrix[T]) run(t *testing.T) {
	columns := c.columns()
	bind := cql.BindEncoder(c.enc)
	t.Run("RoundTrip", func(t *testing.T) {
		for i, value := range c.values {
			w, err := matrixEncode(columns, bind, cql.BindValue[T]{Value: value}, false)
			if err != nil {
				t.Fatalf("value %d: encode: %v", i, err)
			}
			for path, decoder := range c.decoders() {
				row, err := matrixDecode(columns, w, decoder)
				if err != nil {
					t.Fatalf("value %d: %s: %v", i, path, err)
				}
				if row.K != 7 || !c.eq(row.V, value) {
					t.Fatalf("value %d: %s: got %+v, want %+v", i, path, row.V, value)
				}
			}
			row, err := matrixDecode(columns, w, matrixColumn(cql.NullableDecoder(c.dec)))
			if err != nil || !row.V.Valid || !c.eq(row.V.Value, value) {
				t.Fatalf("value %d: Nullable: got %+v, %v", i, row.V, err)
			}
		}
	})
	t.Run("Null", func(t *testing.T) {
		w, err := matrixEncode(columns, bind, cql.BindValue[T]{State: cql.Null}, false)
		if err != nil {
			t.Fatal(err)
		}
		for path, decoder := range c.decoders() {
			row, err := matrixDecode(columns, w, decoder)
			if !c.nullDecodes {
				wantColumnError(t, err, "decode", 1, "v")
				continue
			}
			var zero T
			if err != nil || !c.eq(row.V, zero) {
				t.Fatalf("%s: got %+v, %v; want the zero value", path, row.V, err)
			}
		}
		row, err := matrixDecode(columns, w, matrixColumn(cql.NullableDecoder(c.dec)))
		if err != nil || row.V.Valid {
			t.Fatalf("Nullable: got %+v, %v", row.V, err)
		}
		nullable, err := matrixEncode(columns, cql.BindEncoder(cql.NullableEncoder(c.enc)), cql.BindValue[cql.Nullable[T]]{}, false)
		if err != nil {
			t.Fatal(err)
		}
		if row, err := matrixDecode(columns, nullable, matrixColumn(cql.NullableDecoder(c.dec))); err != nil || row.V.Valid {
			t.Fatalf("a zero Nullable did not encode NULL: got %+v, %v", row.V, err)
		}
	})
	t.Run("Empty", func(t *testing.T) {
		// k = 7 and an empty v.
		w := wire.ChainOf([]byte{0, 0, 0, 4, 0, 0, 0, 7, 0, 0, 0, 0})
		empty := cql.EmptyCodec(cql.Codec[T](matrixCodec[T]{c.enc, c.dec}))
		emptyBind := cql.BindEncoder(cql.Encoder[cql.MaybeEmpty[T]](empty))
		encoded, err := matrixEncode(columns, emptyBind, cql.BindValue[cql.MaybeEmpty[T]]{Value: cql.MaybeEmpty[T]{Empty: true}}, false)
		if c.noEmptyCodec {
			wantColumnError(t, err, "compile", 1, "v")
		} else {
			if err != nil {
				t.Fatal(err)
			}
			if got := matrixBytes(t, encoded); string(got) != string(matrixBytes(t, w)) {
				t.Fatalf("EmptyCodec encoded %x, want %x", got, matrixBytes(t, w))
			}
			row, err := matrixDecode(columns, w, matrixColumn(cql.Decoder[cql.MaybeEmpty[T]](empty)))
			if err != nil || !row.V.Empty {
				t.Fatalf("EmptyCodec: got %+v, %v", row.V, err)
			}
		}
		for path, decoder := range c.decoders() {
			row, err := matrixDecode(columns, w, decoder)
			if !c.emptyDecodes {
				wantColumnError(t, err, "decode", 1, "v")
				continue
			}
			var zero T
			if err != nil || !c.eq(row.V, zero) {
				t.Fatalf("%s: got %+v, %v; want the zero value", path, row.V, err)
			}
		}
		if _, err := matrixEncode(columns, bind, cql.BindValue[T]{State: cql.EmptyCell}, false); err == nil {
			t.Fatal("BindValue bound an empty cell")
		} else {
			wantColumnError(t, err, "encode", 1, "v")
		}
	})
	t.Run("Unset", func(t *testing.T) {
		if _, err := matrixEncode(columns, bind, cql.BindValue[T]{State: cql.Unset}, true); err != nil {
			t.Fatal(err)
		}
		_, err := matrixEncode(columns, bind, cql.BindValue[T]{State: cql.Unset}, false)
		wantColumnError(t, err, "encode", 1, "v")
	})
	t.Run("OutOfRange", func(t *testing.T) {
		for i, value := range c.badValues {
			_, err := matrixEncode(columns, bind, cql.BindValue[T]{Value: value}, false)
			if err == nil {
				t.Fatalf("bad value %d (%+v) encoded", i, value)
			}
			wantColumnError(t, err, "encode", 1, "v")
		}
	})
	t.Run("WrongType", func(t *testing.T) {
		wrong := cql.NewColumns(
			cql.ColumnSpec{Name: "k", Type: matrixType(cql.TypeInt)},
			cql.ColumnSpec{Name: "v", Type: c.wrongType},
		)
		for path, decoder := range c.decoders() {
			_, err := decoder.Compile(wrong)
			if err == nil {
				t.Fatalf("%s accepted %s", path, c.wrongType.Kind())
			}
			wantColumnError(t, err, "compile", 1, "v")
		}
		_, err := cql.Params(cql.ParamAt(0, cql.Int32Codec, func(T) int32 { return 0 }), cql.ParamAt(1, c.enc, func(v T) T { return v })).Compile(wrong)
		wantColumnError(t, err, "compile", 1, "v")
	})
}

// matrixCodec joins an encoder and a decoder of the same values.
type matrixCodec[T any] struct {
	cql.Encoder[T]
	cql.Decoder[T]
}

func (c matrixCodec[T]) CheckType(t cql.DataType) error { return c.Encoder.CheckType(t) }

func matrixOf[T any](label string, kind cql.Type, codec cql.Codec[T], wrong cql.Type, values ...T) codecMatrix[T] {
	return matrixOfType(label, matrixType(kind), codec, matrixType(wrong), values...)
}

func matrixOfType[T any](label string, typ cql.DataType, codec cql.Codec[T], wrong cql.DataType, values ...T) codecMatrix[T] {
	return codecMatrix[T]{label: label, typ: typ, enc: codec, dec: codec, wrongType: wrong, values: values}
}

// TestCodecMatrix round-trips every built-in Go mapping of every CQL type
// through the row and parameter plans, with NULL, empty, unset, out-of-range
// values and a mismatched column type, and checks that each failure is a
// CodecError naming the column.
func TestCodecMatrix(t *testing.T) {
	intType := matrixType(cql.TypeInt)
	textType := matrixType(cql.TypeText)
	bigVarint := cql.VarintFromBigInt(new(big.Int).Lsh(big.NewInt(-3), 100))
	textEq := func(a, b cql.Text) bool { return a.String() == b.String() }
	bytesEq := func(a, b cql.Bytes) bool { return string(a.OwnedBytes()) == string(b.OwnedBytes()) }
	varintEq := func(a, b cql.Varint) bool { return a.BigInt().Cmp(b.BigInt()) == 0 }
	decimalEq := func(a, b cql.Decimal) bool { return a.Scale == b.Scale && varintEq(a.Unscaled, b.Unscaled) }
	floatEq32 := func(a, b float32) bool { return math.Float32bits(a) == math.Float32bits(b) }
	floatEq64 := func(a, b float64) bool { return math.Float64bits(a) == math.Float64bits(b) }

	withEq := func(c codecMatrix[cql.Text], eq func(a, b cql.Text) bool) codecMatrix[cql.Text] {
		c.equal = eq
		return c
	}
	text := withEq(matrixOf("text/Text", cql.TypeText, cql.TextCodec, cql.TypeInt, cql.Text{}, mustText("héllo")), textEq)
	text.emptyDecodes, text.noEmptyCodec = true, true
	ascii := withEq(matrixOf("ascii/Text", cql.TypeASCII, cql.ASCIICodec, cql.TypeInt, mustText("plain")), textEq)
	ascii.emptyDecodes, ascii.noEmptyCodec = true, true
	str := matrixOf("text/string", cql.TypeText, cql.StringCodec, cql.TypeBlob, "", "héllo")
	str.emptyDecodes, str.noEmptyCodec, str.badValues = true, true, []string{"\xff"}
	str.paths = matrixDecoders[string]
	asciiStr := matrixOf("ascii/string", cql.TypeASCII, cql.ASCIIStringCodec, cql.TypeText, "plain")
	asciiStr.emptyDecodes, asciiStr.noEmptyCodec, asciiStr.badValues = true, true, []string{"é"}
	varcharStr := matrixOf("varchar/string", cql.TypeVarchar, cql.StringCodec, cql.TypeASCII, "v")
	varcharStr.emptyDecodes, varcharStr.noEmptyCodec = true, true
	blob := matrixOf("blob/Bytes", cql.TypeBlob, cql.BlobCodec, cql.TypeText, cql.BorrowBytes([]byte{1, 2, 3}))
	blob.emptyDecodes, blob.noEmptyCodec, blob.equal = true, true, bytesEq
	byteSlice := matrixOf("blob/[]byte", cql.TypeBlob, cql.ByteSliceCodec, cql.TypeText, []byte{1, 2, 3})
	byteSlice.emptyDecodes, byteSlice.noEmptyCodec = true, true
	varint := matrixOf("varint/Varint", cql.TypeVarint, cql.VarintCodec, cql.TypeBigInt, cql.VarintFromBigInt(big.NewInt(0)), cql.VarintFromBigInt(big.NewInt(-1)), bigVarint)
	varint.equal = varintEq
	decimal := matrixOf("decimal/Decimal", cql.TypeDecimal, cql.DecimalCodec, cql.TypeDouble,
		cql.Decimal{Unscaled: cql.VarintFromBigInt(big.NewInt(12345)), Scale: 2}, cql.Decimal{Unscaled: bigVarint, Scale: math.MinInt32})
	decimal.equal = decimalEq
	f32 := matrixOf("float/float32", cql.TypeFloat, cql.Float32Codec, cql.TypeDouble, 0, float32(math.Inf(-1)), float32(math.NaN()), math.MaxFloat32, math.SmallestNonzeroFloat32)
	f32.equal = floatEq32
	f64 := matrixOf("double/float64", cql.TypeDouble, cql.Float64Codec, cql.TypeFloat, 0, math.Inf(1), math.NaN(), math.MaxFloat64, -math.SmallestNonzeroFloat64)
	f64.equal = floatEq64
	timeOfDay := matrixOf("time/TimeOfDay", cql.TypeTime, cql.TimeCodec, cql.TypeBigInt, 0, cql.TimeOfDay(24*3600*1e9-1))
	timeOfDay.badValues = []cql.TimeOfDay{-1, cql.TimeOfDay(24 * 3600 * 1e9)}
	inet := matrixOf("inet/netip.Addr", cql.TypeInet, cql.InetCodec, cql.TypeBlob, netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr("::1"))
	inet.badValues = []netip.Addr{{}}
	duration := matrixOf("duration/Duration", cql.TypeDuration, cql.DurationCodec, cql.TypeBigInt,
		cql.Duration{}, cql.Duration{Months: math.MaxInt32, Days: math.MaxInt32, Nanoseconds: math.MaxInt64},
		cql.Duration{Months: math.MinInt32, Days: math.MinInt32, Nanoseconds: math.MinInt64})
	duration.noEmptyCodec = true
	duration.badValues = []cql.Duration{{Months: 1, Days: -1}, {Days: -1, Nanoseconds: 1}}
	timeUUID := matrixOf("timeuuid/TimeUUID", cql.TypeTimeUUID, cql.TimeUUIDCodec, cql.TypeUUID, cql.MustParseTimeUUID("8a1b8e54-ed6e-11ee-8001-0242ac120002"))
	timeUUID.badValues = []cql.TimeUUID{{}}

	// Every decode path for a scalar, a string, a collection and a tuple;
	// the other mappings are checked through Column.
	i32 := matrixOf("int/int32", cql.TypeInt, cql.Int32Codec, cql.TypeBigInt, math.MinInt32, 0, math.MaxInt32)
	i32.paths = matrixDecoders[int32]
	counter := matrixOf("counter/Counter", cql.TypeCounter, cql.CounterCodec, cql.TypeInt, math.MinInt64, 0, math.MaxInt64)
	counter.noEmptyCodec = true
	list := matrixOfType("list<int>/[]int32", mustMatrixType(cql.ListType(intType)), cql.ListCodec(cql.Int32Codec), mustMatrixType(cql.SetType(intType)), []int32{}, []int32{1, math.MinInt32})
	list.noEmptyCodec, list.nullDecodes, list.equal = true, true, nilAsEmptyEqual[int32]
	list.paths = matrixDecoders[[]int32]
	set := matrixOfType("set<text>/[]string", mustMatrixType(cql.SetType(textType)), cql.SetCodec(cql.StringCodec), mustMatrixType(cql.ListType(textType)), []string{"a", "b"})
	set.noEmptyCodec, set.nullDecodes, set.equal = true, true, nilAsEmptyEqual[string]
	vector := matrixOfType("vector<float,3>/[]float32", mustMatrixType(cql.VectorType(matrixType(cql.TypeFloat), 3)), cql.VectorCodec(cql.Float32Codec), mustMatrixType(cql.ListType(matrixType(cql.TypeFloat))), []float32{1, -2, 3})
	vector.noEmptyCodec = true
	vector.badValues = [][]float32{{1, 2}, {1, 2, 3, 4}}
	entries := matrixOfType("map<int,text>/[]Entry", mustMatrixType(cql.MapType(intType, textType)), cql.MapCodec(cql.Int32Codec, cql.StringCodec), mustMatrixType(cql.ListType(intType)), []cql.Entry[int32, string]{{Key: 1, Value: "a"}})
	entries.noEmptyCodec, entries.nullDecodes, entries.equal = true, true, nilAsEmptyEqual[cql.Entry[int32, string]]
	goMap := matrixOfType("map<int,text>/map", mustMatrixType(cql.MapType(intType, textType)), cql.GoMapCodec(cql.Int32Codec, cql.StringCodec), mustMatrixType(cql.ListType(intType)), map[int32]string{1: "a", 2: "b"})
	goMap.noEmptyCodec, goMap.nullDecodes = true, true
	goMap.equal = func(a, b map[int32]string) bool { return len(a) == 0 && len(b) == 0 || reflect.DeepEqual(a, b) }
	tupleType := mustMatrixType(cql.TupleType(intType, textType))
	tupleRow := cql.Row(cql.ColumnAt(0, cql.Int32Codec, func(p *cql.Pair[int32, string]) *int32 { return &p.First }),
		cql.ColumnAt(1, cql.StringCodec, func(p *cql.Pair[int32, string]) *string { return &p.Second }))
	tupleParams := cql.Params(cql.ParamAt(0, cql.Int32Codec, func(p cql.Pair[int32, string]) int32 { return p.First }),
		cql.ParamAt(1, cql.StringCodec, func(p cql.Pair[int32, string]) string { return p.Second }))
	tuple := codecMatrix[cql.Pair[int32, string]]{
		label: "tuple<int,text>/Pair", typ: tupleType, wrongType: mustMatrixType(cql.TupleType(intType)),
		enc: cql.TupleEncoder(tupleParams), dec: cql.TupleDecoder(tupleRow),
		values: []cql.Pair[int32, string]{{First: 1, Second: "a"}},
		paths:  matrixDecoders[cql.Pair[int32, string]],
	}

	udtType := mustMatrixType(cql.UDTType("ks", "pair", cql.ColumnSpec{Name: "first", Type: intType}, cql.ColumnSpec{Name: "second", Type: textType}))
	udtOpts, err := cql.UDTOptionsFor(udtType)
	if err != nil {
		t.Fatal(err)
	}
	wrongUDT := mustMatrixType(cql.UDTType("ks", "other", cql.ColumnSpec{Name: "first", Type: intType}, cql.ColumnSpec{Name: "second", Type: textType}))
	udt := codecMatrix[cql.Pair[int32, string]]{
		label: "udt/Pair", typ: udtType, wrongType: wrongUDT,
		enc: cql.UDTEncoder(cql.Params(cql.Param("first", cql.Int32Codec, func(p cql.Pair[int32, string]) int32 { return p.First }),
			cql.Param("second", cql.StringCodec, func(p cql.Pair[int32, string]) string { return p.Second })), udtOpts),
		dec: cql.UDTDecoder(cql.Row(cql.Column("first", cql.Int32Codec, func(p *cql.Pair[int32, string]) *int32 { return &p.First }),
			cql.Column("second", cql.StringCodec, func(p *cql.Pair[int32, string]) *string { return &p.Second })), udtOpts),
		values:       []cql.Pair[int32, string]{{First: 1, Second: "a"}, {}},
		noEmptyCodec: true,
	}

	cases := []matrixCase{
		matrixOf("boolean/bool", cql.TypeBoolean, cql.BoolCodec, cql.TypeTinyInt, false, true),
		matrixOf("tinyint/int8", cql.TypeTinyInt, cql.Int8Codec, cql.TypeSmallInt, math.MinInt8, 0, math.MaxInt8),
		matrixOf("smallint/int16", cql.TypeSmallInt, cql.Int16Codec, cql.TypeInt, math.MinInt16, 0, math.MaxInt16),
		i32,
		matrixOf("bigint/int64", cql.TypeBigInt, cql.Int64Codec, cql.TypeInt, math.MinInt64, 0, math.MaxInt64),
		counter,
		f32, f64, str, asciiStr, varcharStr, text, ascii, blob, byteSlice, varint, decimal,
		matrixOf("date/Date", cql.TypeDate, cql.DateCodec, cql.TypeInt, math.MinInt32, 0, math.MaxInt32),
		timeOfDay,
		matrixOf("timestamp/Timestamp", cql.TypeTimestamp, cql.TimestampCodec, cql.TypeInt, math.MinInt64, 0, math.MaxInt64),
		duration,
		matrixOf("uuid/UUID", cql.TypeUUID, cql.UUIDCodec, cql.TypeBlob, cql.UUID{}, cql.MustParseUUID("00112233-4455-6677-8899-aabbccddeeff")),
		timeUUID, inet, list, set, vector, entries, goMap, tuple, udt,
	}
	for _, c := range cases {
		t.Run(c.name(), c.run)
	}
}

// nilAsEmptyEqual compares slices, treating nil and empty alike.
func nilAsEmptyEqual[T any](a, b []T) bool {
	return len(a) == 0 && len(b) == 0 || reflect.DeepEqual(a, b)
}

func mustText(s string) cql.Text {
	v, err := cql.BorrowText(s)
	if err != nil {
		panic(err)
	}
	return v
}

func matrixBytes(t *testing.T, chain wire.Chain) []byte {
	t.Helper()
	buf := make([]byte, chain.Len())
	if _, err := chain.CopyTo(buf); err != nil {
		t.Fatal(err)
	}
	return buf
}

func matrixType(kind cql.Type) cql.DataType { return mustMatrixType(cql.ScalarType(kind)) }

func mustMatrixType(t cql.DataType, err error) cql.DataType {
	if err != nil {
		panic(err)
	}
	return t
}

// wantColumnError fails t unless err is a CodecError for column index at
// stage.
func wantColumnError(t *testing.T, err error, stage cql.CodecStage, index int, column string) {
	t.Helper()
	if err == nil {
		t.Fatalf("no error, want a %s error for column %s", stage, column)
	}
	codecErr, ok := errors.AsType[*cql.CodecError](err)
	if !ok {
		t.Fatalf("error %q (%T) is not a CodecError", err, err)
	}
	if codecErr.Stage != stage || codecErr.Index != index || codecErr.Column != column {
		t.Fatalf("error %q: got stage %q column %d (%s), want stage %q column %d (%s)",
			err, codecErr.Stage, codecErr.Index, codecErr.Column, stage, index, column)
	}
}
