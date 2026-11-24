// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"math"
	"runtime"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// fuzzType builds a data type from selector bytes, nesting at most depth
// levels: scalars, collections, tuples, UDTs and vectors.
func fuzzType(t *testing.T, sel *[]byte, depth int) DataType {
	next := func() byte {
		if len(*sel) == 0 {
			return 0
		}
		b := (*sel)[0]
		*sel = (*sel)[1:]
		return b
	}
	scalars := []Type{TypeInt, TypeText, TypeBlob, TypeFloat, TypeVarint, TypeDecimal, TypeDuration,
		TypeBoolean, TypeUUID, TypeInet, TypeDate, TypeTime, TypeSmallInt, TypeTinyInt, TypeBigInt, TypeASCII}
	b := next()
	kind := int(b) % (len(scalars) + 6)
	if depth == 0 || kind < len(scalars) {
		return CodecTestMust(ScalarType(scalars[int(b)%len(scalars)]))
	}
	switch kind - len(scalars) {
	case 0:
		return CodecTestMust(ListType(fuzzType(t, sel, depth-1)))
	case 1:
		return CodecTestMust(SetType(fuzzType(t, sel, depth-1)))
	case 2:
		return CodecTestMust(MapType(fuzzType(t, sel, depth-1), fuzzType(t, sel, depth-1)))
	case 3:
		fields := make([]DataType, 1+int(next())%3)
		for i := range fields {
			fields[i] = fuzzType(t, sel, depth-1)
		}
		return CodecTestMust(TupleType(fields...))
	case 4:
		fields := make([]ColumnSpec, 1+int(next())%3)
		for i := range fields {
			fields[i] = ColumnSpec{Name: string(rune('a' + i)), Type: fuzzType(t, sel, depth-1)}
		}
		return CodecTestMust(UDTType("ks", "u", fields...))
	default:
		return CodecTestMust(VectorType(fuzzType(t, sel, depth-1), uint16(1+next()%4)))
	}
}

// fuzzDecoders are typed decoders of a fixed column type each.
func fuzzDecoders(t *testing.T) []func(ColumnSpec, wire.Chain, *wire.Lease) {
	intType, textType := IntType, TextType
	floatType := CodecTestMust(ScalarType(TypeFloat))
	pair := Row(ColumnAt(0, Int32Codec, func(p *Pair[int32, string]) *int32 { return &p.First }),
		ColumnAt(1, StringCodec, func(p *Pair[int32, string]) *string { return &p.Second }))
	udtType := CodecTestMust(UDTType("ks", "u", ColumnSpec{Name: "a", Type: intType}, ColumnSpec{Name: "b", Type: textType}))
	udtOptions := CodecTestMust(UDTOptionsFor(udtType))
	udtOptions.AllowMissingFields = true
	udt := Row(Column("a", Int32Codec, func(p *Pair[int32, string]) *int32 { return &p.First }),
		Column("b", StringCodec, func(p *Pair[int32, string]) *string { return &p.Second }))
	return []func(ColumnSpec, wire.Chain, *wire.Lease){
		fuzzDecoder(CodecTestMust(ListType(intType)), ListCodec(Int32Codec)),
		fuzzDecoder(CodecTestMust(SetType(textType)), SetCodec(StringCodec)),
		fuzzDecoder(CodecTestMust(ListType(CodecTestMust(ListType(BlobType)))), ListCodec(ListCodec(BlobCodec))),
		fuzzDecoder(CodecTestMust(MapType(intType, textType)), MapCodec(Int32Codec, TextCodec)),
		fuzzDecoder(CodecTestMust(MapType(intType, textType)), GoMapCodec(Int32Codec, StringCodec)),
		fuzzDecoder(CodecTestMust(VectorType(floatType, 3)), VectorCodec(Float32Codec)),
		fuzzDecoder(CodecTestMust(VectorType(textType, 2)), VectorCodec(StringCodec)),
		fuzzDecoder(CodecTestMust(TupleType(intType, textType)), TupleDecoder(pair)),
		fuzzDecoder(udtType, UDTDecoder(udt, udtOptions)),
		fuzzDecoder(CodecTestMust(ScalarType(TypeVarint)), VarintCodec),
		fuzzDecoder(CodecTestMust(ScalarType(TypeDecimal)), DecimalCodec),
		fuzzDecoder(CodecTestMust(ScalarType(TypeDuration)), DurationCodec),
		fuzzDecoder(CodecTestMust(ScalarType(TypeInet)), InetCodec),
	}
}

func fuzzDecoder[T any](typ DataType, decoder Decoder[T]) func(ColumnSpec, wire.Chain, *wire.Lease) {
	return func(column ColumnSpec, chain wire.Chain, lease *wire.Lease) {
		column.Type = typ
		columns := NewColumns(column)
		rows := OneColumn(decoder)
		plan, err := rows.Compile(columns)
		if err != nil {
			panic(err) // the fixed types always compile
		}
		value, err := decodeRow(plan, newRowReader(columns, chain, lease))
		if err == nil {
			_ = rows.Clone(value)
		}
	}
}

// walkValue reads every part of a dynamic value.
func walkValue(v Value, depth int) {
	_, _ = v.Bool()
	_, _ = v.Signed()
	_, _ = v.Float()
	_, _ = v.Text()
	_, _ = v.Bytes()
	_, _ = v.Varint()
	_, _ = v.Decimal()
	_, _ = v.Duration()
	_, _ = v.Inet()
	if depth > 8 {
		return
	}
	if it, err := v.Elements(); err == nil {
		for it.Next() {
			walkValue(it.Value(), depth+1)
		}
	}
	if it, err := v.Entries(); err == nil {
		for it.Next() {
			walkValue(it.Value().Key, depth+1)
			walkValue(it.Value().Value, depth+1)
		}
	}
	if it, err := v.Fields(); err == nil {
		for it.Next() {
			walkValue(it.Value().Value, depth+1)
		}
	}
	_ = v.Clone()
}

// FuzzValueDecoders feeds arbitrary cells, as a server sends them, to the
// typed collection, tuple, UDT, vector and numeric decoders and to
// DynamicCodec over a generated type. None may panic, with the cell split
// across spans and with a live or released lease.
func FuzzValueDecoders(f *testing.F) {
	f.Add(uint8(0), []byte{}, []byte{0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 7}, uint8(0), false)
	f.Add(uint8(3), []byte{}, []byte{0, 0, 0, 4, 0x7f, 0xff, 0xff, 0xff}, uint8(1), false)             // a huge map count
	f.Add(uint8(0), []byte{}, []byte{0, 0, 0, 8, 0, 0, 0, 1, 0xff, 0xff, 0xff, 0xfe}, uint8(2), false) // a negative element length
	f.Add(uint8(7), []byte{}, []byte{0, 0, 0, 9, 0, 0, 0, 1, 7, 0xff, 0xff, 0xff, 0xff}, uint8(0), false)
	f.Add(uint8(200), []byte{16, 21, 3, 1}, []byte{0, 0, 0, 4, 0, 0, 0, 0}, uint8(0), true)
	f.Add(uint8(200), []byte{21, 1, 16, 21, 0}, []byte{0, 0, 0, 2, 0, 0}, uint8(3), false)
	f.Add(uint8(200), []byte{19, 2, 0, 1, 2}, []byte{0xff, 0xff, 0xff, 0xff}, uint8(0), false)
	f.Fuzz(func(t *testing.T, which uint8, sel, cell []byte, chunk uint8, released bool) {
		if len(cell) > 1<<12 || len(sel) > 64 {
			return
		}
		var spans []wire.Span
		for rest, i := cell, 0; len(rest) > 0; i++ {
			n := len(rest)
			if chunk > 0 {
				n = min(int(chunk)+i%3, len(rest))
			}
			spans = append(spans, wire.Borrow(rest[:n]))
			rest = rest[n:]
		}
		chain, err := wire.NewChain(spans...)
		if err != nil {
			t.Fatal(err)
		}
		lease := wire.NewLease(chain, nil)
		if released {
			lease.Release()
		} else {
			defer lease.Release()
		}
		column := ColumnSpec{Name: "v"}
		decoders := fuzzDecoders(t)
		if int(which) < len(decoders) {
			decoders[which](column, chain, lease)
			return
		}
		column.Type = fuzzType(t, &sel, 4)
		columns := NewColumns(column)
		plan, err := OneColumn(DynamicCodec).Compile(columns)
		if err != nil {
			t.Fatalf("DynamicCodec rejected %s: %v", column.Type, err)
		}
		if value, err := decodeRow(plan, newRowReader(columns, chain, lease)); err == nil {
			walkValue(value, 0)
		}
	})
}

// TestHostileCollectionFraming decodes collection cells whose counts and
// element lengths overflow or contradict the cell: every decoder must fail
// without allocating for the count it was sent.
func TestHostileCollectionFraming(t *testing.T) {
	u32 := func(v uint32) []byte { return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)} }
	cell := func(parts ...[]byte) []byte {
		var body []byte
		for _, p := range parts {
			body = append(body, p...)
		}
		return append(u32(uint32(len(body))), body...)
	}
	element := append(u32(4), 0, 0, 0, 1)
	bodies := map[string][]byte{
		"count 2^31":            cell(u32(1<<31), element),
		"count 2^32-1":          cell(u32(math.MaxUint32), element),
		"count 2^31-1":          cell(u32(math.MaxInt32), element),
		"count past the cell":   cell(u32(2), element),
		"element length -2":     cell(u32(1), u32(0xfffffffe)),
		"element past the cell": cell(u32(1), u32(100), []byte{0, 0, 0, 1}),
		"trailing bytes":        cell(u32(1), element, []byte{0}),
		"no count":              cell([]byte{0, 0}),
	}
	listType := CodecTestMust(ListType(IntType))
	mapType := CodecTestMust(MapType(IntType, IntType))
	decoders := map[string]func(wire.Chain) error{
		"ListCodec":      oneColumnError(listType, ListCodec(Int32Codec)),
		"SetCodec":       oneColumnError(CodecTestMust(SetType(IntType)), SetCodec(Int32Codec)),
		"MapCodec":       oneColumnError(mapType, MapCodec(Int32Codec, Int32Codec)),
		"GoMapCodec":     oneColumnError(mapType, GoMapCodec(Int32Codec, Int32Codec)),
		"VectorCodec":    oneColumnError(CodecTestMust(VectorType(TextType, 2)), VectorCodec(StringCodec)),
		"Dynamic list":   oneColumnError(listType, DynamicCodec),
		"Dynamic map":    oneColumnError(mapType, DynamicCodec),
		"Dynamic vector": oneColumnError(CodecTestMust(VectorType(TextType, 2)), DynamicCodec),
	}
	for name, body := range bodies {
		for decoderName, decode := range decoders {
			if strings.Contains(decoderName, "ector") && name == "no count" {
				continue // a vector has no count: two empty strings
			}
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			err := decode(wire.ChainOf(body))
			runtime.ReadMemStats(&after)
			if err == nil {
				t.Errorf("%s: %s accepted the cell", name, decoderName)
			}
			if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 1<<20 {
				t.Errorf("%s: %s allocated %d bytes", name, decoderName, allocated)
			}
		}
	}
}

func oneColumnError[T any](typ DataType, decoder Decoder[T]) func(wire.Chain) error {
	columns := NewColumns(ColumnSpec{Name: "v", Type: typ})
	plan := CodecTestMust(OneColumn(decoder).Compile(columns))
	return func(chain wire.Chain) error {
		reader := newRowReader(columns, chain, nil)
		v, err := decodeRow(plan, reader)
		if err == nil {
			if value, ok := any(v).(Value); ok {
				walkValue(value, 0)
			}
			err = finishRowReader(reader)
		}
		return err
	}
}
