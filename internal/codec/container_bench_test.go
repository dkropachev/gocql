// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"fmt"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

var containerIntSink int32
var containerSequenceSink Sequence[int32]

var collectSink []int32

func BenchmarkContainer(b *testing.B) {
	b.Run("CompiledDecode", func(b *testing.B) {
		typ, _ := cqltype.ListType(rowTestType(b, cqltype.TypeInt))
		encoded, err := EncodeValueBorrowed(typ, ListEncoder(Int32), make([]int32, 1024))
		if err != nil {
			b.Fatal(err)
		}
		decoder, err := compileValueDecoder(ListDecoder(Int32), typ)
		if err != nil {
			b.Fatal(err)
		}
		reader := NewValueReader(typ, cqltype.Present, cqltype.BytesChain(encoded.body), nil)
		b.ReportAllocs()
		b.SetBytes(reader.body.Len())
		b.ResetTimer()
		for b.Loop() {
			sequence, err := decoder.Decode(reader)
			if err != nil {
				b.Fatal(err)
			}
			containerSequenceSink = sequence
		}
	})
	b.Run("Iteration", func(b *testing.B) {
		for _, count := range []int{1, 1024} {
			b.Run(fmt.Sprint(count), func(b *testing.B) {
				typ, _ := cqltype.ListType(rowTestType(b, cqltype.TypeInt))
				values := make([]int32, count)
				encoded, err := EncodeValueBorrowed(typ, ListEncoder(Int32), values)
				if err != nil {
					b.Fatal(err)
				}
				sequence, err := DecodeValue(ListDecoder(Int32), encoded)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.SetBytes(int64(count) * 8)
				b.ResetTimer()
				for b.Loop() {
					it := sequence.Iterator()
					for it.Next() {
						containerIntSink = it.Value()
					}
					if it.Err() != nil {
						b.Fatal(it.Err())
					}
				}
			})
		}
	})
	b.Run("LazyAndMaterialized", func(b *testing.B) {
		integer := cqltype.IntType
		for _, n := range []int{1, 1024} {
			for _, kind := range []string{"list", "vector", "tuple", "udt", "map"} {
				b.Run(fmt.Sprintf("%s/%d", kind, n), func(b *testing.B) {
					if kind == "map" {
						typ := CodecTestMust(cqltype.MapType(integer, integer))
						entries := make([]Entry[int32, int32], n)
						encoded := CodecTestMust(EncodeValueBorrowed(typ, MapEncoder(Int32, Int32), entries))
						m := CodecTestMust(DecodeValue(MapDecoder(Int32, Int32), encoded))
						b.ReportAllocs()
						b.ResetTimer()
						for b.Loop() {
							it := m.Iterator()
							for it.Next() {
								containerIntSink = it.Value().Value
							}
							if e := it.Err(); e != nil {
								b.Fatal(e)
							}
						}
						return
					}
					typ := CodecTestMust(cqltype.ListType(integer))
					enc, dec := ListEncoder(Int32), ListDecoder(Int32)
					if kind == "vector" {
						typ = CodecTestMust(cqltype.VectorDataType(integer, uint16(n)))
						enc, dec = VectorEncoder(Int32), VectorDecoder(Int32)
					}
					if kind == "tuple" {
						tuple := CodecTestMust(cqltype.TupleType(integer))
						typ = CodecTestMust(cqltype.ListType(tuple))
						enc, dec = ListEncoder(TupleEncoder(OneParam(Int32))), ListDecoder(TupleDecoder(OneColumn(Int32)))
					}
					if kind == "udt" {
						udt := CodecTestMust(cqltype.UDTType("ks", "r", cqltype.ColumnSpec{Name: "n", Type: integer}))
						typ = CodecTestMust(cqltype.ListType(udt))
						o := UDTOptions{Keyspace: "ks", Name: "r"}
						enc, dec = ListEncoder(UDTEncoder(OneParam(Int32), o)), ListDecoder(UDTDecoder(OneColumn(Int32), o))
					}
					encoded := CodecTestMust(EncodeValueBorrowed(typ, enc, make([]int32, n)))
					seq := CodecTestMust(DecodeValue(dec, encoded))
					for _, mode := range []string{"iterate", "collect", "clone"} {
						b.Run(mode, func(b *testing.B) {
							b.ReportAllocs()
							b.ResetTimer()
							for b.Loop() {
								switch mode {
								case "iterate":
									it := seq.Iterator()
									for it.Next() {
										containerIntSink = it.Value()
									}
									if e := it.Err(); e != nil {
										b.Fatal(e)
									}
								case "collect":
									values, e := Collect(seq.Iterator())
									if e != nil {
										b.Fatal(e)
									}
									collectSink = values
								case "clone":
									containerSequenceSink = seq.Clone()
								}
							}
						})
					}
				})
			}
		}
	})
	b.Run("SegmentedIteration", func(b *testing.B) {
		typ, _ := cqltype.ListType(rowTestType(b, cqltype.TypeInt))
		encoded, err := EncodeValueBorrowed(typ, ListEncoder(Int32), make([]int32, 1024))
		if err != nil {
			b.Fatal(err)
		}
		data := encoded.Bytes().OwnedBytes()
		spans := make([]wire.Span, len(data))
		for i := range data {
			spans[i] = wire.Borrow(data[i : i+1])
		}
		chain, _ := wire.NewChain(spans...)
		sequence, err := ListDecoder(Int32).Decode(NewValueReader(typ, cqltype.Present, chain, nil))
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for b.Loop() {
			it := sequence.Iterator()
			for it.Next() {
				containerIntSink = it.Value()
			}
			if it.Err() != nil {
				b.Fatal(it.Err())
			}
		}
	})
	b.Run("TupleIteration", func(b *testing.B) {
		tuple, _ := cqltype.TupleType(rowTestType(b, cqltype.TypeInt))
		typ, _ := cqltype.ListType(tuple)
		encoded, err := EncodeValueBorrowed(typ, ListEncoder(TupleEncoder(OneParam(Int32))), make([]int32, 1024))
		if err != nil {
			b.Fatal(err)
		}
		sequence, err := DecodeValue(ListDecoder(TupleDecoder(OneColumn(Int32))), encoded)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			it := sequence.Iterator()
			for it.Next() {
				containerIntSink = it.Value()
			}
			if it.Err() != nil {
				b.Fatal(it.Err())
			}
		}
	})
}
