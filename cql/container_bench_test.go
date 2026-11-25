// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"fmt"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

var containerIntSink int32
var containerSequenceSink []int32

func BenchmarkContainer(b *testing.B) {
	integer := IntType
	for _, n := range []int{1, 1024} {
		for _, kind := range []string{"list", "set", "vector", "tuple", "udt", "map"} {
			b.Run(fmt.Sprintf("%s/%d", kind, n), func(b *testing.B) {
				if kind == "map" {
					typ := CodecTestMust(MapType(integer, integer))
					encoded := CodecTestMust(encodeValueBorrowed(typ, MapEncoder(Int32Codec, Int32Codec), make([]Entry[int32, int32], n)))
					dec := CodecTestMust(compileValueDecoder(MapDecoder(Int32Codec, Int32Codec), typ))
					reader := valueReaderFromLease(typ, Present, bytesChain(encoded.body), nil)
					b.ReportAllocs()
					b.SetBytes(reader.body.Len())
					b.ResetTimer()
					for b.Loop() {
						values, err := dec.Decode(reader)
						if err != nil {
							b.Fatal(err)
						}
						containerIntSink = values[n-1].Value
					}
					return
				}
				typ := CodecTestMust(ListType(integer))
				enc, dec := ListEncoder(Int32Codec), ListDecoder(Int32Codec)
				switch kind {
				case "set":
					typ = CodecTestMust(SetType(integer))
					enc, dec = SetEncoder(Int32Codec), SetDecoder(Int32Codec)
				case "vector":
					typ = CodecTestMust(VectorType(integer, uint16(n)))
					enc, dec = VectorEncoder(Int32Codec), VectorDecoder(Int32Codec)
				case "tuple":
					tuple := CodecTestMust(TupleType(integer))
					typ = CodecTestMust(ListType(tuple))
					enc, dec = ListEncoder(TupleEncoder(OneParam(Int32Codec))), ListDecoder(TupleDecoder(OneColumn(Int32Codec)))
				case "udt":
					udt := CodecTestMust(UDTType("ks", "r", ColumnSpec{Name: "n", Type: integer}))
					typ = CodecTestMust(ListType(udt))
					opts := UDTOptions{Keyspace: "ks", Name: "r"}
					enc, dec = ListEncoder(UDTEncoder(OneParam(Int32Codec), opts)), ListDecoder(UDTDecoder(OneColumn(Int32Codec), opts))
				}
				encoded := CodecTestMust(encodeValueBorrowed(typ, enc, make([]int32, n)))
				compiled := CodecTestMust(compileValueDecoder(dec, typ))
				for _, segmented := range []bool{false, true} {
					b.Run(fmt.Sprintf("segmented=%t", segmented), func(b *testing.B) {
						chain := bytesChain(encoded.body)
						if segmented {
							data := encoded.Bytes().ownedBytes()
							spans := make([]wire.Span, len(data))
							for i := range data {
								spans[i] = wire.Borrow(data[i : i+1])
							}
							chain = CodecTestMust(wire.NewChain(spans...))
						}
						reader := valueReaderFromLease(typ, Present, chain, nil)
						b.ReportAllocs()
						b.SetBytes(chain.Len())
						b.ResetTimer()
						for b.Loop() {
							values, err := compiled.Decode(reader)
							if err != nil {
								b.Fatal(err)
							}
							containerSequenceSink = values
						}
					})
				}
			})
		}
	}
}
