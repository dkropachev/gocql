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
var containerSequenceSink []int32

func BenchmarkContainer(b *testing.B) {
	integer := cqltype.IntType
	for _, n := range []int{1, 1024} {
		for _, kind := range []string{"list", "set", "vector", "tuple", "udt", "map"} {
			b.Run(fmt.Sprintf("%s/%d", kind, n), func(b *testing.B) {
				if kind == "map" {
					typ := CodecTestMust(cqltype.MapType(integer, integer))
					encoded := CodecTestMust(EncodeValueBorrowed(typ, MapEncoder(Int32, Int32), make([]Entry[int32, int32], n)))
					dec := CodecTestMust(compileValueDecoder(MapDecoder(Int32, Int32), typ))
					reader := NewValueReader(typ, cqltype.Present, cqltype.BytesChain(encoded.body), nil)
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
				typ := CodecTestMust(cqltype.ListType(integer))
				enc, dec := ListEncoder(Int32), ListDecoder(Int32)
				switch kind {
				case "set":
					typ = CodecTestMust(cqltype.SetType(integer))
					enc, dec = SetEncoder(Int32), SetDecoder(Int32)
				case "vector":
					typ = CodecTestMust(cqltype.VectorDataType(integer, uint16(n)))
					enc, dec = VectorEncoder(Int32), VectorDecoder(Int32)
				case "tuple":
					tuple := CodecTestMust(cqltype.TupleType(integer))
					typ = CodecTestMust(cqltype.ListType(tuple))
					enc, dec = ListEncoder(TupleEncoder(OneParam(Int32))), ListDecoder(TupleDecoder(OneColumn(Int32)))
				case "udt":
					udt := CodecTestMust(cqltype.UDTType("ks", "r", cqltype.ColumnSpec{Name: "n", Type: integer}))
					typ = CodecTestMust(cqltype.ListType(udt))
					opts := UDTOptions{Keyspace: "ks", Name: "r"}
					enc, dec = ListEncoder(UDTEncoder(OneParam(Int32), opts)), ListDecoder(UDTDecoder(OneColumn(Int32), opts))
				}
				encoded := CodecTestMust(EncodeValueBorrowed(typ, enc, make([]int32, n)))
				compiled := CodecTestMust(compileValueDecoder(dec, typ))
				for _, segmented := range []bool{false, true} {
					b.Run(fmt.Sprintf("segmented=%t", segmented), func(b *testing.B) {
						chain := cqltype.BytesChain(encoded.body)
						if segmented {
							data := encoded.Bytes().OwnedBytes()
							spans := make([]wire.Span, len(data))
							for i := range data {
								spans[i] = wire.Borrow(data[i : i+1])
							}
							chain = CodecTestMust(wire.NewChain(spans...))
						}
						reader := NewValueReader(typ, cqltype.Present, chain, nil)
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
