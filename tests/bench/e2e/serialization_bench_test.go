/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package e2e_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

func BenchmarkSerialization(b *testing.B) {
	b.Run("Values", func(b *testing.B) {
		b.Run("Int", func(b *testing.B) { benchmarkValue(b, scalar(cql.TypeInt), cql.Int32Codec, cql.Int32Codec, int32(42)) })
		for _, size := range []int{100, 1024, 1024 * 1024} {
			body := bytes.Repeat([]byte("x"), size)
			b.Run(fmt.Sprintf("Blob/%d", size), func(b *testing.B) {
				benchmarkValue(b, scalar(cql.TypeBlob), cql.BlobCodec, cql.BlobCodec, cql.BorrowBytes(body))
			})
			b.Run(fmt.Sprintf("Text/%d", size), func(b *testing.B) {
				benchmarkValue(b, scalar(cql.TypeText), cql.StringEncoder, cql.TextCodec, string(body))
			})
		}
		b.Run("UUID", func(b *testing.B) {
			benchmarkValue(b, scalar(cql.TypeUUID), cql.UUIDCodec, cql.UUIDCodec, cql.UUID{})
		})
		b.Run("Duration", func(b *testing.B) {
			benchmarkValue(b, scalar(cql.TypeDuration), cql.DurationCodec, cql.DurationCodec, cql.Duration{Nanoseconds: 300000000000})
		})
		b.Run("Timestamp", func(b *testing.B) {
			benchmarkValue(b, scalar(cql.TypeTimestamp), cql.TimestampCodec, cql.TimestampCodec, cql.Timestamp(1700000000000))
		})
		text, integer := scalar(cql.TypeText), scalar(cql.TypeInt)
		list, _ := cql.ListType(text)
		set, _ := cql.SetType(text)
		mapping, _ := cql.MapType(text, integer)
		b.Run("List", func(b *testing.B) {
			benchmarkValue(b, list, cql.ListEncoder(cql.StringEncoder), cql.ListDecoder(cql.TextCodec), []string{"foo", "bar", "baz"})
		})
		b.Run("Set", func(b *testing.B) {
			benchmarkValue(b, set, cql.SetEncoder(cql.StringEncoder), cql.SetDecoder(cql.TextCodec), []string{"foo", "bar", "baz"})
		})
		b.Run("Map", func(b *testing.B) {
			benchmarkValue(b, mapping, cql.MapEncoder(cql.StringEncoder, cql.Int32Codec), cql.MapDecoder(cql.TextCodec, cql.Int32Codec), []cql.Entry[string, int32]{{Key: "a", Value: 1}, {Key: "b", Value: 2}})
		})
		type record struct {
			ID    cql.UUID
			Name  string
			Value int32
		}
		type view struct {
			ID    cql.UUID
			Name  cql.Text
			Value int32
		}
		params := cql.Params(cql.ParamAt(0, cql.UUIDCodec, func(v record) cql.UUID { return v.ID }), cql.ParamAt(1, cql.StringEncoder, func(v record) string { return v.Name }), cql.ParamAt(2, cql.Int32Codec, func(v record) int32 { return v.Value }))
		rows := cql.Row(cql.ColumnAt(0, cql.UUIDCodec, func(v *view) *cql.UUID { return &v.ID }), cql.ColumnAt(1, cql.TextCodec, func(v *view) *cql.Text { return &v.Name }), cql.ColumnAt(2, cql.Int32Codec, func(v *view) *int32 { return &v.Value }))
		tuple, _ := cql.TupleType(scalar(cql.TypeUUID), text, integer)
		b.Run("Tuple", func(b *testing.B) {
			benchmarkValue(b, tuple, cql.TupleEncoder(params), cql.TupleDecoder(rows), record{Name: "test tuple", Value: 123})
		})
		udt, _ := cql.UDTType("myks", "myudt", cql.ColumnSpec{Name: "id", Type: scalar(cql.TypeUUID)}, cql.ColumnSpec{Name: "name", Type: text}, cql.ColumnSpec{Name: "value", Type: integer})
		opts := cql.UDTOptions{Keyspace: "myks", Name: "myudt"}
		udtEnc := cql.UDTEncoder(params, opts)
		udtDec := cql.UDTDecoder(rows, opts)
		b.Run("UDT", func(b *testing.B) { benchmarkValue(b, udt, udtEnc, udtDec, record{Name: "test udt", Value: 123}) })
		nestedMap, _ := cql.MapType(text, udt)
		nested, _ := cql.ListType(nestedMap)
		nestedValue := [][]cql.Entry[string, record]{{{Key: "key1", Value: record{Name: "name1", Value: 123}}, {Key: "key2", Value: record{Name: "name2", Value: 456}}}, {{Key: "key3", Value: record{Name: "name3", Value: 789}}}}
		b.Run("Nested/3-lvl", func(b *testing.B) {
			benchmarkValue(b, nested, cql.ListEncoder(cql.MapEncoder(cql.StringEncoder, udtEnc)), cql.ListDecoder(cql.MapDecoder(cql.TextCodec, udtDec)), nestedValue)
		})
	})

	b.Run("Vector", func(b *testing.B) {
		for _, dim := range []int{128, 384, 768, 1536} {
			b.Run(fmt.Sprint(dim), func(b *testing.B) {
				typ, err := cql.VectorDataType(scalar(cql.TypeFloat), uint16(dim))
				if err != nil {
					b.Fatal(err)
				}
				vector := make([]float32, dim)
				for i := range vector {
					vector[i] = float32(i) * 0.1
				}
				benchmarkValue(b, typ, cql.VectorEncoder(cql.Float32Codec), cql.VectorDecoder(cql.Float32Codec), vector)
			})
		}
	})
}

// benchmarkValue measures explicit borrowed encoding and borrowed decoding.
// Owned conversions have separate names so their copy costs remain visible.
func benchmarkValue[P, R any](b *testing.B, typ cql.DataType, enc cql.Encoder[P], dec cql.Decoder[R], value P) {
	encoded, err := cql.EncodeValueBorrowed(typ, enc, value)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("EncodeBorrowed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := cql.EncodeValueBorrowed(typ, enc, value); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("EncodeOwned", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := cql.EncodeValue(typ, enc, value); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("DecodeBorrowed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := cql.DecodeValue(dec, encoded); err != nil {
				b.Fatal(err)
			}
		}
	})
}
func scalar(kind cql.Type) cql.DataType {
	t, err := cql.ScalarType(kind)
	if err != nil {
		panic(err)
	}
	return t
}
