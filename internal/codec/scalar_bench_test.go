// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"bytes"
	"fmt"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

var scalarIntSink int64
var scalarBytesSink cqltype.Bytes
var scalarTextSink cqltype.Text

var uuidSink cqltype.UUID

var scalarVarintSink Varint

func BenchmarkScalar(b *testing.B) {
	// These two cases exactly reproduce the original benchmark work: varchar into
	// existing []byte and UUID into a fixed scalar. Borrowed Text is measured apart.
	b.Run("BaselineEquivalent", func(b *testing.B) {
		b.Run("Varchar1024IntoBytes", func(b *testing.B) {
			data := bytes.Repeat([]byte{'x'}, 1024)
			typ := cqltype.VarcharType
			r := NewValueReader(typ, cqltype.Present, CodecTestMust(wire.NewChain(wire.Borrow(data))), nil)
			dst := make([]byte, 1024)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				text, e := TextCodec.Decode(r)
				if e != nil {
					b.Fatal(e)
				}
				if _, e = text.Bytes().CopyTo(dst); e != nil {
					b.Fatal(e)
				}
			}
		})
		b.Run("UUID", func(b *testing.B) {
			r := ScalarReader(b, cqltype.TypeUUID, cqltype.Present, make([]byte, 16))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				v, e := UUIDCodec.Decode(r)
				if e != nil {
					b.Fatal(e)
				}
				uuidSink = v
			}
		})
	})
	b.Run("BlobDecode", func(b *testing.B) {
		data := make([]byte, 1<<20)
		r := ScalarReader(b, cqltype.TypeBlob, cqltype.Present, data[:123], data[123:])
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for b.Loop() {
			v, err := BlobCodec.Decode(r)
			if err != nil {
				b.Fatal(err)
			}
			scalarBytesSink = v
		}
	})
	b.Run("BlobEncodeBorrowed", func(b *testing.B) {
		data := make([]byte, 1<<20)
		typ := scalarType(b, cqltype.TypeBlob)
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for b.Loop() {
			builder := wire.NewBuilder(16)
			w := NewValueWriter(builder, typ, true)
			if err := ByteSliceCodec.Encode(w, data); err != nil {
				b.Fatal(err)
			}
			if err := FinishValueWriter(w); err != nil {
				b.Fatal(err)
			}
			if _, err := builder.Freeze(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("BorrowedSizes", func(b *testing.B) {
		for _, size := range []int{0, 32, 4096, 500 << 10} {
			for _, text := range []bool{false, true} {
				b.Run(fmt.Sprintf("bytes%d/text%t", size, text), func(b *testing.B) {
					data := bytes.Repeat([]byte{'q'}, size)
					typ := cqltype.BlobType
					if text {
						typ = cqltype.TextType
					}
					stringData := string(data)
					b.Run("bind", func(b *testing.B) {
						b.ReportAllocs()
						b.SetBytes(int64(size))
						for b.Loop() {
							if text {
								if _, e := EncodeValueBorrowed(typ, StringCodec, stringData); e != nil {
									b.Fatal(e)
								}
							} else {
								if _, e := EncodeValueBorrowed(typ, ByteSliceCodec, data); e != nil {
									b.Fatal(e)
								}
							}
						}
					})
					chain := CodecTestMust(wire.NewChain(wire.Borrow(data)))
					reader := NewValueReader(typ, cqltype.Present, chain, nil)
					b.Run("decode", func(b *testing.B) {
						b.ReportAllocs()
						b.SetBytes(int64(size))
						for b.Loop() {
							if text {
								v, e := TextCodec.Decode(reader)
								if e != nil {
									b.Fatal(e)
								}
								scalarTextSink = v
							} else {
								v, e := BlobCodec.Decode(reader)
								if e != nil {
									b.Fatal(e)
								}
								scalarBytesSink = v
							}
						}
					})
				})
			}
		}
	})
	b.Run("Int64Decode", func(b *testing.B) {
		data := []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
		r := ScalarReader(b, cqltype.TypeBigInt, cqltype.Present, data[:3], data[3:])
		b.ReportAllocs()
		b.SetBytes(8)
		b.ResetTimer()
		for b.Loop() {
			v, err := Int64.Decode(r)
			if err != nil {
				b.Fatal(err)
			}
			scalarIntSink = v
		}
	})
	b.Run("TextDecode", func(b *testing.B) {
		data := make([]byte, 1<<20)
		for i := range data {
			data[i] = 'a'
		}
		r := ScalarReader(b, cqltype.TypeText, cqltype.Present, data[:123], data[123:])
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for b.Loop() {
			v, err := TextCodec.Decode(r)
			if err != nil {
				b.Fatal(err)
			}
			scalarTextSink = v
		}
	})
	b.Run("VarintDecode", func(b *testing.B) {
		data := make([]byte, 1<<20)
		data[0] = 0x01
		r := ScalarReader(b, cqltype.TypeVarint, cqltype.Present, data[:123], data[123:])
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for b.Loop() {
			v, err := VarintCodec.Decode(r)
			if err != nil {
				b.Fatal(err)
			}
			scalarVarintSink = v
		}
	})
}
