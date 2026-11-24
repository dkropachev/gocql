//go:build gocql_perf

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"bytes"
	"errors"
	"fmt"
	"math/bits"
	"net/netip"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestCodecPerf(t *testing.T) {
	t.Run("Borrow", func(t *testing.T) {
		t.Run("IdentityAndWrites", func(t *testing.T) {
			for _, size := range []int{0, 32, 4096, 500 << 10} {
				for _, text := range []bool{false, true} {
					t.Run(fmt.Sprintf("size%d/text%t", size, text), func(t *testing.T) {
						data := bytes.Repeat([]byte{'q'}, size)
						typ := BlobType
						if text {
							typ = TextType
						}
						wire.ResetPerfCounters()
						var encoded EncodedValue
						var err error
						if text {
							encoded, err = EncodeValueBorrowed(typ, StringCodec, string(data))
						} else {
							encoded, err = EncodeValueBorrowed(typ, ByteSliceCodec, data)
						}
						if err != nil {
							t.Fatal(err)
						}
						c := wire.ReadPerfCounters()
						if c.CopyBytes != 0 {
							t.Fatal(c)
						}
						var view Bytes
						if text {
							v, e := DecodeValue(TextCodec, encoded)
							if e != nil {
								t.Fatal(e)
							}
							view = v.Bytes()
						} else {
							view, err = DecodeValue(BlobCodec, encoded)
							if err != nil {
								t.Fatal(err)
							}
						}
						eachBytes(view, func(s wire.Span) error {
							body := bytesChain(encoded.body)
							same := false
							body.Each(func(p wire.Span) error {
								if len(s.Bytes()) > 0 && len(p.Bytes()) > 0 && &s.Bytes()[0] == &p.Bytes()[0] {
									same = true
								}
								return nil
							})
							if !same {
								t.Fatal("decoder payload backing changed")
							}
							return nil
						})
						if !text && size > 0 {
							eachBytes(view, func(s wire.Span) error {
								if &s.Bytes()[0] != &data[0] {
									t.Fatal("bind payload copied")
								}
								return nil
							})
						}
						t.Logf("bind+decode counters=%+v payload_bytes=%d", wire.ReadPerfCounters(), size)
					})
				}
			}
			integer := BigIntType
			wire.ResetPerfCounters()
			value := CodecTestMust(EncodeValue(integer, Int64Codec, int64(42)))
			c := wire.ReadPerfCounters()
			if c.ScalarWrites != 2 || c.ScalarBytes != 12 || c.CopyBytes != 0 {
				t.Fatal(c)
			}
			if CodecTestMust(DecodeValue(Int64Codec, value)) != 42 {
				t.Fatal("scalar")
			}
			t.Logf("owned scalar: %+v", c)
		})
		t.Run("ScalarArenaWrites", func(t *testing.T) {
			oneScalarWrite(t, UUIDType, UUIDCodec, UUID{})
			oneScalarWrite(t, InetType, InetCodec, netip.MustParseAddr("2001:db8::1"))
			oneScalarWrite(t, DurationType, DurationCodec, Duration{Months: 2147483647, Days: 2147483647, Nanoseconds: 9223372036854775807})
		})
	})
	t.Run("CompiledScalar", func(t *testing.T) {
		integer := IntType
		text := TextType
		blob := BlobType
		list := CodecTestMust(ListType(integer))
		type result struct {
			N        int32
			Text     Text
			Blob     Bytes
			List     []int32
			Nullable Nullable[int32]
			Empty    MaybeEmpty[int32]
			Dynamic  Value
		}
		cols := NewColumns(ColumnSpec{Name: "n", Type: integer}, ColumnSpec{Name: "text", Type: text}, ColumnSpec{Name: "blob", Type: blob}, ColumnSpec{Name: "list", Type: list}, ColumnSpec{Name: "nullable", Type: integer}, ColumnSpec{Name: "empty", Type: integer}, ColumnSpec{Name: "dynamic", Type: integer})
		body := encodeTestParams(t, cols, DynamicParams, []Value{IntValue(3), TextValue("safe-🙂"), BlobValue([]byte("borrowed")), CodecTestMust(ListValue(list, []Value{IntValue(7)})), IntValue(4), IntValue(5), IntValue(6)})
		decoder := Row(Column("n", Int32Codec, func(r *result) *int32 { return &r.N }), Column("text", TextCodec, func(r *result) *Text { return &r.Text }), Column("blob", BlobCodec, func(r *result) *Bytes { return &r.Blob }), Column("list", ListDecoder(Int32Codec), func(r *result) *[]int32 { return &r.List }), Column("nullable", NullableDecoder(Int32Codec), func(r *result) *Nullable[int32] { return &r.Nullable }), Column("empty", EmptyCodec(Int32Codec), func(r *result) *MaybeEmpty[int32] { return &r.Empty }), Column("dynamic", DynamicCodec, func(r *result) *Value { return &r.Dynamic }))
		plan := CodecTestMust(decoder.Compile(cols))
		lease := wire.NewLease(body, nil)
		var dst result
		ResetPerfCounters()
		for page := 0; page < 10000; page++ {
			if e := plan.DecodeInto(newRowReader(cols, body, lease), &dst); e != nil {
				t.Fatal(e)
			}
			if len(dst.List) != 1 || dst.List[0] != 7 {
				t.Fatal(dst.List)
			}

		}
		comp, maps := ReadPerfCounters()
		if comp != 0 || maps != 0 || ReadPerfTypeChecks() != 0 {
			t.Fatal("warm recursive/type work", comp, maps, ReadPerfTypeChecks())
		}
		t.Log("10000 compiled scalar/text/blob/container/nullable/empty/dynamic rows Compile=0 CheckType=0 name_maps=0")
		changed := NewColumns(ColumnSpec{Name: "n", Type: text})
		direct := CodecTestMust(OneColumn(Int32Codec).Compile(NewColumns(ColumnSpec{Name: "n", Type: integer})))
		if _, e := decodeRow(direct, newRowReader(changed, CodecTestMust(wire.NewChain(wire.Borrow([]byte{0, 0, 0, 4, 0, 0, 0, 3}))), nil)); e == nil {
			t.Fatal("mismatched reader accepted")
		}
		lease.Release()
		if e := plan.DecodeInto(newRowReader(cols, body, lease), &dst); !errors.Is(e, ErrReleased) {
			t.Fatal("expired compiled reader", e)
		}
	})
	t.Run("Dynamic", func(t *testing.T) {
		t.Run("IndexBounds", func(t *testing.T) {
			blob := BlobType
			for _, n := range []int{1, 128, 4096} {
				for _, fragments := range []int{1, 256, 65536} {
					t.Run(fmt.Sprintf("N%d/S%d", n, fragments), func(t *testing.T) {
						specs := make([]ColumnSpec, n)
						values := make([]Value, n)
						for i := range specs {
							specs[i] = ColumnSpec{Name: fmt.Sprint("v", i), Type: blob}
							values[i] = BlobValue(make([]byte, (1<<20)/n))
						}
						cols := NewColumns(specs...)
						raw := bytesView(dynamicTestRowBody(t, cols, values), wire.Ref{}).OwnedBytes()
						spans := make([]wire.Span, fragments)
						for i := range spans {
							spans[i] = wire.Borrow(raw[len(raw)*i/fragments : len(raw)*(i+1)/fragments])
						}
						chain := CodecTestMust(wire.NewChain(spans...))
						plan := CodecTestMust(DynamicRows.Compile(cols))
						row := CodecTestMust(decodeRow(plan, newRowReader(cols, chain, nil)))
						ResetPerfCounters()
						wire.ResetPerfCounters()
						CodecTestMust(row.At(n - 1))
						first := wire.ReadPerfCounters()
						wire.ResetPerfCounters()
						for j := 0; j < 16; j++ {
							CodecTestMust(row.At(n - 1))
						}
						builds, cells := ReadPerfDynamicIndexCounters()
						c := wire.ReadPerfCounters()
						if builds != 1 || cells != int64(n) || first.SequentialVisits > int64(8*n+fragments) || c.SequentialVisits != 0 || c.Takes != 0 || c.IndexComparisons > int64(32*(bits.Len(uint(fragments))+1)) || c.CopyBytes != 0 || c.Retains != 0 || c.Releases != 0 {
							t.Fatal("dynamic index repeated row scan", builds, cells, first, c)
						}
						t.Logf("N=%d S=%d first=%+v J=16 repeated=%+v", n, fragments, first, wire.ReadPerfCounters())
					})
				}
			}
		})
		t.Run("LazyReuse", func(t *testing.T) {
			integer := IntType
			blob := BlobType
			tuple := CodecTestMust(TupleType(integer, blob))
			list := CodecTestMust(ListType(tuple))
			cols := NewColumns(ColumnSpec{Name: "list", Type: list})
			values := make([]Value, 1024)
			for i := range values {
				values[i] = CodecTestMust(TupleValue(tuple, []Value{IntValue(int32(i)), BlobValue(bytes.Repeat([]byte{'x'}, 4096))}))
			}
			sequence := CodecTestMust(ListValue(list, values))
			source := dynamicTestRowBody(t, cols, []Value{sequence})
			plan := CodecTestMust(DynamicRows.Compile(cols))
			ResetPerfCounters()
			wire.ResetPerfCounters()
			for page := 0; page < 4; page++ {
				reader := newRowReader(cols, source, nil)
				row := CodecTestMust(decodeRow(plan, reader))
				v := CodecTestMust(row.Get("list"))
				it := CodecTestMust(v.Elements())
				n := 0
				for it.Next() {
					tuple := CodecTestMust(it.Value().Elements())
					for tuple.Next() {
						_ = tuple.Value()
					}
					if tuple.Err() != nil {
						t.Fatal(tuple.Err())
					}
					n++
				}
				if it.Err() != nil || n != 1024 {
					t.Fatal(n, it.Err())
				}
			}
			comp, maps := ReadPerfCounters()
			c := wire.ReadPerfCounters()
			if comp != 0 || maps != 0 || c.CopyBytes != 0 || c.Retains != 0 || c.Releases != 0 || ReadPerfTypeChecks() != 0 {
				t.Fatal(comp, maps, c)
			}
			t.Logf("4 pages 1024 nested 4KiB payloads warm_compiles=%d name_maps=%d counts=%+v", comp, maps, c)
		})
	})
	t.Run("LazyTraversal", func(t *testing.T) {
		integer := IntType
		for _, n := range []int{1, 1024} {
			t.Run(fmt.Sprintf("list%d", n), func(t *testing.T) {
				sequenceWork(t, CodecTestMust(ListType(integer)), ListEncoder(Int32Codec), ListDecoder(Int32Codec), make([]int32, n))
			})
			t.Run(fmt.Sprintf("vector%d", n), func(t *testing.T) {
				sequenceWork(t, CodecTestMust(VectorType(integer, uint16(n))), VectorEncoder(Int32Codec), VectorDecoder(Int32Codec), make([]int32, n))
			})
			t.Run(fmt.Sprintf("tuple%d", n), func(t *testing.T) {
				tuple := CodecTestMust(TupleType(integer))
				sequenceWork(t, CodecTestMust(ListType(tuple)), ListEncoder(TupleEncoder(OneParam(Int32Codec))), ListDecoder(TupleDecoder(OneColumn(Int32Codec))), make([]int32, n))
			})
			t.Run(fmt.Sprintf("udt%d", n), func(t *testing.T) {
				udt := CodecTestMust(UDTType("ks", "record", ColumnSpec{Name: "n", Type: integer}))
				o := UDTOptions{Keyspace: "ks", Name: "record"}
				sequenceWork(t, CodecTestMust(ListType(udt)), ListEncoder(UDTEncoder(OneParam(Int32Codec), o)), ListDecoder(UDTDecoder(OneColumn(Int32Codec), o)), make([]int32, n))
			})
			t.Run(fmt.Sprintf("map%d", n), func(t *testing.T) {
				typ := CodecTestMust(MapType(integer, integer))
				values := make([]Entry[int32, int32], n)
				for i := range values {
					values[i] = Entry[int32, int32]{Key: int32(i), Value: int32(i)}
				}
				encoded := CodecTestMust(EncodeValueBorrowed(typ, MapEncoder(Int32Codec, Int32Codec), values))
				compiled := CodecTestMust(compileValueDecoder(MapDecoder(Int32Codec, Int32Codec), typ))
				ResetPerfCounters()
				wire.ResetPerfCounters()
				v := CodecTestMust(compiled.Decode(valueReaderFromLease(typ, Present, bytesChain(encoded.body), nil)))
				if len(v) != n {
					t.Fatal(len(v))
				}
				c := wire.ReadPerfCounters()
				comp, maps := ReadPerfCounters()
				if c.CopyBytes != 0 || comp != 0 || maps != 0 || ReadPerfTypeChecks() != 0 {
					t.Fatal(c, comp, maps)
				}
				t.Logf("N=%d counts=%+v", n, c)
			})
		}
	})
	t.Run("NestedVisitBounds", func(t *testing.T) {
		for _, depth := range []int{1, 4, 16} {
			for _, fragments := range []int{1, 256, 65536} {
				t.Run(fmt.Sprintf("depth%d/S%d", depth, fragments), func(t *testing.T) {
					typ := BlobType
					v := BlobValue(make([]byte, 1<<20))
					for i := 0; i < depth; i++ {
						typ = CodecTestMust(ListType(typ))
						v = CodecTestMust(ListValue(typ, []Value{v}))
					}
					encoded := CodecTestMust(EncodeValueBorrowed(typ, DynamicCodec, v))
					raw := encoded.Bytes().OwnedBytes()
					spans := make([]wire.Span, fragments)
					for i := range spans {
						spans[i] = wire.Borrow(raw[len(raw)*i/fragments : len(raw)*(i+1)/fragments])
					}
					chain := CodecTestMust(wire.NewChain(spans...))
					plan := CodecTestMust(compileValueDecoder(DynamicCodec, typ))
					wire.ResetPerfCounters()
					ResetPerfCounters()
					value := CodecTestMust(plan.Decode(valueReaderFromLease(typ, Present, chain, nil)))
					for i := 0; i < depth; i++ {
						it := CodecTestMust(value.Elements())
						if !it.Next() {
							t.Fatal(it.Err())
						}
						value = it.Value()
						if it.Next() || it.Err() != nil {
							t.Fatal(it.Err())
						}
					}
					c := wire.ReadPerfCounters()
					if c.SequentialVisits > int64(32*(depth+1)) || c.IndexComparisons > int64(8*depth*(bits.Len(uint(fragments))+1)) || c.CopyBytes != 0 || ReadPerfTypeChecks() != 0 {
						t.Fatal("nested skip growth", c)
					}
					t.Logf("D=%d S=%d actual=%+v type_checks=%d", depth, fragments, wire.ReadPerfCounters(), ReadPerfTypeChecks())
				})
			}
		}
	})
}
