/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package codec

import (
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestTupleUDT(t *testing.T) {
	t.Run("Clone", func(t *testing.T) {
		text := rowTestType(t, cqltype.TypeText)
		list, _ := cqltype.ListType(text)
		tuple, _ := cqltype.TupleType(list)
		encoder := TupleEncoder(OneParam(ListEncoder(StringCodec)))
		decoder := TupleDecoder(OneColumn(ListDecoder(TextCodec)))
		encoded, err := EncodeValueBorrowed(tuple, encoder, []string{"first", "second"})
		if err != nil {
			t.Fatal(err)
		}
		lease := wire.NewLease(cqltype.BytesChain(encoded.body), nil)
		encoded.body = cqltype.BytesView(cqltype.BytesChain(encoded.body), wire.RefOf(lease))
		value, err := DecodeValue(decoder, encoded)
		if err != nil {
			t.Fatal(err)
		}
		clone := decoder.Clone(value)
		lease.Release()
		if err := cqltype.CheckBytesAlive(cqltype.TextBytes(value[0])); !errors.Is(err, cqltype.ErrReleased) {
			t.Fatal("released nested element survived")
		}
		if clone[1].String() != "second" {
			t.Fatal("nested clone failed")
		}

	})
	t.Run("Tuple", func(t *testing.T) {
		t.Run("ConcurrentPlansCheckOnce", func(t *testing.T) {
			var checks atomic.Int64
			integer := rowTestType(t, cqltype.TypeInt)
			tuple, _ := cqltype.TupleType(integer)
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "record", Type: tuple})
			decoder := TupleDecoder(OneColumn(containerCheckedInt{checks: &checks}))
			plan, err := OneColumn(decoder).Compile(columns)
			if err != nil {
				t.Fatal(err)
			}
			chain := encodeTestParams(t, columns, OneParam(TupleEncoder(OneParam(Int32))), int32(9))
			var group sync.WaitGroup
			for range 16 {
				group.Add(1)
				go func() {
					defer group.Done()
					for range 30 {
						got, err := plan.Decode(NewRowReader(columns, chain, nil))
						if err != nil || got != 9 {
							t.Errorf("got=%v err=%v", got, err)
							return
						}
					}
				}()
			}
			group.Wait()
			if checks.Load() != 1 {
				t.Fatalf("tuple child metadata checked %d times", checks.Load())
			}
		})
		t.Run("PhysicalFramingAndNullableSuffix", func(t *testing.T) {
			type input struct {
				ID   int32
				Name cqltype.Nullable[string]
			}
			type output struct {
				ID   int32
				Name cqltype.Nullable[cqltype.Text]
			}
			integer, text := rowTestType(t, cqltype.TypeInt), rowTestType(t, cqltype.TypeText)
			tuple, _ := cqltype.TupleType(integer, text)
			encoder := TupleEncoder(Params(
				ParamAt(0, Int32, func(v input) int32 { return v.ID }),
				ParamAt(1, NullableEncoder(StringCodec), func(v input) cqltype.Nullable[string] { return v.Name }),
			))
			decoder := TupleDecoder(Row(
				ColumnAt(0, Int32, func(v *output) *int32 { return &v.ID }),
				ColumnAt(1, NullableDecoder(TextCodec), func(v *output) *cqltype.Nullable[cqltype.Text] { return &v.Name }),
			))
			encoded := containerGolden(t, tuple, encoder, input{ID: 7}, "0000000400000007ffffffff")
			decoded, err := DecodeValue(decoder, encoded)
			if err != nil || decoded.ID != 7 || decoded.Name.Valid {
				t.Fatalf("tuple=%+v err=%v", decoded, err)
			}
			short, _ := hex.DecodeString("0000000400000007")
			decoded, err = DecodeValue(decoder, containerValue(t, tuple, short))
			if err != nil || decoded.ID != 7 || decoded.Name.Valid {
				t.Fatalf("short tuple=%+v err=%v", decoded, err)
			}
			nonnullable := TupleDecoder(PairRows(OneColumn(Int32), OneColumn(TextCodec)))
			if _, err := DecodeValue(nonnullable, containerValue(t, tuple, short)); err == nil {
				t.Fatal("absent suffix became nonnullable zero value")
			}
			for _, bad := range []string{"000000040000000700", "0000000400000007fffffffe", "0000000400000007ffffffff00"} {
				body, _ := hex.DecodeString(bad)
				if _, err := DecodeValue(decoder, containerValue(t, tuple, body)); err == nil {
					t.Fatalf("malformed tuple accepted: %s", bad)
				}
			}
			if err := TupleDecoder(OneColumn(Int32)).CheckType(tuple); err == nil {
				t.Fatal("tuple arity mismatch accepted")
			}
			list, _ := cqltype.ListType(tuple)
			listEncoded := containerGolden(t, list, ListEncoder(encoder), []input{{ID: 7}}, "000000010000000c0000000400000007ffffffff")
			sequence, err := DecodeValue(ListDecoder(decoder), listEncoded)
			if err != nil {
				t.Fatal(err)
			}
			if len(sequence) != 1 || sequence[0].ID != 7 || sequence[0].Name.Valid {
				t.Fatalf("nested tuple: %v", sequence)
			}
			// The tuple stays one column even when adjacent to an ordinary scalar.
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "record", Type: tuple}, cqltype.ColumnSpec{Name: "tail", Type: integer})
			chain := encodeTestParams(t, columns, PairParams(OneParam(encoder), OneParam(Int32)), Pair[input, int32]{First: input{ID: 7}, Second: 9})
			plan, err := PairRows(OneColumn(decoder), OneColumn(Int32)).Compile(columns)
			if err != nil {
				t.Fatal(err)
			}
			row, err := plan.Decode(NewRowReader(columns, chain, nil))
			if err != nil || row.First.ID != 7 || row.Second != 9 {
				t.Fatalf("physical tuple row=%+v err=%v", row, err)
			}
		})
	})
	t.Run("UDTDecoder", func(t *testing.T) {
		t.Run("IgnoredSuffixValidatesFraming", func(t *testing.T) {
			typ, _ := cqltype.UDTType("ks", "record", cqltype.ColumnSpec{Name: "id", Type: rowTestType(t, cqltype.TypeInt)}, cqltype.ColumnSpec{Name: "ignored", Type: rowTestType(t, cqltype.TypeBlob)})
			decoder := UDTDecoder(Row(Column("id", Int32, func(v *int32) *int32 { return v })), UDTOptions{Keyspace: "ks", Name: "record", AllowExtraDatabaseFields: true})
			for _, bad := range []string{"000000040000000700", "00000004000000070000000401", "0000000400000007fffffffe", "0000000400000007ffffffff00"} {
				body, _ := hex.DecodeString(bad)
				if _, err := DecodeValue(decoder, containerValue(t, typ, body)); err == nil {
					t.Fatalf("malformed ignored suffix accepted: %s", bad)
				}
			}
		})
		t.Run("MissingMetadataDiffersFromNull", func(t *testing.T) {
			type record struct {
				ID   int32
				Body cqltype.Text
			}
			typ, _ := cqltype.UDTType("ks", "record", cqltype.ColumnSpec{Name: "id", Type: rowTestType(t, cqltype.TypeInt)}, cqltype.ColumnSpec{Name: "body", Type: rowTestType(t, cqltype.TypeText)})
			rows := Row(
				Column("id", Int32, func(v *record) *int32 { return &v.ID }),
				Column("body", TextCodec, func(v *record) *cqltype.Text { return &v.Body }),
			)
			short, _ := hex.DecodeString("0000000400000007")
			for _, options := range []UDTOptions{{Keyspace: "ks", Name: "record"}, {Keyspace: "ks", Name: "record", AllowMissingFields: true}, {Keyspace: "ks", Name: "record", AllowExtraDatabaseFields: true, AllowMissingFields: true}} {
				if _, err := DecodeValue(UDTDecoder(rows, options), containerValue(t, typ, short)); err == nil {
					t.Fatal("missing serialized field became zero through AllowMissingFields")
				}
			}
			type nullableRecord struct {
				ID   int32
				Body cqltype.Nullable[cqltype.Text]
			}
			nullableRows := Row(
				Column("id", Int32, func(v *nullableRecord) *int32 { return &v.ID }),
				Column("body", NullableDecoder(TextCodec), func(v *nullableRecord) *cqltype.Nullable[cqltype.Text] { return &v.Body }),
			)
			got, err := DecodeValue(UDTDecoder(nullableRows, UDTOptions{Keyspace: "ks", Name: "record"}), containerValue(t, typ, short))
			if err != nil || got.ID != 7 || got.Body.Valid {
				t.Fatalf("nullable suffix=%+v err=%v", got, err)
			}
		})
		t.Run("NamedEvolution", func(t *testing.T) {
			type record struct {
				ID      int32
				Body    cqltype.Text
				Missing int32
			}
			integer, text := rowTestType(t, cqltype.TypeInt), rowTestType(t, cqltype.TypeText)
			typ, _ := cqltype.UDTType("ks", "record", cqltype.ColumnSpec{Name: "body", Type: text}, cqltype.ColumnSpec{Name: "extra", Type: integer}, cqltype.ColumnSpec{Name: "id", Type: integer})
			params := Params(
				Param("id", Int32, func(v record) int32 { return v.ID }),
				Param("body", TextCodec, func(v record) cqltype.Text { return v.Body }),
			)
			rows := Row(
				Column("id", Int32, func(v *record) *int32 { return &v.ID }),
				Column("body", TextCodec, func(v *record) *cqltype.Text { return &v.Body }),
			)
			if err := UDTEncoder(params, UDTOptions{Keyspace: "ks", Name: "record"}).CheckType(typ); err == nil {
				t.Fatal("extra database field accepted without option")
			}
			if err := UDTDecoder(rows, UDTOptions{Keyspace: "ks", Name: "record"}).CheckType(typ); err == nil {
				t.Fatal("extra database field accepted without option")
			}
			options := UDTOptions{Keyspace: "ks", Name: "record", AllowExtraDatabaseFields: true}
			encoder, decoder := UDTEncoder(params, options), UDTDecoder(rows, options)
			encoded := containerGolden(t, typ, encoder, record{ID: 7, Body: CodecTestMust(cqltype.BorrowText("x"))}, "0000000178ffffffff0000000400000007")
			decoded, err := DecodeValue(decoder, encoded)
			if err != nil || decoded.ID != 7 || decoded.Body.String() != "x" {
				t.Fatalf("UDT=%+v err=%v", decoded, err)
			}
			missingRows := Row(
				Column("id", Int32, func(v *record) *int32 { return &v.ID }),
				Column("body", TextCodec, func(v *record) *cqltype.Text { return &v.Body }),
				Column("gone", Int32, func(v *record) *int32 { return &v.Missing }),
			)
			if err := UDTDecoder(missingRows, options).CheckType(typ); err == nil {
				t.Fatal("unknown application field accepted")
			}
			options.AllowMissingFields = true
			decoded, err = DecodeValue(UDTDecoder(missingRows, options), encoded)
			if err != nil || decoded.Missing != 0 || decoded.ID != 7 {
				t.Fatalf("missing application field=%+v err=%v", decoded, err)
			}
			missingParams := Params(
				Param("id", Int32, func(v record) int32 { return v.ID }),
				Param("body", TextCodec, func(v record) cqltype.Text { return v.Body }),
				Param("gone", Int32, func(v record) int32 { return v.Missing }),
			)
			containerGolden(t, typ, UDTEncoder(missingParams, options), record{ID: 7, Body: CodecTestMust(cqltype.BorrowText("x")), Missing: 42}, "0000000178ffffffff0000000400000007")
			// The compiled identity includes the keyspace and UDT name, not just fields.
			bound, err := compileValueDecoder(decoder, typ)
			if err != nil {
				t.Fatal(err)
			}
			other, _ := cqltype.UDTType("other", "record", cqltype.ColumnSlice(typ.Fields())...)
			if _, err := bound.Decode(NewValueReader(other, cqltype.Present, cqltype.BytesChain(encoded.body), nil)); err == nil {
				t.Fatal("compiled UDT accepted different identity")
			}
		})
	})
	t.Run("UDTEncoder", func(t *testing.T) {
		typ := CodecTestMust(cqltype.UDTType("ks", "record", cqltype.ColumnSpec{Name: "v", Type: FoundationScalar(t, cqltype.TypeInt)}))
		params := Params(Param("v", Int32, func(v int32) int32 { return v }))
		rows := Row(Column("v", Int32, func(v *int32) *int32 { return v }))
		for _, options := range []UDTOptions{{}, {Keyspace: "other", Name: "record"}, {Keyspace: "ks", Name: "wrong"}} {
			if _, err := EncodeValueBorrowed(typ, NullableEncoder(UDTEncoder(params, options)), cqltype.Nullable[int32]{}); err == nil {
				t.Fatal("invalid UDT identity hidden by null")
			}
			if _, err := compileValueDecoder(NullableDecoder(UDTDecoder(rows, options)), typ); err == nil {
				t.Fatal("invalid UDT identity hidden by nullable decoder")
			}
		}
	})
	t.Run("UDTOptions", func(t *testing.T) {
		t.Run("ForDerivesIdentity", func(t *testing.T) {
			typ, err := cqltype.UDTType("ks", "record", cqltype.ColumnSpec{Name: "id", Type: cqltype.IntType})
			if err != nil {
				t.Fatal(err)
			}
			options, err := UDTOptionsFor(typ)
			if err != nil || options != (UDTOptions{Keyspace: "ks", Name: "record"}) {
				t.Fatalf("UDTOptionsFor=%+v err=%v", options, err)
			}
			encoded := containerGolden(t, typ, UDTEncoder(Params(Param("id", Int32, func(v int32) int32 { return v })), options), int32(7), "0000000400000007")
			decoded, err := DecodeValue(UDTDecoder(OneColumn(Int32), options), encoded)
			if err != nil || decoded != 7 {
				t.Fatalf("decoded=%v err=%v", decoded, err)
			}
			if _, err := UDTOptionsFor(cqltype.IntType); err == nil {
				t.Fatal("UDTOptionsFor accepted a scalar type")
			}
		})
		t.Run("PropagateThroughComposition", func(t *testing.T) {
			integer := rowTestType(t, cqltype.TypeInt)
			typ, _ := cqltype.UDTType("ks", "record", cqltype.ColumnSpec{Name: "id", Type: integer}, cqltype.ColumnSpec{Name: "extra", Type: integer})
			options := UDTOptions{Keyspace: "ks", Name: "record", AllowExtraDatabaseFields: true}
			params := ConvertParams(Params(Param("id", Int32, func(v int32) int32 { return v })), func(v int64) int32 { return int32(v) })
			rows := ConvertRows(Row(Column("id", Int32, func(v *int32) *int32 { return v })), func(v int32) int64 { return int64(v) }, func(v int64) int64 { return v })
			encoded := containerGolden(t, typ, UDTEncoder(params, options), int64(7), "0000000400000007ffffffff")
			decoded, err := DecodeValue(UDTDecoder(rows, options), encoded)
			if err != nil || decoded != 7 {
				t.Fatalf("mapped UDT=%v err=%v", decoded, err)
			}
			positional, err := DecodeValue(UDTDecoder(OneColumn(Int32), options), encoded)
			if err != nil || positional != 7 {
				t.Fatalf("single positional UDT=%v err=%v", positional, err)
			}
		})
	})
}
