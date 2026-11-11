//go:build integration

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package integration

import (
	"fmt"
	"math/big"
	"net/netip"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	inf "gopkg.in/inf.v0"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/tests"
)

type person struct {
	FirstName, LastName string
	Age                 int32
}

func (p person) String() string {
	return fmt.Sprintf("Person{firstName: %s, lastName: %s, Age: %d}", p.FirstName, p.LastName, p.Age)
}
func serializationVectorGate(t *testing.T, allTypes bool) {
	t.Helper()
	if *flagDistribution == "cassandra" && flagCassVersion.Before(5, 0, 0) {
		t.Skip("Vector types have been introduced in Cassandra 5.0")
	}
	minor := 3
	if allTypes {
		minor = 4
	}
	if *flagDistribution == "scylla" && flagCassVersion.Before(2025, minor, 0) {
		t.Skipf("Vector types require ScyllaDB 2025.%d", minor)
	}
}

var serializationBlob = cql.NamedScalar(cql.BlobCodec, func(v []byte) cql.Bytes { return cql.BorrowBytes(v) }, func(v cql.Bytes) []byte { return v.OwnedBytes() })
var serializationTimestamp = cql.NamedScalar(cql.TimestampCodec, func(v time.Time) cql.Timestamp {
	n, e := cql.TimestampFromTime(v)
	if e != nil {
		panic(e)
	}
	return n
}, func(v cql.Timestamp) time.Time {
	n, e := v.Time()
	if e != nil {
		panic(e)
	}
	return n.UTC()
})
var serializationDate = cql.NamedScalar(cql.DateCodec, func(v time.Time) cql.Date {
	n, e := cql.DateFromTime(v)
	if e != nil {
		panic(e)
	}
	return n
}, func(v cql.Date) time.Time {
	n, e := v.Time()
	if e != nil {
		panic(e)
	}
	return n.UTC()
})
var serializationDecimal = cql.NamedScalar(cql.DecimalCodec, func(v inf.Dec) cql.Decimal {
	return cql.Decimal{Scale: int32(v.Scale()), Unscaled: cql.VarintFromBigInt(v.UnscaledBig())}
}, func(v cql.Decimal) inf.Dec { return *inf.NewDecBig(v.Unscaled.BigInt(), inf.Scale(v.Scale)) })

type serializationVectorCase struct {
	name, stmt string
	run        func(*testing.T, *gocql.Session[testDialect], string, string)
}

func vectorCase[T any](name, stmt string, codec cql.Codec[T], input []T, compare func(*testing.T, []T, []T)) serializationVectorCase {
	return serializationVectorCase{name, stmt, func(t *testing.T, s *gocql.Session[testDialect], table, column string) {
		got := serializationRoundTrip(t, s, table, column, 1, serializationVector(codec), input)
		if compare != nil {
			compare(t, input, got)
		} else if !reflect.DeepEqual(input, got) {
			t.Fatalf("expected %#v, got %#v", input, got)
		}
	}}
}

func TestVector(t *testing.T) {
	t.Parallel()

	t.Run("Bind", func(t *testing.T) {
		t.Parallel()

		t.Run("WrongDimension", func(t *testing.T) {
			t.Parallel()
			serializationVectorGate(t, false)
			s := createV2Session(t)
			defer s.Close()
			table := serializationTableName(t)
			serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,vec vector<float,3>)", table))
			for _, v := range [][]float32{{8, -5}, {8, -5, 1, 3}} {
				err := serializationExec(s, fmt.Sprintf("INSERT INTO %s(id,vec) VALUES(1,?)", table), cql.OneParam(cql.VectorEncoder(cql.Float32Codec)), v)
				require.Error(t, err, "expected vector with 3 dimensions, received %d", len(v))
			}
		})
	})

	t.Run("ParseType", func(t *testing.T) {
		t.Parallel()

		if *flagDistribution == "scylla" && flagCassVersion.Before(2025, 4, 0) {
			t.Skip("Vector types are useful in ScyllaDB from 2025.4 and on")
		}
		prefix := apacheCassandraTypePrefix
		vectorTypePrefix := prefix + "VectorType"
		testCases := []struct {
			name     string
			custom   string
			expected cqltype.TypeInfo
		}{
			{name: "text", custom: prefix + "UTF8Type", expected: cqltype.NewCustomType(0, cql.TypeVarchar, "")},
			{name: "set_int", custom: prefix + "SetType(" + prefix + "Int32Type)", expected: cqltype.CollectionType{NativeType: cqltype.NewCustomType(0, cql.TypeSet, ""), Key: nil, Elem: cqltype.NewCustomType(0, cql.TypeInt, "")}},
			{
				name:   "udt",
				custom: prefix + ("UserType(" + fixtureKeyspace("") + ",706572736f6e,66697273745f6e616d65:") + prefix + "UTF8Type,6c6173745f6e616d65:" + prefix + "UTF8Type,616765:" + prefix + "Int32Type)",
				expected: cqltype.UDTTypeInfo{
					NativeType: cqltype.NewCustomType(0, cql.TypeUDT, ""),
					KeySpace:   fixtureKeyspace(""),
					Name:       "person",
					Elements: []cqltype.UDTField{
						cqltype.UDTField{Name: "first_name", Type: cqltype.NewCustomType(0, cql.TypeVarchar, "")},
						cqltype.UDTField{Name: "last_name", Type: cqltype.NewCustomType(0, cql.TypeVarchar, "")},
						cqltype.UDTField{Name: "age", Type: cqltype.NewCustomType(0, cql.TypeInt, "")},
					},
				},
			},
			{
				name:   "tuple",
				custom: prefix + "TupleType(" + prefix + "UTF8Type," + prefix + "Int32Type," + prefix + "UTF8Type)",
				expected: cqltype.TupleTypeInfo{
					NativeType: cqltype.NewCustomType(0, cql.TypeTuple, ""),
					Elems:      []cqltype.TypeInfo{cqltype.NewCustomType(0, cql.TypeVarchar, ""), cqltype.NewCustomType(0, cql.TypeInt, ""), cqltype.NewCustomType(0, cql.TypeVarchar, "")},
				},
			},
			{
				name:   "vector_vector_inet",
				custom: prefix + "VectorType(" + prefix + "VectorType(" + prefix + "InetAddressType, 2), 3)",
				expected: cqltype.VectorType{
					NativeType: cqltype.NewCustomType(0, cql.TypeCustom, vectorTypePrefix),
					SubType: cqltype.VectorType{
						NativeType: cqltype.NewCustomType(0, cql.TypeCustom, vectorTypePrefix),
						SubType:    cqltype.NewCustomType(0, cql.TypeInet, ""),
						Dimensions: 2,
					},
					Dimensions: 3,
				},
			},
			{
				name:   "map_int_vector_text",
				custom: prefix + "MapType(" + prefix + "Int32Type," + prefix + "VectorType(" + prefix + "UTF8Type, 10))",
				expected: cqltype.CollectionType{
					NativeType: cqltype.NewCustomType(0, cql.TypeMap, ""),
					Key:        cqltype.NewCustomType(0, cql.TypeInt, ""),
					Elem: cqltype.VectorType{
						NativeType: cqltype.NewCustomType(0, cql.TypeCustom, vectorTypePrefix),
						SubType:    cqltype.NewCustomType(0, cql.TypeVarchar, ""),
						Dimensions: 10,
					},
				},
			},
			{
				name:   "set_map_vector_text_text",
				custom: prefix + "SetType(" + prefix + "MapType(" + prefix + "VectorType(" + prefix + "Int32Type, 10)," + prefix + "UTF8Type))",
				expected: cqltype.CollectionType{
					NativeType: cqltype.NewCustomType(0, cql.TypeSet, ""),
					Key:        nil,
					Elem: cqltype.CollectionType{
						NativeType: cqltype.NewCustomType(0, cql.TypeMap, ""),
						Key: cqltype.VectorType{
							NativeType: cqltype.NewCustomType(0, cql.TypeCustom, vectorTypePrefix),
							SubType:    cqltype.NewCustomType(0, cql.TypeInt, ""),
							Dimensions: 10,
						},
						Elem: cqltype.NewCustomType(0, cql.TypeVarchar, ""),
					},
				},
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				f := newFramer(nil, 0)
				f.WriteShort(0)
				f.WriteString(fmt.Sprintf("%sVectorType(%s, 2)", prefix, test.custom))
				parsedType := f.ReadTypeInfo()
				descriptor, err := dataTypeFromInfo(parsedType)
				require.NoError(t, err)
				dimension, ok := descriptor.Dimension()
				require.True(t, ok)
				require.Equal(t, uint16(2), dimension)
				element, ok := descriptor.Element()
				require.True(t, ok)
				expected, err := dataTypeFromInfo(test.expected)
				require.NoError(t, err)
				require.True(t, expected.Equal(element), "expected %s, got %s", expected, element)
			})
		}
	})

	t.Run("RoundTrip", func(t *testing.T) {
		t.Parallel()

		t.Run("Empty", func(t *testing.T) {
			t.Parallel()
			serializationVectorGate(t, false)
			s := createV2Session(t)
			defer s.Close()
			fixed, variable := serializationTableName(t, "fixed"), serializationTableName(t, "variable")
			serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,vec vector<float,3>)", fixed))
			serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,vec vector<text,4>)", variable))
			serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id) VALUES(1)", fixed))
			serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id) VALUES(1)", variable))
			fl, e := serializationOne(s, fmt.Sprintf("SELECT vec FROM %s WHERE id=1", fixed), cql.OneColumn(cql.NullableDecoder(serializationVector(cql.Float32Codec))))
			require.NoError(t, e)
			require.False(t, fl.Valid)
			require.Nil(t, fl.Value)
			tx, e := serializationOne(s, fmt.Sprintf("SELECT vec FROM %s WHERE id=1", variable), cql.OneColumn(cql.NullableDecoder(serializationVector(serializationString))))
			require.NoError(t, e)
			require.False(t, tx.Valid)
			require.Nil(t, tx.Value)
		})
		t.Run("Marshaler", func(t *testing.T) {
			t.Parallel()
			serializationVectorGate(t, false)
			s := createV2Session(t)
			defer s.Close()
			fixed, variable := serializationTableName(t, "fixed"), serializationTableName(t, "variable")
			serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,vec vector<float,3>)", fixed))
			serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,vec vector<text,4>)", variable))
			floats := []float32{8, 2.5, -5}
			require.Equal(t, floats, serializationRoundTrip(t, s, fixed, "vec", 1, serializationVector(cql.Float32Codec), floats))
			texts := []string{"apache", "cassandra", tests.RandomText(500), "gocql"}
			require.Equal(t, texts, serializationRoundTrip(t, s, variable, "vec", 1, serializationVector(serializationString), texts))
		})
		t.Run("Types", func(t *testing.T) {
			t.Parallel()
			serializationVectorGate(t, true)
			s := createV2Session(t)
			defer s.Close()
			parse := func(layout, value string) time.Time {
				v, e := time.Parse(layout, value)
				if e != nil {
					t.Fatal(e)
				}
				return v
			}
			timestamps := []time.Time{parse("2006-01-02", "2000-01-01"), parse("2006-01-02 15:04:05", "2024-01-01 10:31:45"), parse("2006-01-02 15:04:05.000", "2024-05-01 10:31:45.987")}
			dates := []time.Time{parse("2006-01-02", "2000-01-01"), parse("2006-01-02", "2022-03-14"), parse("2006-01-02", "2024-12-31")}
			times := []time.Time{parse("15:04:05", "01:00:00"), parse("15:04:05", "15:23:59"), parse("15:04:05.000", "10:31:45.987")}
			// The historical time subcase stores timestamp values anchored at year zero.
			// Preserve that wire coverage alongside the scalar time fixtures above.
			ascii := cql.NamedScalar(cql.ASCIICodec, func(s string) cql.Text { return codecTestMust(cql.BorrowText(s)) }, func(t cql.Text) string { return t.String() })
			uintVarint := cql.NamedScalar(cql.VarintCodec, func(n uint64) cql.Varint { return cql.VarintFromBigInt(new(big.Int).SetUint64(n)) }, func(n cql.Varint) uint64 { return n.BigInt().Uint64() })
			type triple struct {
				Text  string
				Int   int32
				Float float32
			}
			tripleCodec := serializationTuple(cql.Params(cql.ParamAt(0, serializationString, func(v triple) string { return v.Text }), cql.ParamAt(1, cql.Int32Codec, func(v triple) int32 { return v.Int }), cql.ParamAt(2, cql.Float32Codec, func(v triple) float32 { return v.Float })), cql.Row(cql.ColumnAt(0, serializationString, func(v *triple) *string { return &v.Text }), cql.ColumnAt(1, cql.Int32Codec, func(v *triple) *int32 { return &v.Int }), cql.ColumnAt(2, cql.Float32Codec, func(v *triple) *float32 { return &v.Float })))
			type textList struct {
				Text string
				List []string
			}
			textListCodec := serializationTuple(cql.Params(cql.ParamAt(0, serializationString, func(v textList) string { return v.Text }), cql.ParamAt(1, serializationList(serializationString), func(v textList) []string { return v.List })), cql.Row(cql.ColumnAt(0, serializationString, func(v *textList) *string { return &v.Text }), cql.ColumnAt(1, serializationList(serializationString), func(v *textList) *[]string { return &v.List })))
			cases := []serializationVectorCase{
				vectorCase("ascii", "ascii", ascii, []string{"a", "1", "Z"}, nil),
				vectorCase("bigint", "bigint", cql.Int64Codec, []int64{1, 2, 3}, nil),
				vectorCase("blob", "blob", serializationBlob, [][]byte{{1, 2, 3}, {4, 5, 6, 7}, {8, 9}}, nil),
				vectorCase("boolean", "boolean", cql.BoolCodec, []bool{true, false, true}, nil),
				vectorCase("counter", "counter", cql.CounterCodec, []cql.Counter{5, 6, 7}, nil),
				vectorCase("decimal", "decimal", serializationDecimal, []inf.Dec{*inf.NewDec(1, 0), *inf.NewDec(2, 1), *inf.NewDec(-3, 2)}, nil),
				vectorCase("double", "double", cql.Float64Codec, []float64{0.1, -1.2, 3}, nil),
				vectorCase("float", "float", cql.Float32Codec, []float32{0.1, -1.2, 3}, nil),
				vectorCase("int", "int", cql.Int32Codec, []int32{1, 2, 3}, nil),
				vectorCase("text", "text", serializationString, []string{"a", "b", "c"}, nil),
				vectorCase("timestamp", "timestamp", serializationTimestamp, timestamps, nil),
				vectorCase("uuid", "uuid", cql.UUIDCodec, []cql.UUID{cql.MustRandomUUID(), cql.MustRandomUUID(), cql.MustRandomUUID()}, nil),
				vectorCase("varchar", "varchar", serializationString, []string{"abc", "def", "ghi"}, nil),
				vectorCase("varint", "varint", uintVarint, []uint64{1234, 123498765, 18446744073709551615}, nil),
				vectorCase("timeuuid", "timeuuid", cql.TimeUUIDCodec, []cql.TimeUUID{cql.TimeUUID(cql.NewTimeUUID()), cql.TimeUUID(cql.NewTimeUUID()), cql.TimeUUID(cql.NewTimeUUID())}, nil),
				vectorCase("inet", "inet", cql.InetCodec, []netip.Addr{netip.MustParseAddr("127.0.0.1"), netip.MustParseAddr("192.168.1.1"), netip.MustParseAddr("8.8.8.8")}, nil),
				vectorCase("date", "date", serializationDate, dates, nil),
				vectorCase("time", "timestamp", serializationTimestamp, times, nil),
				vectorCase("smallint", "smallint", cql.Int16Codec, []int16{127, 256, -1234}, nil),
				vectorCase("tinyint", "tinyint", cql.Int8Codec, []int8{127, 9, -123}, nil),
				vectorCase("duration", "duration", cql.DurationCodec, []cql.Duration{{Months: 0, Days: 1, Nanoseconds: 1920000000000}, {Months: 1, Days: 1, Nanoseconds: 1920000000000}, {Months: 31, Days: 0, Nanoseconds: 60000000000}}, nil),
				vectorCase("vector_vector_float", "vector<float,5>", serializationVector(cql.Float32Codec), [][]float32{{0.1, -1.2, 3, 5, 5}, {10.1, -122222.0002, 35, 1, 1}, {0, 0, 0, 0, 0}}, nil),
				vectorCase("vector_vector_set_float", "vector<set<float>,5>", serializationVector(serializationSet(cql.Float32Codec)), [][][]float32{{{1, 2}, {2, -1}, {3}, {0}, {-1.3}}, {{2, 3}, {2, -1}, {3}, {0}, {-1.3}}, {{1, 1000}, {0}, {}, {12, 14, 15, 16}, {-1.3}}}, func(t *testing.T, want, got [][][]float32) {
					require.Len(t, got, len(want))
					for i := range want {
						require.Len(t, got[i], len(want[i]))
						for j := range want[i] {
							a, b := append([]float32(nil), want[i][j]...), append([]float32(nil), got[i][j]...)
							sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
							sort.Slice(b, func(i, j int) bool { return b[i] < b[j] })
							require.Equal(t, a, b)
						}
					}
				}),
				vectorCase("vector_tuple_text_int_float", "tuple<text,int,float>", tripleCodec, []triple{{"a", 1, 0.5}, {"b", 2, -1.2}, {"c", 3, 0}}, nil),
				vectorCase("vector_tuple_text_list_text", "tuple<text,list<text>>", textListCodec, []textList{{"a", []string{"b", "c"}}, {"d", []string{"e", "g", "f"}}, {"h", []string{"i"}}}, nil),
				vectorCase("vector_set_text", "set<text>", serializationSet(serializationString), [][]string{{"a", "b"}, {"c", "d"}, {"f", "e"}}, func(t *testing.T, want, got [][]string) {
					require.Len(t, got, len(want))
					for i := range want {
						a, b := append([]string(nil), want[i]...), append([]string(nil), got[i]...)
						sort.Strings(a)
						sort.Strings(b)
						require.Equal(t, a, b)
					}
				}),
				vectorCase("vector_list_int", "list<int>", serializationList(cql.Int32Codec), [][]int32{{1, 2, 3}, {-1, -2, -3}, {0, 0, 0}}, nil),
				vectorCase("vector_map_text_int", "map<text,int>", serializationMap(serializationString, cql.Int32Codec), []map[string]int32{{"a": 1, "b": 2, "c": 3}, {"abc": 123}, {}}, nil),
			}
			table := serializationTableName(t, "all_types")
			cols := ""
			for _, tc := range cases {
				cols += fmt.Sprintf(",vec_%s vector<%s,3>", tc.name, tc.stmt)
			}
			serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY%s)", table, cols))
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) { tc.run(t, s, table, "vec_"+tc.name) })
			}
		})
		t.Run("UDT", func(t *testing.T) {
			t.Parallel()
			serializationVectorGate(t, false)
			s := createV2Session(t)
			defer s.Close()
			table, typ := serializationTableName(t), serializationTypeName(t)
			serializationDDL(t, s, fmt.Sprintf("CREATE TYPE %s(first_name text,last_name text,age int)", typ))
			serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,couple vector<%s,2>)", table, typ))
			codec := serializationUDT(cql.Params(cql.Param("first_name", serializationString, func(v person) string { return v.FirstName }), cql.Param("last_name", serializationString, func(v person) string { return v.LastName }), cql.Param("age", cql.Int32Codec, func(v person) int32 { return v.Age })), cql.Row(cql.Column("first_name", serializationString, func(v *person) *string { return &v.FirstName }), cql.Column("last_name", serializationString, func(v *person) *string { return &v.LastName }), cql.Column("age", cql.Int32Codec, func(v *person) *int32 { return &v.Age })), serializationUDTOptions(typ))
			want := []person{{"Johny", "Bravo", 25}, {"Capitan", "Planet", 5}}
			require.Equal(t, want, serializationRoundTrip(t, s, table, "couple", 1, serializationVector(codec), want))
		})
	})
}
