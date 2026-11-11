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
	"context"
	"fmt"
	"math/big"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cql/dynamic"
)

func TestScan(t *testing.T) {
	t.Parallel()

	t.Run("Dynamic", func(t *testing.T) {
		t.Parallel()

		t.Run("Collections", func(t *testing.T) {
			t.Parallel()
			s := createV2Session(t)
			defer s.Close()
			table := serializationTableName(t)
			serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,list_col list<text>,set_col set<int>,map_col map<text,int>)", table))
			if err := truncateTable(t, s, table); err != nil {
				t.Fatal(err)
			}
			tx, in := cql.TextType, cql.IntType
			list, set, mp := serializationMustType(cql.ListType(tx)), serializationMustType(cql.SetType(in)), serializationMustType(cql.MapType(tx, in))
			checkDynamicSerializationCases(t, s, table, []serializationDynamicCase{
				{"list", "['a','b','c']", serializationMustValue(dynamic.List(list, []dynamic.Value{codecTestMust(dynamic.Text("a")), codecTestMust(dynamic.Text("b")), codecTestMust(dynamic.Text("c"))}))},
				{"set", "{1,2,3}", serializationMustValue(dynamic.Set(set, []dynamic.Value{dynamic.Int(1), dynamic.Int(2), dynamic.Int(3)}))},
				{"map", "{'key1':1,'key2':2}", serializationMustValue(dynamic.Map(mp, []dynamic.Entry{{Key: codecTestMust(dynamic.Text("key1")), Value: dynamic.Int(1)}, {Key: codecTestMust(dynamic.Text("key2")), Value: dynamic.Int(2)}}))},
			})
		})
		t.Run("Counter", func(t *testing.T) {
			t.Parallel()
			s := createV2SessionFromConfig(createV2Cluster(), t, true)
			defer s.Close()
			table := serializationTableName(t)
			serializationDDL(t, s, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(id int PRIMARY KEY,counter_col counter)", table))
			if err := truncateTable(t, s, table); err != nil {
				t.Fatal(err)
			}
			serializationWrite(t, s, fmt.Sprintf("UPDATE %s SET counter_col=counter_col+42 WHERE id=?", table), dynamic.Int(1))
			for _, method := range []string{"SliceMap", "MapScan"} {
				t.Run(method, func(t *testing.T) {
					got := dynamicSerializationResult(t, s, fmt.Sprintf("SELECT counter_col FROM %s WHERE id=?", table), method, dynamic.Int(1))
					assertSerializationValue(t, dynamic.Counter(42), got)
				})
			}
		})
		t.Run("NestedCollections", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id text PRIMARY KEY,val list<frozen<map<text,text>>>)", table))
			type nested = []cql.Entry[string, string]
			want := []nested{{{Key: "key1", Value: "val1"}}, {{Key: "key2", Value: "val2"}}}
			prepared := cassandraPrepared(t, session, cql.Statement[[]nested, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(id,val) VALUES('key',?)", table), Params: cql.OneParam(cql.ListEncoder(cql.MapEncoder(cql.StringCodec, cql.StringCodec))), Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
			_, err := prepared.Exec(context.Background(), want)
			require.NoError(t, err)
			row := cassandraUnpreparedOne(t, session, fmt.Sprintf("SELECT val FROM %s WHERE id='key'", table), cql.OneColumn(cql.ListDecoder(cql.MapDecoder(cql.TextCodec, cql.TextCodec))), gocql.Options{})
			var got []nested
			for _, mapping := range row {
				var entries nested
				for _, entry := range mapping {
					entries = append(entries, cql.Entry[string, string]{Key: entry.Key.String(), Value: entry.Value.String()})
				}
				got = append(got, entries)
			}
			require.Equal(t, want, got)
		})
		t.Run("Scalars", func(t *testing.T) {
			t.Parallel()
			s := createV2Session(t)
			defer s.Close()
			table := serializationTableName(t)
			date, err := cql.DateFromTime(time.Date(2015, 5, 3, 0, 0, 0, 0, time.UTC))
			if err != nil {
				t.Fatal(err)
			}
			cases := []serializationDynamicCase{
				{"tinyint", "42", dynamic.TinyInt(42)}, {"smallint", "1234", dynamic.SmallInt(1234)}, {"int", "123456", dynamic.Int(123456)}, {"bigint", "1234567890", dynamic.BigInt(1234567890)},
				{"float", "3.14", dynamic.Float(3.14)}, {"double", "2.718281828", dynamic.Double(2.718281828)}, {"boolean", "true", dynamic.Bool(true)},
				{"text", "'hello world'", codecTestMust(dynamic.Text("hello world"))}, {"ascii", "'hello ascii'", serializationMustValue(dynamic.ASCII("hello ascii"))}, {"varchar", "'hello varchar'", codecTestMust(dynamic.Text("hello varchar"))},
				{"timestamp", "1388534400000", dynamic.Timestamp(1388534400000)}, {"uuid", "550e8400-e29b-41d4-a716-446655440000", dynamic.UUID(mustParseUUID("550e8400-e29b-41d4-a716-446655440000"))},
				{"timeuuid", "60d79c23-5793-11f0-8afe-bcfce78b517a", serializationMustValue(dynamic.TimeUUID(cql.TimeUUID(mustParseUUID("60d79c23-5793-11f0-8afe-bcfce78b517a"))))},
				{"inet", "'127.0.0.1'", serializationMustValue(dynamic.Inet(netip.MustParseAddr("127.0.0.1")))}, {"blob", "0x48656c6c6f", dynamic.Blob([]byte("Hello"))},
				{"varint", "123456789012345678901234567890", dynamic.Varint(cql.VarintFromBigInt(mustParseBigInt("123456789012345678901234567890")))},
				{"decimal", "123.45", dynamic.Decimal(cql.Decimal{Scale: 2, Unscaled: cql.VarintFromBigInt(big.NewInt(12345))})}, {"date", "'2015-05-03'", dynamic.Date(date)},
				{"time", "'13:30:54.234'", serializationMustValue(dynamic.Time(cql.TimeOfDay(13*time.Hour + 30*time.Minute + 54*time.Second + 234*time.Millisecond)))},
				{"duration", "1y2mo3d4h5m6s789ms", serializationMustValue(dynamic.Duration(mustCreateDuration(14, 3, 4*time.Hour+5*time.Minute+6*time.Second+789*time.Millisecond)))},
			}
			cols := ""
			for _, tc := range cases {
				cols += fmt.Sprintf(",%s_col %s", tc.name, tc.name)
			}
			serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY%s)", table, cols))
			if err := truncateTable(t, s, table); err != nil {
				t.Fatal(err)
			}
			checkDynamicSerializationCases(t, s, table, cases)
		})
		t.Run("Tuple", func(t *testing.T) {
			t.Parallel()
			s := createV2Session(t)
			defer s.Close()
			table := serializationTableName(t)
			serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,tuple_col tuple<int,text>)", table))
			if err := truncateTable(t, s, table); err != nil {
				t.Fatal(err)
			}
			typ := serializationMustType(cql.TupleType(cql.IntType, cql.TextType))
			checkDynamicSerializationCases(t, s, table, []serializationDynamicCase{{"tuple", "(42,'hello')", serializationMustValue(dynamic.Tuple(typ, []dynamic.Value{dynamic.Int(42), codecTestMust(dynamic.Text("hello"))}))}})
		})
		t.Run("Vector", func(t *testing.T) {
			t.Parallel()
			serializationVectorGate(t, false)
			s := createV2Session(t)
			defer s.Close()
			table := serializationTableName(t)
			serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,vector_float_col vector<float,3>,vector_text_col vector<text,2>)", table))
			if err := truncateTable(t, s, table); err != nil {
				t.Fatal(err)
			}
			fl := serializationMustType(cql.VectorType(cql.FloatType, 3))
			tx := serializationMustType(cql.VectorType(cql.TextType, 2))
			checkDynamicSerializationCases(t, s, table, []serializationDynamicCase{{"vector_float", "[1.0,2.5,-3.0]", serializationMustValue(dynamic.Vector(fl, []dynamic.Value{dynamic.Float(1), dynamic.Float(2.5), dynamic.Float(-3)}))}, {"vector_text", "['hello','world']", serializationMustValue(dynamic.Vector(tx, []dynamic.Value{codecTestMust(dynamic.Text("hello")), codecTestMust(dynamic.Text("world"))}))}})
		})
	})

	t.Run("MapScan", func(t *testing.T) {
		t.Parallel()

		t.Run("RefMap", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(testtext text PRIMARY KEY,testfullname text,testint int)", table))
			type input struct {
				Text   string
				Name   FullName
				Number int32
			}
			params := cql.Params(cql.Param("testtext", cql.StringCodec, func(v input) string { return v.Text }), cql.Param("testfullname", fullNameCodec{}, func(v input) FullName { return v.Name }), cql.Param("testint", cql.Int32Codec, func(v input) int32 { return v.Number }))
			prepared := cassandraPrepared(t, session, cql.Statement[input, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(testtext,testfullname,testint) VALUES(?,?,?)", table), Params: params, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
			_, err := prepared.Exec(context.Background(), input{"testtext", FullName{"John", "Doe"}, 100})
			require.NoError(t, err)
			type output struct {
				Text   cql.Text
				Name   FullName
				Number int32
			}
			decoder := cql.Row(cql.Column("testtext", cql.TextCodec, func(v *output) *cql.Text { return &v.Text }), cql.Column("testfullname", fullNameCodec{}, func(v *output) *FullName { return &v.Name }), cql.Column("testint", cql.Int32Codec, func(v *output) *int32 { return &v.Number }))
			got := cassandraUnpreparedOne(t, session, "SELECT * FROM "+table, decoder, gocql.Options{})
			require.Equal(t, "testtext", got.Text.String())
			require.Equal(t, FullName{"John", "Doe"}, got.Name)
			require.Equal(t, int32(100), got.Number)
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(testtext,testint) VALUES(?,?)", table), codecTestMust(dynamic.Text("null-int")), dynamic.Null(dynamic.Int(0).Type()))
			nullable := cassandraPrepared(t, session, cql.Statement[string, cql.Nullable[int32]]{CQL: fmt.Sprintf("SELECT testint FROM %s WHERE testtext=?", table), Params: cql.OneParam(cql.StringCodec), Rows: cql.OneColumn(cql.NullableCodec(cql.Int32Codec))}, gocql.PrepareOptions{})
			require.False(t, cassandraReadOne(t, session, nullable, "null-int", gocql.Options{}).Valid)
		})
		t.Run("Rows", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(fullname text PRIMARY KEY,age int,address inet,data blob)", table))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(fullname,age,address) VALUES(?,?,?)", table), codecTestMust(dynamic.Text("Grace Hopper")), dynamic.Int(31), cassandraValue(dynamic.Inet(netip.MustParseAddr("10.0.0.1"))))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(fullname,age,address,data) VALUES(?,?,?,?)", table), codecTestMust(dynamic.Text("Ada Lovelace")), dynamic.Int(30), cassandraValue(dynamic.Inet(netip.MustParseAddr("10.0.0.2"))), dynamic.Blob([]byte(`{"foo": "bar"}`)))
			rows := cassandraUnpreparedAll(t, session, "SELECT * FROM "+table, dynamic.Rows, gocql.Options{})
			require.Len(t, rows, 2)
			for i, want := range []struct {
				name    string
				age     int64
				address string
			}{{"Ada Lovelace", 30, "10.0.0.2"}, {"Grace Hopper", 31, "10.0.0.1"}} {
				row := rows[i]
				require.Equal(t, want.name, cassandraText(t, row, "fullname"))
				require.Equal(t, want.age, cassandraInt(t, row, "age"))
				ip, err := cassandraField(t, row, "address").Inet()
				require.NoError(t, err)
				require.Equal(t, want.address, ip.String())
				data := cassandraField(t, row, "data")
				if i == 0 {
					b, e := data.Bytes()
					require.NoError(t, e)
					require.Equal(t, []byte(`{"foo": "bar"}`), b.OwnedBytes())
				} else {
					require.Equal(t, cql.Null, data.State())
				}
			}
		})
	})

	t.Run("Row", func(t *testing.T) {
		t.Parallel()

		// Don't initialize time.Time bind variable if cassandra timestamp column is empty
		t.Run("EmptyTimestamp", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,time timestamp,num int)", table))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,num) VALUES(?,?)", table), dynamic.Int(1), dynamic.Int(561))
			read := cassandraPrepared(t, session, cql.Statement[int32, cql.Nullable[cql.Timestamp]]{CQL: fmt.Sprintf("SELECT time FROM %s WHERE id=?", table), Params: cql.OneParam(cql.Int32Codec), Rows: cql.OneColumn(cql.NullableCodec(cql.TimestampCodec))}, gocql.PrepareOptions{})
			value := cassandraReadOne(t, session, read, int32(1), gocql.Options{})
			require.False(t, value.Valid)
			require.Zero(t, value.Value)
		})
		t.Run("IgnoredColumn", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(foo varchar,bar int,PRIMARY KEY(foo,bar))", table))
			for i := int32(1); i <= 20; i++ {
				cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(foo,bar) VALUES(?,?)", table), codecTestMust(dynamic.Text("squares")), dynamic.Int(i*i))
			}
			type result struct{ N int32 }
			decoder := cql.Row(cql.IgnoreColumn[result]("foo"), cql.Column("bar", cql.Int32Codec, func(r *result) *int32 { return &r.N }))
			prepared := cassandraPrepared(t, session, cql.Statement[string, result]{CQL: fmt.Sprintf("SELECT * FROM %s WHERE foo=?", table), Params: cql.OneParam(cql.StringCodec), Rows: decoder}, gocql.PrepareOptions{})
			rows, err := prepared.Run(context.Background(), "squares")
			require.NoError(t, err)
			defer rows.Close()
			var sum int32
			var row result
			for rows.NextInto(&row) {
				sum += row.N
			}
			require.NoError(t, rows.Close())
			require.Equal(t, int32(2870), sum)
		})
	})

	t.Run("SliceMap", func(t *testing.T) {
		t.Parallel()
		session := createV2Session(t)
		defer session.Close()
		table := testTableName(t)
		cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(testuuid timeuuid PRIMARY KEY,testtimestamp timestamp,testvarchar varchar,testbigint bigint,testblob blob,testbool boolean,testfloat float,testdouble double,testint int,testdecimal decimal,testlist list<text>,testset set<int>,testmap map<varchar,varchar>,testvarint varint,testinet inet)", table))
		textType := cql.VarcharType
		intType := cql.IntType
		listType, err := cql.ListType(textType)
		require.NoError(t, err)
		setType, err := cql.SetType(intType)
		require.NoError(t, err)
		varcharType := cql.VarcharType
		mapType, err := cql.MapType(varcharType, varcharType)
		require.NoError(t, err)
		bigInt, ok := new(big.Int).SetString("830169365738487321165427203929228", 10)
		require.True(t, ok)
		expected := []dynamic.Field{{Name: "testuuid", Value: cassandraValue(dynamic.TimeUUID(cql.TimeUUID(cql.NewTimeUUID())))}, {Name: "testtimestamp", Value: dynamic.Timestamp(cql.Timestamp(time.Now().UnixMilli()))}, {Name: "testvarchar", Value: codecTestMust(dynamic.Text("Test VarChar"))}, {Name: "testbigint", Value: dynamic.BigInt(time.Now().Unix())}, {Name: "testblob", Value: dynamic.Blob([]byte("test blob"))}, {Name: "testbool", Value: dynamic.Bool(true)}, {Name: "testfloat", Value: dynamic.Float(4.564)}, {Name: "testdouble", Value: dynamic.Double(4.815162342)}, {Name: "testint", Value: dynamic.Int(2343)}, {Name: "testdecimal", Value: dynamic.Decimal(cql.Decimal{Unscaled: cql.VarintFromBigInt(big.NewInt(100))})}, {Name: "testlist", Value: cassandraValue(dynamic.List(listType, []dynamic.Value{codecTestMust(dynamic.Text("quux")), codecTestMust(dynamic.Text("foo")), codecTestMust(dynamic.Text("bar")), codecTestMust(dynamic.Text("baz")), codecTestMust(dynamic.Text("quux"))}))}, {Name: "testset", Value: cassandraValue(dynamic.Set(setType, []dynamic.Value{dynamic.Int(1), dynamic.Int(2), dynamic.Int(3), dynamic.Int(4), dynamic.Int(5), dynamic.Int(6), dynamic.Int(7), dynamic.Int(8), dynamic.Int(9)}))}, {Name: "testmap", Value: cassandraValue(dynamic.Map(mapType, []dynamic.Entry{{Key: codecTestMust(dynamic.Text("field1")), Value: codecTestMust(dynamic.Text("val1"))}, {Key: codecTestMust(dynamic.Text("field2")), Value: codecTestMust(dynamic.Text("val2"))}, {Key: codecTestMust(dynamic.Text("field3")), Value: codecTestMust(dynamic.Text("val3"))}}))}, {Name: "testvarint", Value: dynamic.Varint(cql.VarintFromBigInt(bigInt))}, {Name: "testinet", Value: cassandraValue(dynamic.Inet(netip.MustParseAddr("213.212.2.19")))}}
		var values []dynamic.Value
		for _, field := range expected {
			values = append(values, field.Value)
		}
		cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(testuuid,testtimestamp,testvarchar,testbigint,testblob,testbool,testfloat,testdouble,testint,testdecimal,testlist,testset,testmap,testvarint,testinet) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", table), values...)
		check := func(row dynamic.Row) {
			t.Helper()
			require.Equal(t, len(expected), row.Columns().Len())
			for _, field := range expected {
				actual := cassandraField(t, row, field.Name)
				want, err := cql.EncodeValue(actual.Type(), dynamic.Codec, field.Value)
				require.NoError(t, err, field.Name)
				got, err := cql.EncodeValue(actual.Type(), dynamic.Codec, actual)
				require.NoError(t, err, field.Name)
				require.Equal(t, want.State(), got.State(), field.Name)
				require.Equal(t, want.Bytes().OwnedBytes(), got.Bytes().OwnedBytes(), field.Name)
			}
		}
		// Preserve the three result-consumption forms as cloned slices, a cursor,
		// and an exactly-one-row read, all with explicit dynamic values. Only a
		// prepared statement returns a cursor.
		rows := cassandraUnpreparedAll(t, session, "SELECT * FROM "+table, dynamic.Rows, gocql.Options{})
		require.Len(t, rows, 1)
		check(rows[0])
		selectAll := cassandraDynamicPrepared(t, session, "SELECT * FROM "+table, gocql.PrepareOptions{})
		cursor, err := selectAll.Run(context.Background(), nil)
		require.NoError(t, err)
		defer cursor.Close()
		var row dynamic.Row
		require.True(t, cursor.NextInto(&row))
		check(row)
		require.NoError(t, cursor.Close())
		check(cassandraRead(t, session, "SELECT * FROM "+table))
	})
}
