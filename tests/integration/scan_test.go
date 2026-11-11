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
				{"list", "['a','b','c']", serializationMustValue(cql.ListValue(list, []cql.Value{codecTestMust(cql.TextValue("a")), codecTestMust(cql.TextValue("b")), codecTestMust(cql.TextValue("c"))}))},
				{"set", "{1,2,3}", serializationMustValue(cql.SetValue(set, []cql.Value{cql.IntValue(1), cql.IntValue(2), cql.IntValue(3)}))},
				{"map", "{'key1':1,'key2':2}", serializationMustValue(cql.MapValue(mp, []cql.ValueEntry{{Key: codecTestMust(cql.TextValue("key1")), Value: cql.IntValue(1)}, {Key: codecTestMust(cql.TextValue("key2")), Value: cql.IntValue(2)}}))},
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
			serializationWrite(t, s, fmt.Sprintf("UPDATE %s SET counter_col=counter_col+42 WHERE id=?", table), cql.IntValue(1))
			for _, method := range []string{"SliceMap", "MapScan"} {
				t.Run(method, func(t *testing.T) {
					got := dynamicSerializationResult(t, s, fmt.Sprintf("SELECT counter_col FROM %s WHERE id=?", table), method, cql.IntValue(1))
					assertSerializationValue(t, cql.CounterValue(42), got)
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
			_, err := Exec(context.Background(), session, cassandraBound(t, prepared, want), gocql.CompiledOptions{})
			require.NoError(t, err)
			row := cassandraReadBound(t, session, cassandraUnprepared(t, fmt.Sprintf("SELECT val FROM %s WHERE id='key'", table), cql.OneColumn(cql.ListDecoder(cql.MapDecoder(cql.TextCodec, cql.TextCodec)))), gocql.Options{})
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
				{"tinyint", "42", cql.TinyIntValue(42)}, {"smallint", "1234", cql.SmallIntValue(1234)}, {"int", "123456", cql.IntValue(123456)}, {"bigint", "1234567890", cql.BigIntValue(1234567890)},
				{"float", "3.14", cql.FloatValue(3.14)}, {"double", "2.718281828", cql.DoubleValue(2.718281828)}, {"boolean", "true", cql.BoolValue(true)},
				{"text", "'hello world'", codecTestMust(cql.TextValue("hello world"))}, {"ascii", "'hello ascii'", serializationMustValue(cql.ASCIIValue("hello ascii"))}, {"varchar", "'hello varchar'", codecTestMust(cql.TextValue("hello varchar"))},
				{"timestamp", "1388534400000", cql.TimestampValue(1388534400000)}, {"uuid", "550e8400-e29b-41d4-a716-446655440000", cql.UUIDValue(mustParseUUID("550e8400-e29b-41d4-a716-446655440000"))},
				{"timeuuid", "60d79c23-5793-11f0-8afe-bcfce78b517a", serializationMustValue(cql.TimeUUIDValue(cql.TimeUUID(mustParseUUID("60d79c23-5793-11f0-8afe-bcfce78b517a"))))},
				{"inet", "'127.0.0.1'", serializationMustValue(cql.InetValue(netip.MustParseAddr("127.0.0.1")))}, {"blob", "0x48656c6c6f", cql.BlobValue([]byte("Hello"))},
				{"varint", "123456789012345678901234567890", cql.VarintValue(cql.VarintFromBigInt(mustParseBigInt("123456789012345678901234567890")))},
				{"decimal", "123.45", cql.DecimalValue(cql.Decimal{Scale: 2, Unscaled: cql.VarintFromBigInt(big.NewInt(12345))})}, {"date", "'2015-05-03'", cql.DateValue(date)},
				{"time", "'13:30:54.234'", serializationMustValue(cql.TimeValue(cql.TimeOfDay(13*time.Hour + 30*time.Minute + 54*time.Second + 234*time.Millisecond)))},
				{"duration", "1y2mo3d4h5m6s789ms", serializationMustValue(cql.DurationValue(mustCreateDuration(14, 3, 4*time.Hour+5*time.Minute+6*time.Second+789*time.Millisecond)))},
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
			checkDynamicSerializationCases(t, s, table, []serializationDynamicCase{{"tuple", "(42,'hello')", serializationMustValue(cql.TupleValue(typ, []cql.Value{cql.IntValue(42), codecTestMust(cql.TextValue("hello"))}))}})
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
			checkDynamicSerializationCases(t, s, table, []serializationDynamicCase{{"vector_float", "[1.0,2.5,-3.0]", serializationMustValue(cql.VectorValue(fl, []cql.Value{cql.FloatValue(1), cql.FloatValue(2.5), cql.FloatValue(-3)}))}, {"vector_text", "['hello','world']", serializationMustValue(cql.VectorValue(tx, []cql.Value{codecTestMust(cql.TextValue("hello")), codecTestMust(cql.TextValue("world"))}))}})
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
			_, err := Exec(context.Background(), session, cassandraBound(t, prepared, input{"testtext", FullName{"John", "Doe"}, 100}), gocql.CompiledOptions{})
			require.NoError(t, err)
			type output struct {
				Text   cql.Text
				Name   FullName
				Number int32
			}
			decoder := cql.Row(cql.Column("testtext", cql.TextCodec, func(v *output) *cql.Text { return &v.Text }), cql.Column("testfullname", fullNameCodec{}, func(v *output) *FullName { return &v.Name }), cql.Column("testint", cql.Int32Codec, func(v *output) *int32 { return &v.Number }))
			got := cassandraReadBound(t, session, cassandraUnprepared(t, "SELECT * FROM "+table, decoder), gocql.Options{})
			require.Equal(t, "testtext", got.Text.String())
			require.Equal(t, FullName{"John", "Doe"}, got.Name)
			require.Equal(t, int32(100), got.Number)
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(testtext,testint) VALUES(?,?)", table), codecTestMust(cql.TextValue("null-int")), cql.NullValue(cql.IntValue(0).Type()))
			nullable := cassandraPrepared(t, session, cql.Statement[string, cql.Nullable[int32]]{CQL: fmt.Sprintf("SELECT testint FROM %s WHERE testtext=?", table), Params: cql.OneParam(cql.StringCodec), Rows: cql.OneColumn(cql.NullableCodec(cql.Int32Codec))}, gocql.PrepareOptions{})
			require.False(t, cassandraReadBound(t, session, cassandraBound(t, nullable, "null-int"), gocql.Options{}).Valid)
		})
		t.Run("Rows", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(fullname text PRIMARY KEY,age int,address inet,data blob)", table))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(fullname,age,address) VALUES(?,?,?)", table), codecTestMust(cql.TextValue("Grace Hopper")), cql.IntValue(31), cassandraValue(cql.InetValue(netip.MustParseAddr("10.0.0.1"))))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(fullname,age,address,data) VALUES(?,?,?,?)", table), codecTestMust(cql.TextValue("Ada Lovelace")), cql.IntValue(30), cassandraValue(cql.InetValue(netip.MustParseAddr("10.0.0.2"))), cql.BlobValue([]byte(`{"foo": "bar"}`)))
			rows := cassandraRows(t, session, cassandraUnprepared(t, "SELECT * FROM "+table, cql.DynamicRows), gocql.Options{})
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
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,num) VALUES(?,?)", table), cql.IntValue(1), cql.IntValue(561))
			read := cassandraPrepared(t, session, cql.Statement[int32, cql.Nullable[cql.Timestamp]]{CQL: fmt.Sprintf("SELECT time FROM %s WHERE id=?", table), Params: cql.OneParam(cql.Int32Codec), Rows: cql.OneColumn(cql.NullableCodec(cql.TimestampCodec))}, gocql.PrepareOptions{})
			value := cassandraReadBound(t, session, cassandraBound(t, read, int32(1)), gocql.Options{})
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
				cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(foo,bar) VALUES(?,?)", table), codecTestMust(cql.TextValue("squares")), cql.IntValue(i*i))
			}
			type result struct{ N int32 }
			decoder := cql.Row(cql.IgnoreColumn[result]("foo"), cql.Column("bar", cql.Int32Codec, func(r *result) *int32 { return &r.N }))
			prepared := cassandraPrepared(t, session, cql.Statement[string, result]{CQL: fmt.Sprintf("SELECT * FROM %s WHERE foo=?", table), Params: cql.OneParam(cql.StringCodec), Rows: decoder}, gocql.PrepareOptions{})
			rows, err := Run(context.Background(), session, cassandraBound(t, prepared, "squares"), gocql.CompiledOptions{})
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
		expected := []cql.ValueField{{Name: "testuuid", Value: cassandraValue(cql.TimeUUIDValue(cql.TimeUUID(cql.NewTimeUUID())))}, {Name: "testtimestamp", Value: cql.TimestampValue(cql.Timestamp(time.Now().UnixMilli()))}, {Name: "testvarchar", Value: codecTestMust(cql.TextValue("Test VarChar"))}, {Name: "testbigint", Value: cql.BigIntValue(time.Now().Unix())}, {Name: "testblob", Value: cql.BlobValue([]byte("test blob"))}, {Name: "testbool", Value: cql.BoolValue(true)}, {Name: "testfloat", Value: cql.FloatValue(4.564)}, {Name: "testdouble", Value: cql.DoubleValue(4.815162342)}, {Name: "testint", Value: cql.IntValue(2343)}, {Name: "testdecimal", Value: cql.DecimalValue(cql.Decimal{Unscaled: cql.VarintFromBigInt(big.NewInt(100))})}, {Name: "testlist", Value: cassandraValue(cql.ListValue(listType, []cql.Value{codecTestMust(cql.TextValue("quux")), codecTestMust(cql.TextValue("foo")), codecTestMust(cql.TextValue("bar")), codecTestMust(cql.TextValue("baz")), codecTestMust(cql.TextValue("quux"))}))}, {Name: "testset", Value: cassandraValue(cql.SetValue(setType, []cql.Value{cql.IntValue(1), cql.IntValue(2), cql.IntValue(3), cql.IntValue(4), cql.IntValue(5), cql.IntValue(6), cql.IntValue(7), cql.IntValue(8), cql.IntValue(9)}))}, {Name: "testmap", Value: cassandraValue(cql.MapValue(mapType, []cql.ValueEntry{{Key: codecTestMust(cql.TextValue("field1")), Value: codecTestMust(cql.TextValue("val1"))}, {Key: codecTestMust(cql.TextValue("field2")), Value: codecTestMust(cql.TextValue("val2"))}, {Key: codecTestMust(cql.TextValue("field3")), Value: codecTestMust(cql.TextValue("val3"))}}))}, {Name: "testvarint", Value: cql.VarintValue(cql.VarintFromBigInt(bigInt))}, {Name: "testinet", Value: cassandraValue(cql.InetValue(netip.MustParseAddr("213.212.2.19")))}}
		var values []cql.Value
		for _, field := range expected {
			values = append(values, field.Value)
		}
		cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(testuuid,testtimestamp,testvarchar,testbigint,testblob,testbool,testfloat,testdouble,testint,testdecimal,testlist,testset,testmap,testvarint,testinet) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", table), values...)
		check := func(row cql.DynamicRow) {
			t.Helper()
			require.Equal(t, len(expected), row.Columns().Len())
			for _, field := range expected {
				actual := cassandraField(t, row, field.Name)
				want, err := cql.EncodeValue(actual.Type(), cql.DynamicCodec, field.Value)
				require.NoError(t, err, field.Name)
				got, err := cql.EncodeValue(actual.Type(), cql.DynamicCodec, actual)
				require.NoError(t, err, field.Name)
				require.Equal(t, want.State(), got.State(), field.Name)
				require.Equal(t, want.Bytes().OwnedBytes(), got.Bytes().OwnedBytes(), field.Name)
			}
		}
		// Preserve the three result-consumption forms as cloned slices, a cursor,
		// and an exactly-one-row read, all with explicit dynamic values.
		bound := cassandraUnprepared(t, "SELECT * FROM "+table, cql.DynamicRows)
		rows := cassandraRows(t, session, bound, gocql.Options{})
		require.Len(t, rows, 1)
		check(rows[0])
		cursor, err := Run(context.Background(), session, bound, gocql.CompiledOptions{})
		require.NoError(t, err)
		defer cursor.Close()
		var row cql.DynamicRow
		require.True(t, cursor.NextInto(&row))
		check(row)
		require.NoError(t, cursor.Close())
		check(cassandraRead(t, session, "SELECT * FROM "+table))
	})
}
