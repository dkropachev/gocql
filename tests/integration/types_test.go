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
	"bytes"
	"context"
	"fmt"
	"math"
	"math/big"
	"net"
	"net/netip"
	"testing"

	implSession "github.com/scylladb/gocql/v2/internal/session"

	"github.com/stretchr/testify/require"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cql/dynamic"
	valcases "github.com/scylladb/gocql/v2/internal/tests/valuecases"
)

func TestTypes(t *testing.T) {
	t.Parallel()

	t.Run("RoundTrip", func(t *testing.T) {
		t.Parallel()

		t.Run("Duration", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			if (*implSession.Session)(session).Data().InternalCfg.ProtoVersion < 5 {
				t.Skip("skipped below protocol 5 by this test's own guard, not by a limit of the duration type")
			}
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(k int PRIMARY KEY,v duration)", table))
			for _, want := range []cql.Duration{{Months: 250, Days: 500, Nanoseconds: 300010001}, {Months: -250, Days: -500, Nanoseconds: -300010001}, {Days: 128, Nanoseconds: 127}, {Months: 0x7fffffff, Days: 0x7fffffff, Nanoseconds: 0x7fffffffffffffff}} {
				cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(k,v) VALUES(1,?)", table), cassandraValue(dynamic.Duration(want)))
				row := cassandraRead(t, session, "SELECT k,v FROM "+table)
				require.Equal(t, int64(1), cassandraInt(t, row, "k"))
				got, e := cassandraField(t, row, "v").Duration()
				require.NoError(t, e)
				require.Equal(t, want, got)
			}
		})
		// Tests to see that a pointer to a float64 is marshalled correctly.
		t.Run("Float64Ptr", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id double PRIMARY KEY,test double)", table))
			testNum := 7500.0
			params := cql.Params(cql.Param("id", cql.Float64Codec, func(v *float64) float64 { return *v }), cql.Param("test", cql.Float64Codec, func(v *float64) float64 { return *v }))
			prepared := cassandraPrepared(t, session, cql.Statement[*float64, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(id,test) VALUES(?,?)", table), Params: params, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
			_, err := prepared.Exec(context.Background(), &testNum)
			require.NoError(t, err)
			got, err := cassandraField(t, cassandraRead(t, session, "SELECT test FROM "+table), "test").Float()
			require.NoError(t, err)
			require.Equal(t, testNum, got)
		})
		// Tests to see that a pointer to a float64 is marshalled correctly.
		t.Run("Inet", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(ip inet PRIMARY KEY,name text)", table))
			ipFromNet, ok := netip.AddrFromSlice(net.ParseIP("222.43.54.65"))
			require.True(t, ok)
			for _, want := range []netip.Addr{netip.MustParseAddr("123.34.45.56"), ipFromNet.Unmap()} {
				cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(ip,name) VALUES(?,?)", table), cassandraValue(dynamic.Inet(want)), codecTestMust(dynamic.Text("Test IP")))
				for i := 0; i < 2; i++ {
					row := cassandraRead(t, session, "SELECT ip FROM "+table)
					got, err := cassandraField(t, row, "ip").Inet()
					require.NoError(t, err)
					require.Equal(t, want.String(), got.String())
					require.Equal(t, want, got)
				}
				cassandraWrite(t, session, fmt.Sprintf("DELETE FROM %s WHERE ip=?", table), cassandraValue(dynamic.Inet(want)))
			}
		})
		t.Run("SmallInt", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(testsmallint smallint PRIMARY KEY)", table))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(testsmallint) VALUES(?)", table), dynamic.SmallInt(2))
			require.Equal(t, int64(2), cassandraInt(t, cassandraRead(t, session, "SELECT * FROM "+table), "testsmallint"))
		})
		t.Run("Varint", func(t *testing.T) {
			t.Parallel()
			session := createV2Session(t)
			defer session.Close()
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id varchar PRIMARY KEY,test varint,test2 varint)", table))
			insert := fmt.Sprintf("INSERT INTO %s(id,test) VALUES(?,?)", table)
			selectCQL := "SELECT test FROM " + table
			for _, want := range []int64{0, -1, int64(math.MaxInt32) + 1} {
				cassandraWrite(t, session, insert, codecTestMust(dynamic.Text("id")), dynamic.Varint(cql.VarintFromBigInt(big.NewInt(want))))
				row := cassandraRead(t, session, selectCQL)
				got, err := cassandraField(t, row, "test").Varint()
				require.NoError(t, err)
				require.Equal(t, want, got.BigInt().Int64())
			}
			cassandraWrite(t, session, insert, codecTestMust(dynamic.Text("id")), dynamic.Null(dynamic.Varint(cql.Varint{}).Type()))
			null := cassandraUnpreparedOne(t, session, selectCQL, cql.OneColumn(cql.NullableCodec(cql.VarintCodec)), gocql.Options{})
			require.False(t, null.Valid)
			require.Zero(t, null.Value.BigInt().Sign())
			biggie, ok := new(big.Int).SetString("36893488147419103232", 10)
			require.True(t, ok)
			cassandraWrite(t, session, insert, codecTestMust(dynamic.Text("id")), dynamic.Varint(cql.VarintFromBigInt(biggie)))
			got, err := cassandraField(t, cassandraRead(t, session, selectCQL), "test").Varint()
			require.NoError(t, err)
			require.Equal(t, biggie.String(), got.BigInt().String())
			var narrowed int64
			decoded := false
			_, err = session.UnpreparedEach(context.Background(), cql.Statement[cql.EmptyParams, int64]{CQL: selectCQL, Rows: cql.OneColumn(cassandraVarintInt64{})}, cql.EmptyParams{}, &narrowed, func(*int64) error {
				decoded = true
				return nil
			})
			require.False(t, decoded)
			require.ErrorContains(t, err, "int64 range")
			absent := cassandraUnpreparedOne(t, session, "SELECT test2 FROM "+table, cql.OneColumn(cql.NullableCodec(cql.VarintCodec)), gocql.Options{})
			require.False(t, absent.Valid)
			require.Zero(t, absent.Value.BigInt().Sign())
		})
	})

	t.Run("Simple", func(t *testing.T) {
		t.Parallel()
		types := valcases.GetSimple()
		// Preserve every one of the 96 binary fixtures and every language adapter.
		t.Run("Marshal", func(t *testing.T) {
			for _, tc := range types {
				t.Run(tc.CQLName, func(t *testing.T) {
					for _, vc := range tc.Cases {
						for _, lc := range vc.LangCases {
							t.Run(vc.Name+"/"+lc.LangType, func(t *testing.T) {
								typ, err := cql.ScalarType(cql.Type(tc.CQLType))
								if err != nil {
									t.Fatal(err)
								}
								value, err := serializationFixtureValue(cql.Type(tc.CQLType), lc.Value)
								if err != nil {
									t.Fatal(err)
								}
								for _, owned := range []bool{false, true} {
									var encoded cql.EncodedValue
									if owned {
										encoded, err = cql.EncodeValue(typ, dynamic.Codec, value)
									} else {
										encoded, err = cql.EncodeValueBorrowed(typ, dynamic.Codec, value)
									}
									if (err != nil) != lc.ErrInsert {
										t.Fatalf("encode error: %v (expected error %t)", err, lc.ErrInsert)
									}
									if err != nil {
										continue
									}
									if !bytes.Equal(vc.Data, encoded.Bytes().OwnedBytes()) {
										t.Fatalf("binary fixture: expected %x, got %x", vc.Data, encoded.Bytes().OwnedBytes())
									}
									if (vc.Data == nil) != (encoded.State() == cql.Null) {
										t.Fatalf("null/present distinction lost: %s", vc.Name)
									}
								}
							})
						}
					}
				})
			}
		})
		t.Run("Unmarshal", func(t *testing.T) {
			for _, tc := range types {
				t.Run(tc.CQLName, func(t *testing.T) {
					for _, vc := range tc.Cases {
						for _, lc := range vc.LangCases {
							t.Run(vc.Name+"/"+lc.LangType, func(t *testing.T) {
								typ, err := cql.ScalarType(cql.Type(tc.CQLType))
								if err != nil {
									t.Fatal(err)
								}
								state := cql.Present
								if vc.Data == nil {
									state = cql.Null
								}
								encoded, err := cql.NewEncodedValue(typ, state, cql.BorrowBytes(vc.Data))
								if err != nil {
									t.Fatal(err)
								}
								actual, err := cql.DecodeValue(dynamic.Codec, encoded)
								if (err != nil) != lc.ErrSelect {
									t.Fatalf("decode error: %v (expected error %t)", err, lc.ErrSelect)
								}
								if err != nil {
									return
								}
								expected, err := serializationFixtureValue(cql.Type(tc.CQLType), lc.Value)
								if err != nil {
									t.Fatal(err)
								}
								assertSerializationValue(t, expected, actual)
							})
						}
					}
				})
			}
		})
		s := createV2Session(t)
		defer s.Close()
		tables := make([]string, len(types))
		for i, tc := range types {
			tables[i] = serializationTableName(t, tc.CQLName)
			serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(test_id text PRIMARY KEY,test_col %s)", tables[i], tc.CQLName))
		}
		t.Run("InsertSelect", func(t *testing.T) {
			for i, tc := range types {
				t.Run(tc.CQLName, func(t *testing.T) {
					for _, vc := range tc.Cases {
						for _, lc := range vc.LangCases {
							t.Run(vc.Name+"/"+lc.LangType, func(t *testing.T) {
								input, err := serializationFixtureValue(cql.Type(tc.CQLType), lc.Value)
								if err != nil {
									t.Fatal(err)
								}
								id := vc.Name + "/" + lc.LangType
								_, err = v2Exec(t.Context(), s, fmt.Sprintf("INSERT INTO %s(test_id,test_col) VALUES(?,?)", tables[i]), codecTestMust(dynamic.Text(id)), input)
								if (err != nil) != lc.ErrInsert {
									t.Fatalf("insert error: %v (expected error %t)", err, lc.ErrInsert)
								}
								if err != nil {
									return
								}
								stmt := fmt.Sprintf("SELECT test_col FROM %s WHERE test_id=?", tables[i])
								got := serializationColumn(t, s, stmt, codecTestMust(dynamic.Text(id)))
								assertSerializationValue(t, input, got)
								raw, err := serializationOne(s, stmt, cql.OneColumn(serializationRawDecoder{}), codecTestMust(dynamic.Text(id)))
								if err != nil {
									t.Fatal(err)
								}
								if !bytes.Equal(vc.Data, raw.Bytes().OwnedBytes()) {
									t.Fatalf("database binary fixture: expected %x, got %x", vc.Data, raw.Bytes().OwnedBytes())
								}
								if (vc.Data == nil) != (raw.State() == cql.Null) {
									t.Fatal("database null/present distinction lost")
								}
							})
						}
					}
				})
			}
		})
	})
}
