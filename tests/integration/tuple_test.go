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
	"reflect"
	"testing"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
)

type serializationCoord struct{ X, Y int32 }

var serializationCoordCodec = serializationTuple(
	cql.Params(cql.ParamAt(0, cql.Int32, func(v serializationCoord) int32 { return v.X }), cql.ParamAt(1, cql.Int32, func(v serializationCoord) int32 { return v.Y })),
	cql.Row(cql.ColumnAt(0, cql.Int32, func(v *serializationCoord) *int32 { return &v.X }), cql.ColumnAt(1, cql.Int32, func(v *serializationCoord) *int32 { return &v.Y })))

type serializationNullableCoord struct{ X, Y cql.Nullable[int32] }

var serializationNullableCoordCodec = serializationTuple(
	cql.Params(cql.ParamAt(0, cql.NullableCodec(cql.Int32), func(v serializationNullableCoord) cql.Nullable[int32] { return v.X }), cql.ParamAt(1, cql.NullableCodec(cql.Int32), func(v serializationNullableCoord) cql.Nullable[int32] { return v.Y })),
	cql.Row(cql.ColumnAt(0, cql.NullableCodec(cql.Int32), func(v *serializationNullableCoord) *cql.Nullable[int32] { return &v.X }), cql.ColumnAt(1, cql.NullableCodec(cql.Int32), func(v *serializationNullableCoord) *cql.Nullable[int32] { return &v.Y })))

func tupleTable(t *testing.T, s *gocql.Session, typ string) string {
	t.Helper()
	table := serializationTableName(t)
	serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,val %s)", table, typ))
	return table
}

func checkDynamicTuple(t *testing.T, mode string) {
	t.Helper()
	s := createV2Session(t)
	defer s.Close()
	table := tupleTable(t, s, "frozen<tuple<int,int>>")
	switch mode {
	case "present":
		serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id,val) VALUES(1,(1,2))", table))
	case "members":
		serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id,val) VALUES(1,(null,null))", table))
	case "absent":
		serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id) VALUES(1)", table))
	}
	r, err := v2ReadOne(t.Context(), s, fmt.Sprintf("SELECT * FROM %s", table))
	if err != nil {
		t.Fatal(err)
	}
	if r.Columns().Len() != 2 {
		t.Fatalf("tuple expanded physical columns: %d", r.Columns().Len())
	}
	v, err := r.Get("val")
	if err != nil {
		t.Fatal(err)
	}
	if mode == "absent" {
		if v.State() != cql.Null {
			t.Fatal("unset tuple is not null")
		}
		return
	}
	iter, err := v.Elements()
	if err != nil {
		t.Fatal(err)
	}
	members, err := cql.Collect(iter)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(members))
	}
	for i, m := range members {
		if mode == "members" {
			if m.State() != cql.Null {
				t.Errorf("member %d not null", i)
			}
		} else {
			n, e := m.Signed()
			if e != nil || n != int64(i+1) {
				t.Errorf("member %d: %d, %v", i, n, e)
			}
		}
	}
}

type serializationTextPair struct{ A, B cql.Nullable[string] }

var serializationTextPairCodec = serializationTuple(
	cql.Params(cql.ParamAt(0, cql.NullableCodec(serializationString), func(v serializationTextPair) cql.Nullable[string] { return v.A }), cql.ParamAt(1, cql.NullableCodec(serializationString), func(v serializationTextPair) cql.Nullable[string] { return v.B })),
	cql.Row(cql.ColumnAt(0, cql.NullableCodec(serializationString), func(v *serializationTextPair) *cql.Nullable[string] { return &v.A }), cql.ColumnAt(1, cql.NullableCodec(serializationString), func(v *serializationTextPair) *cql.Nullable[string] { return &v.B })))

type serializationMixedTuple struct {
	A int32
	B string
}

var serializationMixedTupleCodec = serializationTuple(
	cql.Params(cql.ParamAt(0, cql.Int32, func(v serializationMixedTuple) int32 { return v.A }), cql.ParamAt(1, serializationString, func(v serializationMixedTuple) string { return v.B })),
	cql.Row(cql.ColumnAt(0, cql.Int32, func(v *serializationMixedTuple) *int32 { return &v.A }), cql.ColumnAt(1, serializationString, func(v *serializationMixedTuple) *string { return &v.B })))

func TestTuple(t *testing.T) {
	t.Parallel()

	t.Run("MapScan", func(t *testing.T) {
		t.Parallel()

		t.Run("Absent", func(t *testing.T) {
			t.Parallel()
			checkDynamicTuple(t, "absent")
		})
		t.Run("NullMembers", func(t *testing.T) {
			t.Parallel()
			checkDynamicTuple(t, "members")
		})
		t.Run("Present", func(t *testing.T) {
			t.Parallel()
			checkDynamicTuple(t, "present")
		})
	})

	t.Run("NestedCollection", func(t *testing.T) {
		t.Parallel()

		t.Run("NonNull", func(t *testing.T) {
			t.Parallel()
			s := createV2Session(t)
			defer s.Close()
			table := tupleTable(t, s, "list<frozen<tuple<int,text>>>")
			// Each historical representation has an explicit typed adapter. Slice/array
			// members use Value's discriminant instead of untyped heterogeneous elements.
			sliceCodec := cql.NamedScalar(serializationMixedTupleCodec, func(v []cql.Value) serializationMixedTuple {
				n, _ := v[0].Signed()
				x, _ := v[1].Text()
				return serializationMixedTuple{int32(n), x.String()}
			}, func(v serializationMixedTuple) []cql.Value {
				return []cql.Value{cql.IntValue(v.A), codecTestMust(cql.TextValue(v.B))}
			})
			arrayCodec := cql.NamedScalar(serializationMixedTupleCodec, func(v [2]cql.Value) serializationMixedTuple {
				n, _ := v[0].Signed()
				x, _ := v[1].Text()
				return serializationMixedTuple{int32(n), x.String()}
			}, func(v serializationMixedTuple) [2]cql.Value {
				return [2]cql.Value{cql.IntValue(v.A), codecTestMust(cql.TextValue(v.B))}
			})
			t.Run("slice", func(t *testing.T) {
				input := [][]cql.Value{{cql.IntValue(1), codecTestMust(cql.TextValue("2"))}, {cql.IntValue(3), codecTestMust(cql.TextValue("4"))}}
				got := serializationRoundTrip(t, s, table, "val", 0, serializationList(sliceCodec), input)
				for i := range got {
					for j := range got[i] {
						assertSerializationValue(t, input[i][j], got[i][j])
					}
				}
			})
			t.Run("array", func(t *testing.T) {
				input := [][2]cql.Value{{cql.IntValue(1), codecTestMust(cql.TextValue("2"))}, {cql.IntValue(3), codecTestMust(cql.TextValue("4"))}}
				got := serializationRoundTrip(t, s, table, "val", 1, serializationList(arrayCodec), input)
				for i := range got {
					for j := range got[i] {
						assertSerializationValue(t, input[i][j], got[i][j])
					}
				}
			})
			t.Run("struct", func(t *testing.T) {
				input := []serializationMixedTuple{{1, "2"}, {3, "4"}}
				got := serializationRoundTrip(t, s, table, "val", 2, serializationList(serializationMixedTupleCodec), input)
				if !reflect.DeepEqual(got, input) {
					t.Fatalf("expected %#v, got %#v", input, got)
				}
			})
		})
		t.Run("Nullable", func(t *testing.T) {
			t.Parallel()
			s := createV2Session(t)
			defer s.Close()
			table := tupleTable(t, s, "list<frozen<tuple<text,text>>>")
			valid := func(s string) cql.Nullable[string] { return cql.Nullable[string]{Value: s, Valid: true} }
			input := []serializationTextPair{{valid("1"), cql.Nullable[string]{}}, {cql.Nullable[string]{}, valid("2")}, {valid("3"), valid("")}}
			sliceCodec := cql.NamedScalar(serializationTextPairCodec, func(v []cql.Nullable[string]) serializationTextPair { return serializationTextPair{v[0], v[1]} }, func(v serializationTextPair) []cql.Nullable[string] { return []cql.Nullable[string]{v.A, v.B} })
			arrayCodec := cql.NamedScalar(serializationTextPairCodec, func(v [2]cql.Nullable[string]) serializationTextPair { return serializationTextPair{v[0], v[1]} }, func(v serializationTextPair) [2]cql.Nullable[string] { return [2]cql.Nullable[string]{v.A, v.B} })
			t.Run("slice", func(t *testing.T) {
				v := [][]cql.Nullable[string]{{input[0].A, input[0].B}, {input[1].A, input[1].B}, {input[2].A, input[2].B}}
				got := serializationRoundTrip(t, s, table, "val", 0, serializationList(sliceCodec), v)
				if !reflect.DeepEqual(v, got) {
					t.Fatalf("expected %#v, got %#v", v, got)
				}
			})
			t.Run("array", func(t *testing.T) {
				v := [][2]cql.Nullable[string]{{input[0].A, input[0].B}, {input[1].A, input[1].B}, {input[2].A, input[2].B}}
				got := serializationRoundTrip(t, s, table, "val", 1, serializationList(arrayCodec), v)
				if !reflect.DeepEqual(v, got) {
					t.Fatalf("expected %#v, got %#v", v, got)
				}
			})
			t.Run("struct", func(t *testing.T) {
				got := serializationRoundTrip(t, s, table, "val", 2, serializationList(serializationTextPairCodec), input)
				if !reflect.DeepEqual(input, got) {
					t.Fatalf("expected %#v, got %#v", input, got)
				}
			})
		})
	})

	t.Run("RoundTrip", func(t *testing.T) {
		t.Parallel()

		t.Run("LastFieldEmpty", func(t *testing.T) {
			t.Parallel()
			s := createV2Session(t)
			defer s.Close()
			table := tupleTable(t, s, "frozen<tuple<text,text>>")
			want := serializationTextPair{cql.Nullable[string]{Value: "abc", Valid: true}, cql.Nullable[string]{Value: "", Valid: true}}
			got := serializationRoundTrip(t, s, table, "val", 1, serializationTextPairCodec, want)
			if got != want {
				t.Fatalf("trailing present empty text became null: expected %+v, got %+v", want, got)
			}
		})
		t.Run("NotSet", func(t *testing.T) {
			t.Parallel()
			s := createV2Session(t)
			defer s.Close()
			table := tupleTable(t, s, "frozen<tuple<int,int>>")
			want := serializationCoord{1, 2}
			got := serializationRoundTrip(t, s, table, "val", 1, serializationCoordCodec, want)
			if got != want {
				t.Fatalf("expected %+v, got %+v", want, got)
			}
			serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id) VALUES(?)", table), cql.IntValue(2))
			nullable, err := serializationOne(s, fmt.Sprintf("SELECT val FROM %s WHERE id=?", table), cql.OneColumn(cql.NullableDecoder(serializationCoordCodec)), cql.IntValue(2))
			if err != nil {
				t.Fatal(err)
			}
			if nullable.Valid || nullable.Value != (serializationCoord{}) {
				t.Fatalf("unset tuple must be explicit null with zero destination: %+v", nullable)
			}
			if _, err := serializationOne(s, fmt.Sprintf("SELECT val FROM %s WHERE id=?", table), cql.OneColumn(serializationCoordCodec), cql.IntValue(2)); err == nil {
				t.Fatal("nonnullable tuple accepted null")
			}
		})
		t.Run("Null", func(t *testing.T) {
			t.Parallel()
			s := createV2Session(t)
			defer s.Close()
			table := tupleTable(t, s, "frozen<tuple<int,int>>")
			got := serializationRoundTrip(t, s, table, "val", 1, serializationNullableCoordCodec, serializationNullableCoord{})
			if got.X.Valid || got.Y.Valid {
				t.Fatalf("expected two null members: %+v", got)
			}
		})
		t.Run("Simple", func(t *testing.T) {
			t.Parallel()
			s := createV2Session(t)
			defer s.Close()
			table := tupleTable(t, s, "frozen<tuple<int,int>>")
			serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id,val) VALUES(?,(?,?))", table), cql.IntValue(1), cql.IntValue(100), cql.IntValue(-100))
			type result struct {
				ID    int32
				Coord serializationCoord
			}
			decoder := cql.Row(cql.Column("id", cql.Int32, func(r *result) *int32 { return &r.ID }), cql.Column("val", serializationCoordCodec, func(r *result) *serializationCoord { return &r.Coord }))
			row, err := serializationOne(s, fmt.Sprintf("SELECT id,val FROM %s WHERE id=?", table), decoder, cql.IntValue(1))
			if err != nil {
				t.Fatal(err)
			}
			if row.ID != 1 || row.Coord.X != 100 || row.Coord.Y != -100 {
				t.Fatalf("unexpected id and coordinates: %+v", row)
			}
		})
	})
}
