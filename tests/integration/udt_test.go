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
	"strings"
	"testing"
	"time"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
)

type position struct {
	Lat, Lon int32
	Padding  string
}

func positionFields(options cql.UDTOptions) cql.Codec[position] {
	return serializationUDT(
		cql.Params(cql.Param("lat", cql.Int32, func(p position) int32 { return p.Lat }), cql.Param("lon", cql.Int32, func(p position) int32 { return p.Lon }), cql.Param("padding", serializationString, func(p position) string { return p.Padding })),
		cql.Row(cql.Column("lat", cql.Int32, func(p *position) *int32 { return &p.Lat }), cql.Column("lon", cql.Int32, func(p *position) *int32 { return &p.Lon }), cql.Column("padding", serializationString, func(p *position) *string { return &p.Padding })), options)
}

// The original per-member hooks become an application-owned typed Codec.
// Clone is valid before any metadata has been compiled.
type positionCodec struct{ fields cql.Codec[position] }

func (c positionCodec) CheckType(t cql.DataType) error              { return c.fields.CheckType(t) }
func (c positionCodec) Encode(w *cql.ValueWriter, p position) error { return c.fields.Encode(w, p) }
func (c positionCodec) Decode(r cql.ValueReader) (position, error)  { return c.fields.Decode(r) }
func (positionCodec) Clone(p position) position                     { return p }

func udtTable(t *testing.T, s *gocql.Session, fields string) (string, string) {
	t.Helper()
	table, typ := serializationTableName(t), serializationTypeName(t)
	serializationDDL(t, s, fmt.Sprintf("CREATE TYPE %s(%s)", typ, fields))
	serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,udt_col frozen<%s>)", table, typ))
	return table, typ
}
func TestUDT_Marshaler(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table, typ := udtTable(t, s, "lat int,lon int,padding text")
	want := position{-1, 2, strings.Repeat("X", 1000)}
	got := serializationRoundTrip(t, s, table, "udt_col", 1, positionCodec{fields: positionFields(serializationUDTOptions(typ))}, want)
	if got != want {
		t.Fatalf("custom position codec: expected %+v, got %+v", want, got)
	}
}

type serializationHorse struct{ Name, Owner string }

func serializationHorseCodec(typ string) cql.Codec[serializationHorse] {
	return serializationUDT(
		cql.Params(cql.Param("name", serializationString, func(v serializationHorse) string { return v.Name }), cql.Param("owner", serializationString, func(v serializationHorse) string { return v.Owner })),
		cql.Row(cql.Column("name", serializationString, func(v *serializationHorse) *string { return &v.Name }), cql.Column("owner", serializationString, func(v *serializationHorse) *string { return &v.Owner })), serializationUDTOptions(typ))
}

func TestUDT_Reflect(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table, typ := udtTable(t, s, "name text,owner text")
	// Explicit accessors replace struct-tag discovery while preserving both fields.
	want := serializationHorse{"pony", "jim"}
	got := serializationRoundTrip(t, s, table, "udt_col", 1, serializationHorseCodec(typ), want)
	if got != want {
		t.Fatalf("expected %+v, got %+v", want, got)
	}
}
func TestUDT_NullObject(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table, typ := udtTable(t, s, "name text,owner text")
	serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id) VALUES(1)", table))
	got := cql.Nullable[serializationHorse]{Value: serializationHorse{"temp", "temp"}, Valid: true}
	var err error
	got, err = serializationOne(s, fmt.Sprintf("SELECT udt_col FROM %s WHERE id=1", table), cql.OneColumn(cql.NullableDecoder(serializationHorseCodec(typ))))
	if err != nil {
		t.Fatal(err)
	}
	if got.Valid || got.Value != (serializationHorse{}) {
		t.Fatalf("null UDT did not reset destination: %+v", got)
	}
	if _, err := serializationOne(s, fmt.Sprintf("SELECT udt_col FROM %s WHERE id=1", table), cql.OneColumn(serializationHorseCodec(typ))); err == nil {
		t.Fatal("nonnullable UDT accepted null")
	}
}
func TestMapScanUDT(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table, typ := serializationTableName(t), serializationTypeName(t)
	serializationDDL(t, s, fmt.Sprintf("CREATE TYPE %s(created_timestamp timestamp,message text)", typ))
	serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,type int,log_entries list<frozen<%s>>)", table, typ))
	type entry struct {
		Created cql.Timestamp
		Message string
	}
	codec := serializationUDT(cql.Params(cql.Param("created_timestamp", cql.TimestampCodec, func(v entry) cql.Timestamp { return v.Created }), cql.Param("message", serializationString, func(v entry) string { return v.Message })), cql.Row(cql.Column("created_timestamp", cql.TimestampCodec, func(v *entry) *cql.Timestamp { return &v.Created }), cql.Column("message", serializationString, func(v *entry) *string { return &v.Message })), serializationUDTOptions(typ))
	now := time.Now().Truncate(time.Millisecond)
	stamp, err := cql.TimestampFromTime(now)
	if err != nil {
		t.Fatal(err)
	}
	want := []entry{{stamp, "test time now"}}
	serializationRoundTrip(t, s, table, "log_entries", 1, serializationList(codec), want)
	serializationWrite(t, s, fmt.Sprintf("UPDATE %s SET type=1 WHERE id=1", table))
	rows, err := v2Query(t.Context(), s, fmt.Sprintf("SELECT * FROM %s WHERE id=1", table))
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatalf("missing row: %v", rows.Err())
	}
	lease, err := rows.Retain()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	retained := rows.Row()
	clone, err := rows.Clone()
	if err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, r := range []cql.DynamicRow{retained, clone} {
		v, err := r.Get("log_entries")
		if err != nil {
			t.Fatal(err)
		}
		it, err := v.Elements()
		if err != nil {
			t.Fatal(err)
		}
		entries, err := cql.Collect(it)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		fs, err := entries[0].Fields()
		if err != nil {
			t.Fatal(err)
		}
		fields, err := cql.Collect(fs)
		if err != nil {
			t.Fatal(err)
		}
		if len(fields) != 2 {
			t.Fatalf("expected timestamp and message, got %d fields", len(fields))
		}
		for _, f := range fields {
			switch f.Name {
			case "created_timestamp":
				n, e := f.Value.Signed()
				if e != nil || cql.Timestamp(n) != stamp {
					t.Fatalf("timestamp: %d %v", n, e)
				}
			case "message":
				v, e := f.Value.Text()
				if e != nil || v.String() != want[0].Message {
					t.Fatalf("message: %v %v", v, e)
				}
			default:
				t.Fatalf("unexpected field %q", f.Name)
			}
		}
	}
}
func TestUDT_MissingField(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table, typ := udtTable(t, s, "name text,owner text")
	type col struct{ Name string }
	p := cql.Params(cql.Param("name", serializationString, func(v col) string { return v.Name }))
	r := cql.Row(cql.Column("name", serializationString, func(v *col) *string { return &v.Name }))
	strict := serializationUDT(p, r, serializationUDTOptions(typ))
	if err := serializationExec(s, fmt.Sprintf("INSERT INTO %s(id,udt_col) VALUES(1,?)", table), cql.OneParam(strict), col{"test"}); err == nil {
		t.Fatal("strict UDT accepted an unaccounted database field")
	}
	codec := serializationUDT(p, r, cql.UDTOptions{Keyspace: fixtureKeyspace(""), Name: typ, AllowExtraDatabaseFields: true})
	want := col{"test"}
	got := serializationRoundTrip(t, s, table, "udt_col", 1, codec, want)
	if got != want {
		t.Fatalf("expected %+v, got %+v", want, got)
	}
}
func TestUDT_EmptyCollections(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table, typ := udtTable(t, s, "a list<text>,b map<text,text>,c set<text>")
	type udt struct {
		A cql.Nullable[[]string]
		B cql.Nullable[map[string]string]
		C cql.Nullable[[]string]
	}
	list, mp, set := cql.NullableCodec(serializationList(serializationString)), cql.NullableCodec(serializationMap(serializationString, serializationString)), cql.NullableCodec(serializationSet(serializationString))
	codec := serializationUDT(cql.Params(cql.Param("a", list, func(v udt) cql.Nullable[[]string] { return v.A }), cql.Param("b", mp, func(v udt) cql.Nullable[map[string]string] { return v.B }), cql.Param("c", set, func(v udt) cql.Nullable[[]string] { return v.C })), cql.Row(cql.Column("a", list, func(v *udt) *cql.Nullable[[]string] { return &v.A }), cql.Column("b", mp, func(v *udt) *cql.Nullable[map[string]string] { return &v.B }), cql.Column("c", set, func(v *udt) *cql.Nullable[[]string] { return &v.C })), serializationUDTOptions(typ))
	got := serializationRoundTrip(t, s, table, "udt_col", 1, codec, udt{})
	if got.A.Valid || got.B.Valid || got.C.Valid || got.A.Value != nil || got.B.Value != nil || got.C.Value != nil {
		t.Fatalf("null collections: %+v", got)
	}
	got = serializationRoundTrip(t, s, table, "udt_col", 2, codec, udt{A: cql.Nullable[[]string]{Valid: true}, B: cql.Nullable[map[string]string]{Valid: true}, C: cql.Nullable[[]string]{Valid: true}})
	if !got.A.Valid || !got.B.Valid || !got.C.Valid || len(got.A.Value) != 0 || len(got.B.Value) != 0 || len(got.C.Value) != 0 {
		t.Fatalf("present empty collections: %+v", got)
	}
}
func TestUDT_UpdateField(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table, typ := udtTable(t, s, "name text,owner text")
	type col struct {
		Name, Owner string
		Data        cql.Nullable[string]
	}
	p := cql.Params(cql.Param("name", serializationString, func(v col) string { return v.Name }), cql.Param("owner", serializationString, func(v col) string { return v.Owner }), cql.Param("data", cql.NullableCodec(serializationString), func(v col) cql.Nullable[string] { return v.Data }))
	r := cql.Row(cql.Column("name", serializationString, func(v *col) *string { return &v.Name }), cql.Column("owner", serializationString, func(v *col) *string { return &v.Owner }), cql.Column("data", cql.NullableCodec(serializationString), func(v *col) *cql.Nullable[string] { return &v.Data }))
	want := col{Name: "test-name", Owner: "test-owner"}
	if err := serializationExec(s, fmt.Sprintf("INSERT INTO %s(id,udt_col) VALUES(1,?)", table), cql.OneParam(cql.UDTEncoder(p, serializationUDTOptions(typ))), want); err == nil {
		t.Fatal("unknown application field accepted without AllowMissingFields")
	}
	if err := serializationExec(s, fmt.Sprintf("INSERT INTO %s(id,udt_col) VALUES(1,?)", table), cql.OneParam(cql.UDTEncoder(p, cql.UDTOptions{Keyspace: fixtureKeyspace(""), Name: typ, AllowMissingFields: true})), want); err != nil {
		t.Fatal(err)
	}
	serializationDDL(t, s, fmt.Sprintf("ALTER TYPE %s ADD data text", typ))
	got, err := serializationOne(s, fmt.Sprintf("SELECT udt_col FROM %s WHERE id=1", table), cql.OneColumn(cql.UDTDecoder(r, serializationUDTOptions(typ))))
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("added trailing field: expected %+v, got %+v", want, got)
	}
}
func TestUDT_ScanNullUDT(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table, typ := udtTable(t, s, "lat int,lon int,padding text")
	serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id) VALUES(1)", table))
	got, err := serializationOne(s, fmt.Sprintf("SELECT udt_col FROM %s WHERE id=1", table), cql.OneColumn(cql.NullableDecoder(positionCodec{fields: positionFields(serializationUDTOptions(typ))})))
	if err != nil {
		t.Fatal(err)
	}
	if got.Valid {
		t.Fatal("expected null custom UDT")
	}
}
