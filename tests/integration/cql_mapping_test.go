//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/execopt"
)

// TestUDTAlterTypeAdd decodes a UDT value written before ALTER TYPE ADD
// into a struct that maps the added field: the server leaves the field out
// of the old value, and it decodes as zero.
func TestUDTAlterTypeAdd(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table, typ := udtTable(t, s, "name text")
	type before struct{ Name string }
	type person struct {
		Name string
		Age  int32
		Nick *string
	}
	insert := fmt.Sprintf("INSERT INTO %s(id,udt_col) VALUES(1,?)", table)
	if err := serializationExec(s, insert, cql.OneParam(cql.MustUDTCodecOf[before]()), before{Name: "ann"}); err != nil {
		t.Fatal(err)
	}
	serializationDDL(t, s, fmt.Sprintf("ALTER TYPE %s ADD age int", typ))
	serializationDDL(t, s, fmt.Sprintf("ALTER TYPE %s ADD nick text", typ))
	got, err := serializationOne(s, fmt.Sprintf("SELECT udt_col FROM %s WHERE id=1", table), cql.OneColumn(cql.MustUDTCodecOf[person]()))
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "ann" || got.Age != 0 || got.Nick != nil {
		t.Fatalf("value predating ALTER TYPE ADD: %+v", got)
	}
	// A struct that predates the change still decodes with IgnoreUnknownFields.
	old, err := serializationOne(s, fmt.Sprintf("SELECT udt_col FROM %s WHERE id=1", table), cql.OneColumn(cql.MustUDTCodecOf[before](cql.IgnoreUnknownFields())))
	if err != nil || old.Name != "ann" {
		t.Fatalf("struct predating ALTER TYPE ADD: %+v, %v", old, err)
	}
}

// TestRepeatedPositionalMarkers prepares a range query whose two ? markers
// the server names after their column: ParamsOf rejects binding both from
// one field, and named markers bind two fields.
func TestRepeatedPositionalMarkers(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	ctx := context.Background()
	table := serializationTableName(t)
	serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(pk int, ck int, PRIMARY KEY (pk, ck))", table))
	for ck := range 5 {
		if err := serializationExec(s, fmt.Sprintf("INSERT INTO %s(pk,ck) VALUES(1,?)", table), cql.OneParam(cql.Int32Codec), int32(ck)); err != nil {
			t.Fatal(err)
		}
	}
	type key struct{ PK, CK int32 }
	positional := cql.Select(fmt.Sprintf("SELECT ck FROM %s WHERE pk = ? AND ck > ? AND ck < ?", table), cql.MustParamsOf[key](), cql.OneColumn(cql.Int32Codec))
	if _, err := s.Prepare(ctx, positional, execopt.PrepareOptions{}); err == nil || !strings.Contains(err.Error(), ":low and :high") {
		t.Fatalf("repeated ? markers: %v", err)
	}
	type bounds struct{ PK, Low, High int32 }
	named := cql.Select(fmt.Sprintf("SELECT ck FROM %s WHERE pk = :pk AND ck > :low AND ck < :high", table), cql.MustParamsOf[bounds](), cql.OneColumn(cql.Int32Codec))
	p, err := s.Prepare(ctx, named, execopt.PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var got []int32
	var ck int32
	if _, err := p.Each(ctx, bounds{PK: 1, Low: 1, High: 4}, &ck, func(v *int32) error { got = append(got, *v); return nil }); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != "[2 3]" {
		t.Fatalf("named markers: %v", got)
	}
}
