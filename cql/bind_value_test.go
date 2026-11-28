// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql_test

import (
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

// The zero BindValue binds UNSET; MaybeEmptyCodec is the way to bind an empty
// value.
func TestBindValueZeroUnset(t *testing.T) {
	encoder := cql.BindEncoder(cql.Encoder[int32](cql.Int32Codec))
	if v, err := cql.EncodeValue(cql.IntType, encoder, cql.BindValue[int32]{}); err != nil || v.State() != cql.CellUnset {
		t.Fatalf("zero BindValue encoded as %v, %v; want UNSET", v.State(), err)
	}
	type update struct {
		ID   int32
		Name cql.BindValue[string]
	}
	plan, err := cql.MustParamsOf[update]().Compile(cql.NewColumns(cql.ColumnSpec{Name: "id", Type: cql.IntType}, cql.ColumnSpec{Name: "name", Type: cql.TextType}))
	if err != nil || plan == nil {
		t.Fatal(err)
	}
	empty, err := cql.EncodeValue(cql.IntType, cql.MaybeEmptyCodec(cql.Int32Codec), cql.MaybeEmpty[int32]{Empty: true})
	if err != nil || empty.State() != cql.CellEmpty {
		t.Fatalf("MaybeEmptyCodec: %v, %v", empty.State(), err)
	}
}
