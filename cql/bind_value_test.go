// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql_test

import (
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

// BindValue documents EmptyCell as rejected; EmptyCodec is the way to bind
// an empty value.
func TestBindValueRejectsEmptyCell(t *testing.T) {
	encoder := cql.BindEncoder(cql.Encoder[int32](cql.Int32Codec))
	if _, err := cql.EncodeValue(cql.IntType, encoder, cql.BindValue[int32]{State: cql.EmptyCell}); err == nil {
		t.Fatal("EmptyCell bound")
	}
	empty, err := cql.EncodeValue(cql.IntType, cql.EmptyCodec(cql.Int32Codec), cql.EmptyValueOf[int32]{Empty: true})
	if err != nil || empty.State() != cql.EmptyCell {
		t.Fatalf("EmptyCodec: %v, %v", empty.State(), err)
	}
}
