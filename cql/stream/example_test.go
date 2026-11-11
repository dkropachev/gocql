// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package stream_test

import (
	"fmt"
	"log"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cql/stream"
)

type download struct {
	Payload []byte
	Pieces  [][]byte
	ID      int32
}

// RowInto maps columns into caller storage; Compile checks the mapping
// against the result metadata, as Session.PrepareStream does.
func ExampleRowInto() {
	decoder := stream.RowInto(func(*download) {},
		stream.Column("id", stream.Int32, func(r *download) *int32 { return &r.ID }),
		stream.Column("payload", stream.Blob, func(r *download) *[]byte { return &r.Payload }),
		stream.Column("pieces", stream.List(stream.Blob), func(r *download) *[][]byte { return &r.Pieces }),
	)
	pieces, err := cql.ListType(cql.BlobType)
	if err != nil {
		log.Fatal(err)
	}
	_, err = decoder.Compile(cql.NewColumns(
		cql.ColumnSpec{Name: "id", Type: cql.IntType},
		cql.ColumnSpec{Name: "payload", Type: cql.BlobType},
		cql.ColumnSpec{Name: "pieces", Type: pieces},
	))
	fmt.Println(err)
	// The struct mapping builds the same decoder from field names.
	_, err = stream.MustRowOf[download]().Compile(cql.NewColumns(
		cql.ColumnSpec{Name: "id", Type: cql.IntType},
		cql.ColumnSpec{Name: "payload", Type: cql.BlobType},
		cql.ColumnSpec{Name: "pieces", Type: pieces},
	))
	fmt.Println(err)
	// Output:
	// <nil>
	// <nil>
}
