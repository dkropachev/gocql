// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package scylla_test

import (
	"context"
	"fmt"
	"log"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/scylla"
)

// ExampleBatchStatement reports which statement of a conditional batch each
// returned row belongs to. Only a ScyllaDB session compiles here.
func ExampleBatchStatement() {
	ctx := context.Background()
	cfg, err := config.New("localhost:9042").Build()
	if err != nil {
		log.Fatal(err)
	}
	session, err := gocql.Connect[gocql.ScyllaV4](ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	type update struct {
		K       string
		Version int32
	}
	statement := cql.Statement[update, cql.CASResult[cql.EmptyRow]]{
		CQL: "UPDATE example.versions SET version = version WHERE k = ? IF version = ?",
		Params: cql.Params(
			cql.Param("k", cql.StringCodec, func(v update) string { return v.K }),
			cql.Param("version", cql.Int32Codec, func(v update) int32 { return v.Version }),
		),
		Rows: cql.CASRows(cql.Row[cql.EmptyRow]()),
	}
	prepared, err := session.Prepare(ctx, statement)
	if err != nil {
		log.Fatal(err)
	}
	var batch gocql.Batch
	for _, v := range []update{{"a", 1}, {"a", 2}} {
		if err := prepared.AddToBatch(&batch, v); err != nil {
			log.Fatal(err)
		}
	}
	rows, err := session.RunBatch(ctx, &batch, statement.Rows)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var row cql.CASResult[cql.EmptyRow]
	for i := 0; rows.NextInto(&row); i++ {
		if index, ok := scylla.BatchStatement(session, &batch, i); ok {
			fmt.Println("statement", index, "applied:", row.Applied)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
