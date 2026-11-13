// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltrace_test

import (
	"context"
	"fmt"
	"log"
	"time"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqltrace"
	"github.com/scylladb/gocql/v2/exec"
)

func ExampleFetch() {
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
	insert := cql.Statement[int32, cql.EmptyRow]{
		CQL:         "INSERT INTO app.items (id) VALUES (?)",
		Params:      cql.OneParam(cql.Int32Codec),
		BindMarkers: cql.NewColumns(cql.ColumnSpec{Name: "id", Type: cql.IntType}),
	}
	result, err := session.UnpreparedExec(ctx, insert, 42, exec.Options{CQLTrace: true})
	if err != nil {
		log.Fatal(err)
	}
	for _, id := range result.CQLTraceIDs() {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		info, err := cqltrace.Fetch(ctx, session, id)
		cancel()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Print(info)
	}
}
