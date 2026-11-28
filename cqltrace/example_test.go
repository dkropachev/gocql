// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltrace_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/cqltrace"
	"github.com/scylladb/gocql/v2/execopt"
)

func ExampleFetch() {
	ctx := context.Background()
	cfg, err := config.New(config.ScyllaV4, "localhost:9042").Build()
	if err != nil {
		log.Fatal(err)
	}
	session, err := gocql.Connect(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	insert := cql.Statement[int32, cql.EmptyRow]{
		CQL:         "INSERT INTO app.items (id) VALUES (?)",
		Params:      cql.OneParam(cql.Int32Codec),
		BindMarkers: cql.NewColumns(cql.ColumnSpec{Name: "id", Type: cql.IntType}),
	}
	traced, err := session.CompileOptions(execopt.Options{CQLTrace: true})
	if err != nil {
		log.Fatal(err)
	}
	result, err := session.UnpreparedExec(ctx, insert, 42, traced)
	if err != nil {
		log.Fatal(err)
	}
	for _, trace := range result.CQLTraces() {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		info, err := cqltrace.Fetch(ctx, session, trace)
		cancel()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Print(info)
	}
}

// A failed request's attempts are traced too: the ExecutionError's Result
// names their trace sessions, and FetchAll reads them all.
func ExampleFetchAll() {
	ctx := context.Background()
	cfg, err := config.New(config.ScyllaV4, "localhost:9042").Build()
	if err != nil {
		log.Fatal(err)
	}
	session, err := gocql.Connect(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	insert := cql.Statement[int32, cql.EmptyRow]{
		CQL:         "INSERT INTO app.items (id) VALUES (?)",
		Params:      cql.OneParam(cql.Int32Codec),
		BindMarkers: cql.NewColumns(cql.ColumnSpec{Name: "id", Type: cql.IntType}),
	}
	traced, err := session.CompileOptions(execopt.Options{CQLTrace: true, Consistency: cql.Some(cql.All)})
	if err != nil {
		log.Fatal(err)
	}
	_, err = session.UnpreparedExec(ctx, insert, 42, traced)
	failure, ok := errors.AsType[*cqlerr.ExecutionError](err)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	infos, err := cqltrace.FetchAll(ctx, session, failure.Result.CQLTraces())
	if err != nil {
		log.Print(err) // the sessions that could not be read hold only their ID
	}
	for _, info := range infos {
		fmt.Print(info)
	}
}
