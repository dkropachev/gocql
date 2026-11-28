// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltrace_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/cqltrace"
	"github.com/scylladb/gocql/v2/execopt"
	"github.com/scylladb/gocql/v2/observe"
	"github.com/scylladb/gocql/v2/routing"
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
		// Fetch waits at most Fetcher.MaxWait, 10s by default.
		info, err := cqltrace.Fetch(ctx, session, trace)
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

// Example_slowQueries traces one request in a hundred and prints the CQL
// trace of each traced attempt slower than 100ms. Hooks receive no session,
// and the session that runs them does not exist until Connect returns, so
// the hook reads it through an atomic.Pointer stored after Connect; an
// attempt that ends before then is not fetched. The fetch runs on a
// goroutine of its own, since a hook that blocks delays the request.
func Example_slowQueries() {
	ctx := context.Background()
	var current atomic.Pointer[gocql.Session]
	var requests atomic.Uint64
	hooks := observe.Hooks{Attempt: func(_ context.Context, _ routing.AttemptStart) func(routing.AttemptEvent) {
		return func(e routing.AttemptEvent) {
			ref, traced := e.CQLTrace()
			session := current.Load()
			if !traced || session == nil || e.Latency < 100*time.Millisecond {
				return
			}
			go func() {
				info, err := cqltrace.Fetch(context.Background(), session, ref)
				if err != nil {
					log.Printf("trace %s: %v", ref.ID, err)
					return
				}
				log.Printf("slow attempt %d of page %d took %v:\n%s", ref.Attempt, ref.Page, e.Latency, info)
			}()
		}
	}}
	profile := config.NewProfile().WithCQLTraceSampler(func(routing.Info) bool { return requests.Add(1)%100 == 0 })
	cfg, err := config.New(config.ScyllaV4, "localhost:9042").WithObserver(hooks).WithDefaultProfile(profile).Build()
	if err != nil {
		log.Fatal(err)
	}
	session, err := gocql.Connect(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	current.Store(session)
	defer session.Close()
	// Run the application's requests with session.
}
