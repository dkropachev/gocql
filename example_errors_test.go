// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql_test

import (
	"context"
	"errors"
	"log"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
)

// Example_errors shows how to tell whether a failed write changed data.
func Example_errors() {
	ctx := context.Background()
	config, err := gocql.NewConfig("localhost:9042").Build()
	if err != nil {
		log.Fatal(err)
	}
	session, err := gocql.Connect[gocql.ScyllaV4](ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	insert, err := session.Prepare(ctx, cql.Statement[string, cql.EmptyRow]{
		CQL:    "INSERT INTO example.log (line) VALUES (?)",
		Params: cql.OneParam(cql.StringCodec),
	})
	if err != nil {
		log.Fatal(err)
	}

	_, err = insert.Exec(ctx, "hello")
	switch cqlerr.ExecutionStatusOf(err) {
	case cqlerr.NotExecuted:
		// Nothing was applied, e.g. Unavailable or a syntax error: retrying
		// cannot duplicate the write.
	case cqlerr.MaybeExecuted:
		// A timeout or lost connection: the write may have been applied.
	case cqlerr.PartiallyExecuted:
		// Some replicas applied the write; reconcile before retrying.
	}

	// KindOf says whether trying again can help.
	if kind := cqlerr.KindOf(err); kind.Transient() {
		log.Printf("transient %v failure: %v", kind, err)
	}

	// Server errors keep their code and details.
	if errors.Is(err, cqlerr.CodeOverloaded) {
		log.Print("coordinator overloaded")
	}
	if timeout, ok := errors.AsType[*cqlerr.WriteTimeoutError](err); ok {
		log.Printf("write timeout: %d of %d replicas acknowledged", timeout.Received, timeout.BlockFor)
	}

	// Every failed attempt is kept, with the host it went to.
	if failure, ok := errors.AsType[*cqlerr.ExecutionError](err); ok {
		for _, attempt := range failure.Attempts {
			log.Printf("attempt %d on %s: %v", attempt.Number, attempt.Address, attempt.Err)
		}
	}
}

// Example_connectErrors shows how to inspect a failed connect.
func Example_connectErrors() {
	config, err := gocql.NewConfig("localhost:9042").Build()
	if err != nil {
		log.Fatal(err)
	}
	_, err = gocql.Connect[gocql.ScyllaV4](context.Background(), config)
	if errors.Is(err, cqlerr.ErrNoConnectionsStarted) {
		if connect, ok := errors.AsType[*cqlerr.ConnectError](err); ok {
			for _, host := range connect.Hosts {
				log.Printf("%s: %v", host.Address, host.Err)
			}
		}
	}
}
