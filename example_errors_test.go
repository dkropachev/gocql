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
	session, err := gocql.Connect(ctx, gocql.DefaultConfig("localhost:9042"))
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	insert, err := gocql.PrepareQuery(ctx, session, cql.Statement[string, cql.EmptyRow]{
		CQL:    "INSERT INTO example.log (line) VALUES (?)",
		Params: cql.OneParam(cql.StringCodec),
	}, gocql.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer insert.Close()

	err = insert.Exec(ctx, "hello")
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
	if errors.Is(err, cqlerr.ErrServerOverloaded) {
		log.Print("coordinator overloaded")
	}
	var timeout *cqlerr.RequestErrWriteTimeout
	if errors.As(err, &timeout) {
		log.Printf("write timeout: %d of %d replicas acknowledged", timeout.Received, timeout.BlockFor)
	}

	// Every failed attempt is kept, with the host it went to.
	var failure *cqlerr.ExecutionError
	if errors.As(err, &failure) {
		for _, attempt := range failure.Attempts {
			log.Printf("attempt %d on %s: %v", attempt.Number, attempt.Address, attempt.Err)
		}
	}
}

// Example_connectErrors shows how to inspect a failed connect.
func Example_connectErrors() {
	_, err := gocql.Connect(context.Background(), gocql.DefaultConfig("localhost:9042"))
	if errors.Is(err, cqlerr.ErrNoConnectionsStarted) {
		var connect *cqlerr.ConnectError
		if errors.As(err, &connect) {
			for _, host := range connect.Hosts {
				log.Printf("%s: %v", host.Address, host.Err)
			}
		}
	}
}
