// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql_test

import (
	"context"
	"errors"
	"log"
	"time"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
)

// Example_errors shows how to tell whether a failed write changed data.
func Example_errors() {
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
	insert, err := session.Prepare(ctx, cql.Write("INSERT INTO example.log (line) VALUES (?)", cql.OneParam(cql.StringCodec)))
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
	case cqlerr.Executed:
		// err is nil: the write succeeded.
	}

	// KindOf says whether trying again can help, and Retryable whether it
	// also is safe, from the idempotence the driver resolved for the
	// request; it is false once ctx has ended or its deadline passed
	// (KindCanceled). The profile's retry policy has already retried at
	// once, so a loop here multiplies its attempts: back off first.
	if kind := cqlerr.KindOf(err); kind.Transient() {
		log.Printf("transient %v failure: %v", kind, err)
	}
	for attempt := 0; cqlerr.Retryable(err) && attempt < 3; attempt++ {
		time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
		_, err = insert.Exec(ctx, "hello")
	}

	// Server errors keep their code and details.
	if errors.Is(err, cqlerr.CodeOverloaded) {
		log.Print("coordinator overloaded")
	}
	if timeout, ok := errors.AsType[*cqlerr.WriteTimeoutError](err); ok {
		log.Printf("write timeout: %d of %d replicas acknowledged", timeout.Received, timeout.BlockFor)
	}

	// An earlier attempt's error is found by type even when the request
	// stopped for another reason, such as its Timeout.
	if unavailable, ok := cqlerr.AttemptAs[*cqlerr.UnavailableError](err); ok {
		log.Printf("%d of %d replicas alive", unavailable.Alive, unavailable.Required)
	}

	// Every failed attempt is kept, with the host it went to.
	if failure, ok := errors.AsType[*cqlerr.ExecutionError](err); ok {
		for _, attempt := range failure.Attempts {
			log.Printf("attempt %d on %s: %v", attempt.Attempt, attempt.Address, attempt.Err)
		}
	}
}

// Example_connectErrors shows how to inspect a failed connect.
func Example_connectErrors() {
	cfg, err := config.New(config.ScyllaV4, "localhost:9042").Build()
	if err != nil {
		log.Fatal(err)
	}
	_, err = gocql.Connect(context.Background(), cfg)
	if errors.Is(err, cqlerr.ErrNoConnectionsStarted) {
		if connect, ok := errors.AsType[*cqlerr.ConnectError](err); ok {
			for _, host := range connect.Hosts {
				log.Printf("%s: %v", host.Address, host.Err)
			}
		}
	}
}
