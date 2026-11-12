// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql_test

import (
	"context"
	"log"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
)

func ExampleSession_UnpreparedExec() {
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
	// An occasional command needs no preparation or reusable query to close.
	_, err = session.UnpreparedExec(ctx, cql.Command("CREATE KEYSPACE IF NOT EXISTS app WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"), cql.EmptyParams{})
	if err != nil {
		log.Fatal(err)
	}
}
