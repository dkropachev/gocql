// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql_test

import (
	"context"
	"fmt"
	"log"
	"time"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/event"
)

// A subscriber keeps a metadata snapshot current: it subscribes before it
// reads the snapshot, reads a newer one when an event's Generation passes
// it, and starts over when the subscription overflows.
func ExampleSession_Subscribe() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cfg, err := config.New(config.ScyllaV4, "localhost:9042").Build()
	if err != nil {
		log.Fatal(err)
	}
	session, err := gocql.Connect(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	filter := event.Filter{Keyspace: "app", Kinds: []event.Kind{event.KindSchema}}
	// Subscribe first, so that no change falls between the snapshot and the
	// first event.
	sub, err := session.Subscribe(filter)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { sub.Close() }()
	md := session.Metadata()

	for _, stmt := range []string{
		"CREATE KEYSPACE IF NOT EXISTS app WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}",
		"CREATE TABLE IF NOT EXISTS app.events (id int PRIMARY KEY, body text)",
	} {
		if _, err := session.UnpreparedExec(ctx, cql.Command(stmt), cql.EmptyParams{}); err != nil {
			log.Fatal(err)
		}
	}

	for {
		if table, ok := md.Table("app", "events"); ok {
			fmt.Println(table.Keyspace+"."+table.Name, len(table.Columns), "columns")
			return
		}
		select {
		case <-ctx.Done():
			log.Fatal(ctx.Err())
		case e, ok := <-sub.Events():
			switch {
			case !ok || e.Kind == event.KindOverflow:
				// The queue overflowed and closed: changes were lost.
				// Subscribe again, then read a fresh snapshot.
				sub.Close()
				if sub, err = session.Subscribe(filter); err != nil {
					log.Fatal(err)
				}
				md = session.Metadata()
			case e.Generation > md.Generation():
				md = session.Metadata()
			}
		}
	}
}
