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
	"github.com/scylladb/gocql/v2/event"
)

// A subscriber waits for a schema change: it subscribes before it changes
// the schema, so that no event falls between the two, and reads the current
// metadata on each event. The session publishes the metadata of a change
// before it delivers the change's event, unless the event is Incomplete.
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

	sub, err := session.Subscribe(event.Filter{Keyspace: "app", Kinds: []event.Kind{event.KindSchema}})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()
	for _, stmt := range []string{
		"CREATE KEYSPACE IF NOT EXISTS app WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}",
		"CREATE TABLE IF NOT EXISTS app.events (id int PRIMARY KEY, body text)",
	} {
		if _, err := session.UnpreparedCommand(ctx, stmt); err != nil {
			log.Fatal(err)
		}
	}

	for {
		if table, ok := session.Metadata().Table("app", "events"); ok {
			fmt.Println(table.Keyspace+"."+table.Name, len(table.Columns), "columns")
			return
		}
		select {
		case <-ctx.Done():
			log.Fatal(ctx.Err())
		case <-sub.Events():
		}
	}
}
