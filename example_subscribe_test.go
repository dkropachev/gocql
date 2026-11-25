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

// A subscriber keeps a metadata snapshot current: it subscribes before it
// reads the snapshot and reads a newer one when an event's Generation passes
// it. An Overflow event, which replaces events the queue had no room for,
// carries the highest generation it replaced, so the same check catches up.
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
	defer sub.Close()
	md := session.Metadata()

	for _, stmt := range []string{
		"CREATE KEYSPACE IF NOT EXISTS app WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}",
		"CREATE TABLE IF NOT EXISTS app.events (id int PRIMARY KEY, body text)",
	} {
		if _, err := session.UnpreparedCommand(ctx, stmt); err != nil {
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
		case e := <-sub.Events():
			if e.Generation > md.Generation() {
				md = session.Metadata()
			}
		}
	}
}
