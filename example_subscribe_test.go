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

// A watcher of host reachability subscribes to Pool events: the session
// raises ChangePoolDown when it marks a host down, whether a server's DOWN
// status event or the host's own connections made it do so, and ChangePoolUp
// when the host's pool connects again. The event's HostID looks the host up
// in the metadata.
func ExampleSession_Subscribe_nodeDown() {
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

	sub, err := session.Subscribe(event.Filter{Kinds: []event.Kind{event.KindPool}})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case e, open := <-sub.Events():
			if !open {
				return
			}
			if e.Kind != event.KindPool {
				// An Overflow event: read the hosts' state from the metadata.
				for _, h := range session.Metadata().Hosts() {
					fmt.Println(h.HostID, h.Datacenter, "up:", h.Up)
				}
				continue
			}
			host, ok := session.Metadata().Host(e.HostID)
			if !ok {
				fmt.Println(e)
				continue
			}
			fmt.Println(e.Change, host.Address, host.Datacenter, host.Rack)
		}
	}
}
