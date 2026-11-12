// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql_test

import (
	"context"
	"fmt"
	"log"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
)

// Tweet maps to example.tweet by column name. Running
//
//	//go:generate go run github.com/scylladb/gocql/v2/cmd/cqlgen -type=Tweet
//
// next to it replaces the startup reflection with generated code; the call
// sites stay the same.
type Tweet struct {
	ID       cql.UUID `cql:"id"`
	Timeline string   `cql:"timeline"`
	Text     string   `cql:"text"`
	Cached   bool     `cql:"-"`
}

var (
	tweetParams = cql.MustParamsOf[Tweet]()
	tweetRow    = cql.MustRowOf[Tweet]()
)

func ExampleParamsOf() {
	// Assumes example.tweet(timeline text, id uuid, text text, PRIMARY KEY(timeline,id)).
	ctx := context.Background()
	cfg, err := config.New("localhost:9042").WithKeyspace("example").Build()
	if err != nil {
		log.Fatal(err)
	}
	session, err := gocql.Connect[gocql.ScyllaV4](ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	// A prepared statement is shared by every goroutine; Exec, One and Each run
	// it once with nothing to close.
	insert, err := session.Prepare(ctx, cql.Statement[Tweet, cql.EmptyRow]{
		CQL:    "INSERT INTO tweet (timeline, id, text) VALUES (?, ?, ?)",
		Params: tweetParams,
	})
	if err != nil {
		log.Fatal(err)
	}
	if _, err := insert.Exec(ctx, Tweet{ID: cql.UUID(cql.NewTimeUUID()), Timeline: "me", Text: "hello world"}); err != nil {
		log.Fatal(err)
	}

	selectTweets, err := session.Prepare(ctx, cql.Statement[string, Tweet]{
		CQL:    "SELECT id, timeline, text FROM tweet WHERE timeline = ?",
		Params: cql.OneParam(cql.StringCodec),
		Rows:   tweetRow,
	})
	if err != nil {
		log.Fatal(err)
	}
	var row Tweet
	_, err = selectTweets.Each(ctx, "me", &row, func(row *Tweet) error {
		fmt.Println(row.ID, row.Text)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}

// ExamplePrepared_Query shows a worker that runs one statement many times. Its
// Query keeps parameter buffers and resolved options across executions; the
// worker owns it and closes it when done. Close returns at once; pending writes
// drain in the background, and Session.Close joins them.
func ExamplePrepared_Query() {
	// Assumes example.tweet(timeline text, id uuid, text text, PRIMARY KEY(timeline,id)).
	ctx := context.Background()
	cfg, err := config.New("localhost:9042").WithKeyspace("example").Build()
	if err != nil {
		log.Fatal(err)
	}
	session, err := gocql.Connect[gocql.ScyllaV4](ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	// The same struct binds a statement that uses a subset of its columns.
	edit, err := session.Prepare(ctx, cql.Statement[Tweet, cql.EmptyRow]{
		CQL:    "UPDATE tweet SET text = ? WHERE timeline = ? AND id = ?",
		Params: tweetParams,
	})
	if err != nil {
		log.Fatal(err)
	}

	edits := make(chan Tweet)
	go func() {
		defer close(edits)
		for _, text := range []string{"first", "second", "third"} {
			edits <- Tweet{ID: cql.UUID(cql.NewTimeUUID()), Timeline: "me", Text: text}
		}
	}()

	worker, err := edit.Query(gocql.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer worker.Close()
	for tweet := range edits {
		if _, err := worker.Exec(ctx, tweet); err != nil {
			log.Print(err)
		}
	}
}
