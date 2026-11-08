// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql_test

import (
	"context"
	"fmt"
	"log"

	gocql "github.com/scylladb/gocql/v2"
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
	config, err := gocql.NewConfig("localhost:9042").WithKeyspace("example").Build()
	if err != nil {
		log.Fatal(err)
	}
	session, err := gocql.Connect(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	insertPrepared, err := gocql.Prepare(ctx, session, cql.Statement[Tweet, cql.EmptyRow]{
		CQL:    "INSERT INTO tweet (timeline, id, text) VALUES (?, ?, ?)",
		Params: tweetParams,
	})
	if err != nil {
		log.Fatal(err)
	}
	insert, err := insertPrepared.Query(gocql.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer insert.CloseAndWait(ctx)
	if err := insert.Exec(ctx, Tweet{ID: cql.NewTimeUUID(), Timeline: "me", Text: "hello world"}); err != nil {
		log.Fatal(err)
	}

	// The same struct binds a statement that uses a subset of its columns.
	editPrepared, err := gocql.Prepare(ctx, session, cql.Statement[Tweet, cql.EmptyRow]{
		CQL:    "UPDATE tweet SET text = ? WHERE timeline = ? AND id = ?",
		Params: tweetParams,
	})
	if err != nil {
		log.Fatal(err)
	}
	edit, err := editPrepared.Query(gocql.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer edit.CloseAndWait(ctx)

	selectTweetsPrepared, err := gocql.Prepare(ctx, session, cql.Statement[string, Tweet]{
		CQL:    "SELECT id, timeline, text FROM tweet WHERE timeline = ?",
		Params: cql.OneParam(cql.StringCodec),
		Rows:   tweetRow,
	})
	if err != nil {
		log.Fatal(err)
	}
	selectTweets, err := selectTweetsPrepared.Query(gocql.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer selectTweets.CloseAndWait(ctx)
	var row Tweet
	err = selectTweets.Each(ctx, "me", &row, func(row *Tweet) error {
		fmt.Println(row.ID, row.Text)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
