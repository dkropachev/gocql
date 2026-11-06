/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql_test

import (
	"context"
	"fmt"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"

	"log"
)

func Example() {
	// Assumes example.tweet(timeline text, id uuid, text text, PRIMARY KEY(timeline,id)).
	ctx := context.Background()
	config := gocql.DefaultConfig("localhost:9042")
	config.Keyspace = "example"
	session, err := gocql.Connect(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	type tweet struct {
		ID             cql.UUID
		Timeline, Text string
	}
	insert, err := gocql.Prepare(ctx, session, cql.Statement[tweet, cql.Unit]{
		CQL:    "INSERT INTO tweet (id,timeline,text) VALUES (?,?,?)",
		Params: cql.Params(cql.Param("id", cql.UUIDCodec, func(v tweet) cql.UUID { return v.ID }), cql.Param("timeline", cql.StringEncoder, func(v tweet) string { return v.Timeline }), cql.Param("text", cql.StringEncoder, func(v tweet) string { return v.Text })), Rows: cql.NoRows,
	}, gocql.PrepareOptions{})
	if err != nil {
		log.Fatal(err)
	}
	bound, err := gocql.NewQuery(insert, gocql.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		bound.Close()
		if err := bound.Wait(ctx); err != nil {
			log.Print(err)
		}
	}()
	if err := bound.Exec(ctx, tweet{cql.NewTimeUUID(), "me", "hello world"}); err != nil {
		log.Fatal(err)
	}
	type result struct {
		ID   cql.UUID
		Text cql.Text
	}
	query, err := gocql.Prepare(ctx, session, cql.Statement[string, result]{CQL: "SELECT id,text FROM tweet WHERE timeline = ?", Params: cql.OneParam(cql.StringEncoder), Rows: cql.Row(cql.Column("id", cql.UUIDCodec, func(v *result) *cql.UUID { return &v.ID }), cql.Column("text", cql.TextCodec, func(v *result) *cql.Text { return &v.Text }))}, gocql.PrepareOptions{})
	if err != nil {
		log.Fatal(err)
	}
	selectBound, err := gocql.NewQuery(query, gocql.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		selectBound.Close()
		if err := selectBound.Wait(ctx); err != nil {
			log.Print(err)
		}
	}()
	rows, err := selectBound.Run(ctx, "me")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var row result
	for rows.NextInto(&row) {
		fmt.Println(row.ID, row.Text.String())
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
