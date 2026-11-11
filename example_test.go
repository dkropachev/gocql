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
	config, err := gocql.NewConfig("localhost:9042").WithKeyspace("example").Build()
	if err != nil {
		log.Fatal(err)
	}
	session, err := gocql.Connect[gocql.ScyllaV4](ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	type tweet struct {
		ID             cql.UUID
		Timeline, Text string
	}
	insertPrepared, err := session.Prepare(ctx, cql.Statement[tweet, cql.EmptyRow]{
		CQL: "INSERT INTO tweet (id,timeline,text) VALUES (?,?,?)",
		Params: cql.Params(
			cql.Param("id", cql.UUIDCodec, func(v tweet) cql.UUID { return v.ID }),
			cql.Param("timeline", cql.StringCodec, func(v tweet) string { return v.Timeline }),
			cql.Param("text", cql.StringCodec, func(v tweet) string { return v.Text }),
		),
	})
	if err != nil {
		log.Fatal(err)
	}
	if _, err := insertPrepared.Exec(ctx, tweet{cql.UUID(cql.NewTimeUUID()), "me", "hello world"}); err != nil {
		log.Fatal(err)
	}
	selectTweetsPrepared, err := session.Prepare(ctx, cql.Statement[string, tweet]{
		CQL:    "SELECT id,timeline,text FROM tweet WHERE timeline = ?",
		Params: cql.OneParam(cql.StringCodec),
		Rows: cql.Row(
			cql.Column("id", cql.UUIDCodec, func(v *tweet) *cql.UUID { return &v.ID }),
			cql.Column("timeline", cql.StringCodec, func(v *tweet) *string { return &v.Timeline }),
			cql.Column("text", cql.StringCodec, func(v *tweet) *string { return &v.Text }),
		),
	})
	if err != nil {
		log.Fatal(err)
	}
	var row tweet
	_, err = selectTweetsPrepared.Each(ctx, "me", &row, func(row *tweet) error {
		fmt.Println(row.ID, row.Text)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
