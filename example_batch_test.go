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
	"log"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
)

// ExampleExecBatch demonstrates typed batch inputs and borrowed result rows.
func ExampleExecBatch() {
	// Assumes example.batches(pk int, ck int, description text, PRIMARY KEY(pk, ck)).
	ctx := context.Background()
	config := gocql.DefaultConfig("localhost:9042")
	config.Keyspace = "example"
	session, err := gocql.Connect(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	type item struct {
		PK, CK      int32
		Description string
	}
	params := cql.Params(
		cql.Param("pk", cql.Int32, func(v item) int32 { return v.PK }),
		cql.Param("ck", cql.Int32, func(v item) int32 { return v.CK }),
		cql.Param("description", cql.StringEncoder, func(v item) string { return v.Description }),
	)
	prepared, err := gocql.Prepare(ctx, session, cql.Statement[item, cql.Unit]{
		CQL:    "INSERT INTO example.batches (pk, ck, description) VALUES (?, ?, ?)",
		Params: params, Rows: cql.NoRows,
	}, gocql.PrepareOptions{})
	if err != nil {
		log.Fatal(err)
	}
	query, err := gocql.NewQuery(prepared, gocql.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer query.Close()
	var entries []gocql.BatchItem
	for _, value := range []item{{1, 2, "1.2"}, {1, 3, "1.3"}, {1, 4, "1.4"}, {1, 5, "1.5"}} {
		entry, err := query.BatchEntry(value)
		if err != nil {
			log.Fatal(err)
		}
		entries = append(entries, entry)
	}
	idempotent, err := gocql.NewOptions(session, gocql.OptionValues{Idempotent: true})
	if err != nil {
		log.Fatal(err)
	}
	if _, err := gocql.ExecBatch(ctx, session, cql.UnloggedBatch, entries, idempotent); err != nil {
		log.Fatal(err)
	}
	type result struct {
		PK, CK      int32
		Description cql.Text
	}
	row := cql.Row(
		cql.Column("pk", cql.Int32, func(v *result) *int32 { return &v.PK }),
		cql.Column("ck", cql.Int32, func(v *result) *int32 { return &v.CK }),
		cql.Column("description", cql.TextCodec, func(v *result) *cql.Text { return &v.Description }),
	)
	bound, err := gocql.NewUnpreparedQuery(session, cql.Statement[cql.Unit, result]{
		CQL: "SELECT pk, ck, description FROM example.batches", Params: cql.NoParams, Rows: row,
	}, cql.NewColumns(), gocql.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		bound.Close()
		if err := bound.Wait(ctx); err != nil {
			log.Print(err)
		}
	}()
	rows, err := bound.Run(ctx, cql.Unit{})
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		value := rows.Row()
		fmt.Println(value.PK, value.CK, value.Description.String())
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
