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

// ExampleQuery_ExecBatch builds a batch one statement at a time and sends it.
func ExampleQuery_ExecBatch() {
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
	queryPrepared, err := gocql.Prepare(ctx, session, cql.Statement[item, cql.EmptyRow]{
		CQL: "INSERT INTO example.batches (pk, ck, description) VALUES (?, ?, ?)",
		Params: cql.Params(
			cql.Param("pk", cql.Int32, func(v item) int32 { return v.PK }),
			cql.Param("ck", cql.Int32, func(v item) int32 { return v.CK }),
			cql.Param("description", cql.StringCodec, func(v item) string { return v.Description }),
		),
	})
	if err != nil {
		log.Fatal(err)
	}
	query, err := queryPrepared.Query(gocql.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer query.Close()
	var batch gocql.Batch
	for _, value := range []item{
		{PK: 1, CK: 2, Description: "1.2"},
		{PK: 1, CK: 3, Description: "1.3"},
		{PK: 1, CK: 4, Description: "1.4"},
		{PK: 1, CK: 5, Description: "1.5"},
	} {
		if err := query.AddToBatch(&batch, value); err != nil {
			log.Fatal(err)
		}
	}
	idempotent, err := gocql.BindOptions(session, gocql.Options{Idempotent: true})
	if err != nil {
		log.Fatal(err)
	}
	if _, err := query.ExecBatch(ctx, cql.UnloggedBatch, &batch, idempotent); err != nil {
		log.Fatal(err)
	}
	selectAll, err := gocql.NewUnpreparedQuery(session, cql.Statement[cql.EmptyParams, item]{
		CQL: "SELECT pk, ck, description FROM example.batches",
		Rows: cql.Row(
			cql.Column("pk", cql.Int32, func(v *item) *int32 { return &v.PK }),
			cql.Column("ck", cql.Int32, func(v *item) *int32 { return &v.CK }),
			cql.Column("description", cql.StringCodec, func(v *item) *string { return &v.Description }),
		),
	}, gocql.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := selectAll.CloseAndWait(ctx); err != nil {
			log.Print(err)
		}
	}()
	var row item
	err = selectAll.Each(ctx, cql.EmptyParams{}, &row, func(v *item) error {
		fmt.Println(v.PK, v.CK, v.Description)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
